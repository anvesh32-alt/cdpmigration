import logging
import datetime
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils import getSplitterQueries, userPipelineOptions, getRetentionInvalidMpnQuery
from dofns import segregateRecords, filterBackdatedRecords, parseTimestamp, validateOptIndicator, extractId, \
    segregateRegistrationRecords
from ptransforms import readValuesForExternalIds, validateAndUpdateExternalIds
from helpers import constants

date_fields = ['originalTimestamp', 'timestamp', 'postalConsumerChoiceDate', 'socialConsumerChoiceDate',
               'emailConsumerChoiceDate', 'phoneConsumerChoiceDate', 'regulatoryConsumerChoiceDate']

current_date = datetime.date.today()

def run():
    pipeline_options = PipelineOptions(save_main_session=True)
    options = pipeline_options.view_as(userPipelineOptions.UserPipelineOptions)
    registration, clickstream, clt, other = getSplitterQueries.SplitterQueries().queries_list()
    event_type_lists = [registration, clickstream, clt, other]
    logging.info(f"Begin pipeline with options: {options}")

    reformat_base_queries = getRetentionInvalidMpnQuery.ReformatBaseQueries(options.mpn)
    partial_retention_query = reformat_base_queries.get_partial_retention_query()
    email_query = reformat_base_queries.get_email_from_identifies_table()
    source_id_query = reformat_base_queries.get_source_id_from_identifies_table()
    mpn_query = reformat_base_queries.get_mpn_from_identifies_table()
    country_code = reformat_base_queries.get_country_code()

    with beam.Pipeline(options=options) as pipeline:
        gcs_data = (
                pipeline | 'ReadData' >> beam.io.ReadFromText(options.input_bucket + '/non-retention-users/*')
                | 'ParsedData' >> beam.Map(lambda record: json.loads(record))
        )

        # Read bigquery data for external id field for email, source_id, mpn
        read_bq_data_for_external_id = (
            pipeline | "ReadDataFromBQForExternalID" >> readValuesForExternalIds.ReadValuesForExternalIds(email_query,
                                                                                                        source_id_query,
                                                                                                        mpn_query)
        )

        output_tags = [str(event_type) for event_type in range(len(event_type_lists))]
        unmatched_tag = 'unmatched'

        segregated_data = gcs_data | 'SegregateDataOnEvent' >> beam.ParDo(
            segregateRecords.SegregateRecords(event_type_lists)).with_outputs(*output_tags, unmatched_tag)

        registration_data, clickstream_data, clt_data, other_events_data, unmatched_data = segregated_data[0], \
            segregated_data[1], segregated_data[2], segregated_data[3], segregated_data['unmatched']

        # Registration processing
        parsed_registration_data = registration_data | 'ParsedRegistrationData' >> beam.Map(
            lambda record: json.loads(record))

        output_tags = ['changed_opt', 'identify']
        unmatched_tag = 'unmatched'
        segregate_registration_data = parsed_registration_data | "SegregateRegistrationData" >> beam.ParDo(
            segregateRegistrationRecords.SegregateRecordsByChangedOpt()).with_outputs(*output_tags, unmatched_tag)

        changed_opt_records = segregate_registration_data['changed_opt']
        identify_records = segregate_registration_data['identify']

        # Get Partial Retention out users from bigquery
        partial_retention_out_users_bq = pipeline | 'GetPartialRetentionUserFromBQ' >> beam.io.ReadFromBigQuery(
            query=partial_retention_query,
            use_standard_sql=True,
            project=constants.PROJECT,
            gcs_location=constants.TEMP_LOCATION)

        # Extract the userId/anonymousId field from the gcs_data_valid_changed_opt_records pcollection
        extract_id = changed_opt_records | 'ExtractUserIdFromGCSData' >> beam.ParDo(extractId.ExtractId())

        # Extract the userId field from the partial_retention_out_users_bq pcollection
        get_userid_bq = partial_retention_out_users_bq | 'ExtractUserIdFromBQData' >> beam.Map(
            lambda x: (x['user_id'], x))

        # Group the records on userId key and create non_retention_out_users, partial_retention_out_users pcollection
        registration_changed_opt_users, partial_retention_out_users = (
                ({'gcs_data': extract_id, 'retention_out_users_bq': get_userid_bq})
                | 'MapGCSAndBQData' >> beam.CoGroupByKey()
                | 'PartitionNonRetention|RetentionUsers' >> beam.Partition(
            lambda element, num_elements: 0 if len(element[1]['gcs_data'])
                                               > 0 and len(element[1]['retention_out_users_bq']) == 0 else 1, 2)
        )

        registration_non_partial_records = \
            registration_changed_opt_users | "FlattenPartialRegistrationUsers" >> beam.FlatMap(
                lambda x: x[1]['gcs_data'])
        filtered_registration_data = [identify_records, registration_non_partial_records] \
                                     | "MergingRegistrationFilterData" >> beam.Flatten()

        add_external_id_to_registration = (filtered_registration_data, read_bq_data_for_external_id) \
                                          | "AddExternalIdToRegistrationData" >> validateAndUpdateExternalIds.\
                                              ValidateAndUpdateExternalIds(country_code, "Registration")

        registration_parse_timestamp = (
                add_external_id_to_registration | "FormatTimeStamp" >> beam.ParDo(
            parseTimestamp.validateTimestamp(date_fields))
                | "ParseOptIndicatorData" >> beam.ParDo(validateOptIndicator.ValidateOptIndicator())
        )

        filter_and_parse_timestamp_clickstream_records = (
                clickstream_data | "FilterBackDatedCLKRecords" >> beam.ParDo(
            filterBackdatedRecords.FilterBackDatedRecords(current_date, filter_anonymous_id=True))
                | "ParsetimeCLKData" >> beam.ParDo(parseTimestamp.validateTimestamp(date_fields))
        )
        add_external_id_to_clickstream = (filter_and_parse_timestamp_clickstream_records, read_bq_data_for_external_id) \
                                         | "AddExternalIdToClickStreamData" >> validateAndUpdateExternalIds.\
                                             ValidateAndUpdateExternalIds(country_code, "ClickStream")

        filter_and_parse_timestamp_clt_records = (
                clt_data | "FilterBackDatedCLTRecords" >> beam.ParDo(
            filterBackdatedRecords.FilterBackDatedRecords(current_date))
                | "ParsetimeCLTData" >> beam.ParDo(parseTimestamp.validateTimestamp(date_fields))
        )
        add_external_id_to_clt = (filter_and_parse_timestamp_clt_records, read_bq_data_for_external_id) \
                                 | "AddExternalIdToCLTData" >> validateAndUpdateExternalIds.\
                                     ValidateAndUpdateExternalIds(country_code, "CLT")

        add_external_id_to_other_events = (other_events_data, read_bq_data_for_external_id) \
                                          | "AddExternalIdToOtherEventsData" >> validateAndUpdateExternalIds. \
                                              ValidateAndUpdateExternalIds(country_code, "OtherEvents")
        other_events_parse_timestamp = (
                add_external_id_to_other_events | "ParsetimeOtherData" >> beam.ParDo(parseTimestamp.validateTimestamp(date_fields))

        )

        time_stamp = datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S-%f")
        registration_parse_timestamp | "WriteRegistrationData" >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                                      f'/splitter-registration-output/'
                                                                                      f'splitter_output_registration-'
                                                                                      f'{time_stamp}',
                                                                                      file_name_suffix='.txt')
        (
                partial_retention_out_users | "FlattenPartialRetentionUser" >> beam.FlatMap(lambda x: x[1]['gcs_data'])
                | "WritePartialRRetentionUserToFiles" >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                             f'/splitter-registration-output'
                                                                             f'/blocked-users/'
                                                                             f'blocked_users-{time_stamp}',
                                                                             file_name_suffix='.txt'
                                                                             )
        )

        add_external_id_to_clickstream | "WriteClickStreamData" >> beam.io.WriteToText( f'{options.output_bucket}'
                                                                                        f'/splitter-clickstream-output/'
                                                                                        f'splitter_output_clickstream-'
                                                                                        f'{time_stamp}',
                                                                                        file_name_suffix='.txt')

        add_external_id_to_clt | "WriteCLTData" >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                       f'/splitter-clt-output/'
                                                                       f'splitter_output_clt-{time_stamp}',
                                                                       file_name_suffix='.txt')
        other_events_parse_timestamp | "WriteOtherData" >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                               f'/splitter-other-events-output/'
                                                                               f'splitter_output_other_events-'
                                                                               f'{time_stamp}',
                                                                               file_name_suffix='.txt')
        unmatched_data | "WriteUnmatchedData" >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                     f'/splitter-unmatched-output/'
                                                                     f'splitter_output_unmatched-{time_stamp}',
                                                                     file_name_suffix='.txt')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
