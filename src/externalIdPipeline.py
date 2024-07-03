import json
import logging
import apache_beam as beam
from helpers import constants, bigqueryDatasetsConfig, legalEntityConfig
from datetime import datetime
from utils import userPipelineOptions
from apache_beam.options.pipeline_options import PipelineOptions

from dofns import extractDependentTraits, splitChannelProperties, addLastActivityDate, validateFeaturesOther, \
    validatePostalRecords, parseMpnExternalid, parseMpnString,validateSchema, consumerIdValidation, mpnOptsValidationWithCrs, \
    validateSourceIdMpnWithCrs, segregateFrontLoadValidationRecords, extractId, handleCrsTraitAliases, \
    shiftLeftForKeyValidations,addSourceIdForUniversalMpn, updateOptChoiceDate
from ptransforms import writeToBigquery


def run():
    pipeline_options = PipelineOptions(save_main_session=True)
    options = pipeline_options.view_as(userPipelineOptions.UserPipelineOptions)
    logging.info(f"Begin pipeline with options: {options}")

    # get tables names for registration, clickstream, CLT for a particular MPN for migration pipeline
    if options.pipeline_name == 'migration_pipeline':
        registration_table_name = bigqueryDatasetsConfig.bigquery_table_config[options.mpn].\
            get('destination_registration_table')
        clickstream_table_name = bigqueryDatasetsConfig.bigquery_table_config[options.mpn].\
            get('destination_clickstream_table')
        clt_table_name = bigqueryDatasetsConfig.bigquery_table_config[options.mpn].\
            get('destination_clt_table')
        front_load_validation_failure_table = bigqueryDatasetsConfig.bigquery_table_config[options.mpn].\
            get('front_load_validation_failure_table')

    elif options.pipeline_name == 'migration_pipeline_delta_load':
        registration_table_name = bigqueryDatasetsConfig.bigquery_table_config[options.mpn]. \
            get('destination_registration_table_delta_load')
        clickstream_table_name = bigqueryDatasetsConfig.bigquery_table_config[options.mpn]. \
            get('destination_clickstream_table_delta_load')
        clt_table_name = bigqueryDatasetsConfig.bigquery_table_config[options.mpn]. \
            get('destination_clt_table_delta_load')
        other_events_table_name = bigqueryDatasetsConfig.bigquery_table_config[options.mpn]. \
            get('destination_others_events_table_delta_load')
        front_load_validation_failure_table = bigqueryDatasetsConfig.bigquery_table_config[options.mpn]. \
            get('front_load_validation_failure_table_delta_load')

    # Get the legal entity from config file
    legal_entity = legalEntityConfig.legal_entity[options.mpn]['legal_entity']

    logging.info(f"Table names to load data to: Registration Table name: {registration_table_name}, "
                 f"ClickStream Table Name: {clickstream_table_name}, CLT Table Name: {clt_table_name},"
                 f"Front Load Validation: {front_load_validation_failure_table}")

    with beam.Pipeline(options=options) as pipeline:

        # reading splitter data
        gcs_data_registration = (
            pipeline | 'ReadRegistrationData' >> beam.io.ReadFromText(options.input_bucket + '/splitter-registration-output/'
                                                                      + '*.txt')
        )

        gcs_data_clickstream = (
            pipeline | 'ReadDataClickstreamData' >> beam.io.ReadFromText(options.input_bucket + '/splitter-clickstream-output/'
                                                                         + '*.txt')
        )

        gcs_data_clt = (
            pipeline | 'ReadDataCLT' >> beam.io.ReadFromText(options.input_bucket + '/splitter-clt-output/'
                                                             + '*.txt')
        )

        gcs_data_other = (
            pipeline | 'ReadDataOther' >> beam.io.ReadFromText(options.input_bucket + '/splitter-other-events-output/'
                                                               + '*.txt')
        )

        transform_data_with_registration = (gcs_data_registration | 'LastActivityDateCheck' >>
                                             beam.ParDo(addLastActivityDate.AddLastActivityDate())
            | 'DependentsCheck' >> beam.ParDo(extractDependentTraits.ExtractDependentTraits())
            | 'SplitChannelCheck' >> beam.ParDo(splitChannelProperties.SplitChannelProperties())
            | 'AddContactIndicator'>> beam.ParDo(splitChannelProperties.ContactProperties())
            | 'FeaturesCheck' >> beam.ParDo(validateFeaturesOther.FeaturesCheckOther())
            | 'ParseMpnAsString' >> beam.ParDo(parseMpnString.ValidateMpnString())
            | 'ParseMpnRegistrationExternalId' >> beam.ParDo(parseMpnExternalid.ValidateMpnStringExternalId())
            | "HandleAliasTrait" >> beam.ParDo(handleCrsTraitAliases.HandleCrsTraitAliases())
            | 'AddLegalEntityToPayloads' >> beam.ParDo(shiftLeftForKeyValidations.AddLegalEntityToPayloads(legal_entity))
            | 'AddSourceIdToPayloads' >> beam.ParDo(addSourceIdForUniversalMpn.AddSourceIdToPayloads())
            | 'OptChoiceDateUpdate' >> beam.ParDo(updateOptChoiceDate.MappOptChoiceDateWithTimestamp())
            | 'AddFieldsToTraitsBlock' >> beam.ParDo(shiftLeftForKeyValidations.AddFieldsToTraitsBlock())
            )

        output_tags = ['postal_exclude', 'proper']
        unmatched_tag = 'unmatched'
        registration_output_with_userid = (
            transform_data_with_registration | 'PostalExcludeCheckRegistration' >> beam.ParDo(
                validatePostalRecords.PostalCheck()).with_outputs(*output_tags, unmatched_tag)
        )
        postal_exclude_pcoll_with_userid = registration_output_with_userid['postal_exclude']
        registration_pcol_with_userid = registration_output_with_userid['proper']

        # Validate Schema
        tags = ['proper_schema', 'invalid_schema']
        validate_schema = (registration_pcol_with_userid | 'SchemaValidationCheck' >> beam.ParDo(
            validateSchema.SchemaValidation()).with_outputs(*tags))

        valid_registration_pcol_with_userid = validate_schema['proper_schema']
        invalid_registration_pcol_with_userid = validate_schema['invalid_schema']

        # Write registration data with invalid schema to Bigquery
        _ = (invalid_registration_pcol_with_userid
             | "ConvertToJson" >> beam.Map(json.loads)
             | "CallWriteToBQRegistrationInvalidSchema" >> beam.io.WriteToBigQuery(
                    dataset=constants.REGISTRATION_BIGQUERY_DATASET, table=front_load_validation_failure_table,
                    project=constants.PROJECT,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    schema=constants.FRONT_LOAD_VALIDATION_TABLE_SCHEMA))

        # clickstream
        transform_data_with_clkstrm = (
            gcs_data_clickstream
            | 'FeaturesCheckClicKStream' >> beam.ParDo(validateFeaturesOther.FeaturesCheckOther())
            | 'ParseMpnAsStringClickStream' >> beam.ParDo(parseMpnString.ValidateMpnString())
            | 'ParseMpnClickstreamExternalId' >> beam.ParDo(parseMpnExternalid.ValidateMpnStringExternalId())
            | 'AddFieldsToTraitsBlockClickStream' >> beam.ParDo(shiftLeftForKeyValidations.AddFieldsToTraitsBlock())
        )

        # clt Data
        transform_data_with_clt = (
            gcs_data_clt | 'FeaturesCheckCLT' >> beam.ParDo(validateFeaturesOther.FeaturesCheckOther())
                         | 'ParseMpnAsStringCLT' >> beam.ParDo(parseMpnString.ValidateMpnString())
                         | 'ParseMpnCLTExternalId' >> beam.ParDo(parseMpnExternalid.ValidateMpnStringExternalId())
                         | 'AddFieldsToTraitsBlockCLT' >> beam.ParDo(shiftLeftForKeyValidations.AddFieldsToTraitsBlock())
        )
        # Other events data
        transform_data_with_other = (
            gcs_data_other | 'FeaturesCheckOther' >> beam.ParDo(validateFeaturesOther.FeaturesCheckOther())
                           | 'ParseMpnAsStringOtherEvents' >> beam.ParDo(parseMpnString.ValidateMpnString())
                           | 'ParseMpnOtherEventsExternalId' >> beam.ParDo(parseMpnExternalid.ValidateMpnStringExternalId())
                           | 'AddFieldsToTraitsBlockOtherEvents' >> beam.ParDo(shiftLeftForKeyValidations.AddFieldsToTraitsBlock())
        )
        time_stamp = datetime.now().strftime("%d-%M-%Y-%H-%M-%S-%f")
        (
            valid_registration_pcol_with_userid | f'WriteOutFilesRegistration' >> beam.io.WriteToText(
                                                                                f'{options.output_bucket}'
                                                                                f'/externalid-output-registration/'
                                                                                f'externalid-registration-{time_stamp}',
                                                                                file_name_suffix='.txt')

        )
        (

            transform_data_with_clkstrm | f'WriteOutFilesClickstream' >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                                    f'/externalid-output-clickstream/'
                                                                                    f'externalid-clkstream-{time_stamp}',
                                                                                         file_name_suffix='.txt')

        )
        (

            transform_data_with_clt | f'WriteOutFilesCLT' >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                         f'/externalid-output-clt/'
                                                                         f'externalid-clt-{time_stamp}',
                                                                         file_name_suffix='.txt')

        )
        (

            transform_data_with_other | f'WriteOutFilesOtherEvents' >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                             f'/externalid-output-other-events/'
                                                                             f'externalid-other-event-{time_stamp}',
                                                                             file_name_suffix='.txt')

        )
        (

            postal_exclude_pcoll_with_userid | f'WriteOutFilesPostalExcludeWuserid' >> beam.io.WriteToText(
                                                                            f'{options.output_bucket}'
                                                                            f'/externalid-output-registration'
                                                                            f'/postal_blocked_users/'
                                                                            f'externalid-postal-blocked-{time_stamp}',
                                                                            file_name_suffix='.txt')

        )

        # Front load validation for opts/mpn, consumer id and source id/mpn combination
        front_load_validation = (
                valid_registration_pcol_with_userid | "MpnOptsValidation" >> beam.ParDo(
                                                                    mpnOptsValidationWithCrs.MpnOptsValidationWithCRS())
                | "ConsumerIdValidation" >> beam.ParDo(consumerIdValidation.ConsumerIdValidation())
                | "SourceIdMpnValidation" >> beam.ParDo(validateSourceIdMpnWithCrs.ValidateSourceIdMpnWithCrs())
                | "TimestampValidation" >> beam.ParDo(shiftLeftForKeyValidations.ValidateTimestampInPayloads())
        )

        # segregate front load validation failure/success records
        front_load_validation_failed, front_load_validation_passed = (
                front_load_validation | 'SegregateFrontLoadValidationRecords' >>
                beam.ParDo(segregateFrontLoadValidationRecords.SegregateFrontLoadValidationRecords()).with_outputs(
                    'front_load_validation_failed', 'front_load_validation_passed')
        )

        # Write front load validation failed records to different table
        front_load_validation_failed | "WriteFrontLoadValidationFailedRecordsToBQ" >> beam.io.WriteToBigQuery(
            dataset=constants.REGISTRATION_BIGQUERY_DATASET, table=front_load_validation_failure_table,
            project=constants.PROJECT,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            schema=constants.FRONT_LOAD_VALIDATION_TABLE_SCHEMA,
        )

        # Write registration data to Bigquery
        front_load_validation_passed | "CallWriteToBQRegistrationData" >> writeToBigquery.WriteToBigquery(
            dataset_name=constants.REGISTRATION_BIGQUERY_DATASET, table_name=registration_table_name,
            pipeline_name='Registration')

        # Write all events to BQ for delta load Only
        if options.pipeline_name == 'migration_pipeline_delta_load':
            _ = transform_data_with_clt | "CallWriteToBQCLTData" >> writeToBigquery.WriteLatestEventToBigquery(
                dataset_name=constants.CLT_BIGQUERY_DATASET, table_name=clt_table_name,
                pipeline_name='CLT')

            _ = transform_data_with_clkstrm | "CallWriteToBQClickStreamData" >> writeToBigquery.WriteLatestEventToBigquery(
                dataset_name=constants.CLICKSTREAM_BIGQUERY_DATASET, table_name=clickstream_table_name,
                pipeline_name='ClickStream')

            # todo: check dataset name for other events
            _ = transform_data_with_other | "CallWriteToBQOtherEventsData" >> writeToBigquery.WriteLatestEventToBigquery(
                dataset_name=constants.REGISTRATION_BIGQUERY_DATASET, table_name=other_events_table_name,
                pipeline_name='OtherEvents')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
