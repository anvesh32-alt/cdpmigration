import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils import userPipelineOptions, getRetentionInvalidMpnQuery
from dofns import extractId, filterInvalidMpn
from helpers import constants


def run():
    """
    Main Entry point of the Retention Out user Dataflow pipeline
    """

    pipeline_options = PipelineOptions(save_main_session=True)
    options = pipeline_options.view_as(userPipelineOptions.UserPipelineOptions)

    logging.info(f"Begin pipeline with options: {options}")

    retention_invalid_mpn_query = getRetentionInvalidMpnQuery.ReformatBaseQueries(options.mpn)
    retention_query = retention_invalid_mpn_query.get_retention_out_query()
    invalid_mpn_query = retention_invalid_mpn_query.get_invalid_mpn_query()

    with beam.Pipeline(options=options) as pipeline:

        # Read the input files from the GCS Bucket
        gcs_data = (
                pipeline | "ReadFilesFromGCS" >> beam.io.ReadFromText(options.input_bucket)
                | "MapGCSData" >> beam.Map(lambda x: x)
        )

        # Get Retention out users from bigquery
        retention_out_users_bq = pipeline | 'GetRetentionUserFromBQ' >> beam.io.ReadFromBigQuery(
                                                                                query=retention_query,
                                                                                use_standard_sql=True,
                                                                                project=constants.PROJECT,
                                                                                gcs_location=constants.TEMP_LOCATION)

        # Get invalid mpns from bigquery and extract marketing_program_number for each record
        get_invalid_mpns_bq = (
                pipeline | 'GetInvalidMpnsFromBQ' >> beam.io.ReadFromBigQuery(query=invalid_mpn_query,
                                                                              use_standard_sql=True,
                                                                              project=constants.PROJECT,
                                                                              gcs_location=constants.TEMP_LOCATION)
                | 'ExtractMpnFromBQData' >> beam.Map(lambda x: x['marketing_program_number'])
        )

        # Filter input data based on invalid/valid mpn
        gcs_data_invalid_mpn, gcs_data_valid_mpn = (
                gcs_data | 'FilterInvalid|ValidMpnFromGCSData' >> beam.ParDo(filterInvalidMpn.FilterInvalidMpn(),
                                                                             invalid_mpns=beam.pvalue.AsList
                                                                             (get_invalid_mpns_bq)).with_outputs(
                                                                             'invalid_mpn_data', 'valid_mpn_data')
        )

        # Extract the userId/anonymousId field from the gcs_data_valid_mpn pcollection
        extract_id = gcs_data_valid_mpn | 'ExtractUserIdFromGCSData' >> beam.ParDo(extractId.ExtractId())

        # Extract the userId field from the retention_out_users_bq pcollection
        get_userid_bq = retention_out_users_bq | 'ExtractUserIdFromBQData' >> beam.Map(lambda x: (x['user_id'], x))

        # Group the records on userId key and create non_retention_out_users, retention_out_users pcollection
        non_retention_out_users, retention_out_users = (
                ({'gcs_data': extract_id, 'retention_out_users_bq': get_userid_bq})
                | 'MapGCSAndBQData' >> beam.CoGroupByKey()
                | 'PartitionNonRetention|RetentionUsers' >> beam.Partition(
                                                lambda element, num_elements: 0 if len(element[1]['gcs_data'])
                                                > 0 and len(element[1]['retention_out_users_bq']) == 0 else 1, 2)
        )

        # Flatten the records and write to file in .txt format
        time_stamp = datetime.now().strftime("%d-%m-%Y-%H-%M-%S-%f")
        (
            non_retention_out_users | "FlattenNonRetentionUsers" >> beam.FlatMap(lambda x: x[1]['gcs_data'])
            | "WriteNonRetentionUserToFiles" >> beam.io.WriteToText(f'{options.output_bucket}/non-retention-users/'
                                                                    f'non_retention_users-{time_stamp}',
                                                                    file_name_suffix='.txt'
                                                                    )
        )

        (
            retention_out_users | "FlattenRetentionUser" >> beam.FlatMap(lambda x: x[1]['gcs_data'])
            | "WriteRetentionUserToFiles" >> beam.io.WriteToText(f'{options.output_bucket}/blocked-users/'
                                                                 f'blocked_users-{time_stamp}',
                                                                 file_name_suffix='.txt'
                                                                 )
        )

        (
            gcs_data_invalid_mpn | "WriteInvalidMpnDataToFiles" >> beam.io.WriteToText(
                                                                    f'{options.output_bucket}/invalid-mpn-data/'
                                                                    f'invalid_mpn_data-{time_stamp}',
                                                                    file_name_suffix='.txt'
                                                                    )
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
