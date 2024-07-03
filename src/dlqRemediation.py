import argparse
import json
import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from dofns.publishToPubSub import PublishToPubSub
from helpers import constants, dlqConfig


PUBSUB_FAILURE_FILE_PATH = 'gs://dbce-c360-isl-preprod-d942-migration-88fakc29/bigquery_to_pubsub_failures'


def run(pipeline_name, output_bucket, pipeline_args=None):
    """
    Main Entry point of the DLQ Remediation Dataflow pipeline
    Kindly update timestamp config file before running the job:
    File name : dlq_remediation_timestamp.json
    File path: us-east4-isl-composer-2-7238166d-bucket/dags/dlq_timestamp_config
    """

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    logging.info(f"Starting pipeline with Options: {pipeline_options}")

    try:
        client = storage.Client(constants.PROJECT)
        bucket = client.get_bucket(constants.DAG_BUCKET)
        config = json.loads(
            bucket.get_blob(constants.DLQ_TIMESTAMP_CONFIG).download_as_string()
        )
        timestamp_config = config[pipeline_name]

    except Exception as err:
        logging.error(f'Unable to get configuration, err: {str(err)}')

    current_date = datetime.date.today()

    # Get start time and end time for sql query
    sql_start_time = timestamp_config['start_time']
    sql_end_time = timestamp_config['end_time']

    # Get table name and pubsub topic for respective pubsub
    table_name = dlqConfig.dlq_config[pipeline_name]['table_name']
    pubsub_topic = dlqConfig.dlq_config[pipeline_name]['pubsub_topic']

    query = f"SELECT data FROM `{constants.PROJECT}.{constants.DLQ_DATASET_NAME}.{table_name}` " \
            f"where datetime(publish_time) > ('{sql_start_time}') and datetime(publish_time) <= ('{sql_end_time}')"

    logging.info(f"Running DLQ Replay for query: {query}, Pubsub Topic name: {pubsub_topic}")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        get_dlq_data_from_bq = (
                pipeline | f'ReadDataFromBigQuery{table_name}' >>
                beam.io.ReadFromBigQuery(query=query, use_standard_sql=True, project=constants.PROJECT,
                                         gcs_location=constants.TEMP_LOCATION)
                | 'GetRawPayload' >> beam.Map(lambda x: x['data'])
                | 'ConvertToDict' >> beam.Map(lambda x: json.loads(x))
                | 'GetC360ServicePayload' >> beam.Map(lambda x: x['C360ServicePayload'])
        )

        time_stamp = datetime.datetime.now().strftime("%d-%m-%Y-%H-%M-%S-%f")
        _ = get_dlq_data_from_bq | 'WriteDLQRecordToGCS' >> beam.io.WriteToText(
            f'{output_bucket}/{pipeline_name}/{pipeline_name}-{time_stamp}', file_name_suffix='.txt'
        )

        input_data_in_batches = get_dlq_data_from_bq | "BatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                             max_batch_size=1000
                                                                                             )
        failed_pubsub_messages = input_data_in_batches | 'PublishToPubSub' >> beam.ParDo(PublishToPubSub(pubsub_topic))

        _ = failed_pubsub_messages | 'WriteFailedPubSubMessageToFile' >> beam.io.WriteToText(
            f'{PUBSUB_FAILURE_FILE_PATH}/{pipeline_name}/{current_date}/{table_name}/{table_name}',
            file_name_suffix='.txt')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument("--pipeline_name",
                        dest="pipeline_name",
                        required=True,
                        help="Pipeline name for which the DLQ remediation needs to run",
                        )

    parser.add_argument("--output_bucket",
                        dest="output_bucket",
                        required=True,
                        help="The GCS bucket to write data to."
                        )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.pipeline_name,
        known_args.output_bucket,
        pipeline_args)
