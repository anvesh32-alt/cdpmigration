import argparse
import logging
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from helpers import bigqueryDatasetsConfig
from dofns.publishToPubSub import PublishToPubSub
from dofns import extractId

PUBSUB_FAILURE_FILE_PATH = 'gs://dbce-c360-isl-preprod-d942-migration-88fakc29/bigquery_to_pubsub_failures'


def run(pipeline_name, input_bucket, pipeline_args=None):
    """
    Main Entry point of the GCS to Downstream Pubsub pipeline code
    :param pipeline_name: Type of data for which pipeline needs to be triggered
    :param input_bucket: Input Bucket from where data needs to be read to process to downstream
    :param pipeline_args:
    :return: None
    """

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    logging.info(f"Starting pipeline with Options: {pipeline_options}")
    current_date = datetime.date.today()

    # Read pubsub topic name and gcs input folder name
    pubsub_topic = bigqueryDatasetsConfig.datasets_pubsub_config[pipeline_name]['pubsub_topic']
    input_gcs_folder = bigqueryDatasetsConfig.datasets_pubsub_config[pipeline_name]['input_gcs_folder']

    input_gcs_path = f'{input_bucket}/{input_gcs_folder}/*.txt'
    format_pipeline_name = ''.join(x.capitalize() or '_' for x in pipeline_name.split('_'))

    logging.info(f"Pipeline name: {pipeline_name} Pubsub Topic name: {pubsub_topic}, Input GCS path: {input_gcs_path}")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        input_data = (
                pipeline | f"Read{format_pipeline_name}" >> beam.io.ReadFromText(input_gcs_path)
                | f"Map{format_pipeline_name}" >> beam.Map(lambda x: x)
        )

        # extract latest event for each user
        get_latest_event = (
                input_data | "MapUserIdAndEventType" >> beam.ParDo(extractId.ExtractIdForGroupByOnEvents())
                | "GroupRecordsOnUserAndEventType" >> beam.GroupByKey()
                | "FilterLatestRecords" >> beam.ParDo(extractId.GetLatestRecordsForEachUser())
        )

        input_data_in_batches = get_latest_event | "BatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                         max_batch_size=1000)

        failed_pubsub_messages = input_data_in_batches | f'Publish{format_pipeline_name}ToPubSub' >> beam.ParDo(
            PublishToPubSub(pubsub_topic))

        _ = failed_pubsub_messages | 'WriteFailedPubSubMessageToFile' >> beam.io.WriteToText(
            f'{PUBSUB_FAILURE_FILE_PATH}/{current_date}', file_name_suffix='.txt')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument("--pipeline_name",
                        dest="pipeline_name",
                        required=True,
                        help="Pipeline name for which data needs to be read and processed",
                        )

    parser.add_argument("--input_bucket",
                        dest="input_bucket",
                        required=True,
                        help="Input GCS bucket for which pipeline should run."
                        )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.pipeline_name, known_args.input_bucket, pipeline_args)
