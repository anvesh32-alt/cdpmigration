import argparse
import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from dofns.publishToPubSub import PublishToPubSub
from helpers import constants, bigqueryDatasetsConfig

PUBSUB_FAILURE_FILE_PATH = 'gs://dbce-c360-isl-preprod-d942-migration-88fakc29/bigquery_to_pubsub_failures'


def run(pipeline_name, mpn, pipeline_args=None):
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    logging.info(f"Starting pipeline with Options: {pipeline_options}")
    current_date = datetime.date.today()

    # get dataset, table name, pubsub topic for respective pipeline name, mpn
    dataset_name = bigqueryDatasetsConfig.datasets_pubsub_config[pipeline_name]['dataset']
    table_name = bigqueryDatasetsConfig.bigquery_table_config[mpn].get(pipeline_name, {}).get('source_table')
    pubsub_topic = bigqueryDatasetsConfig.datasets_pubsub_config[pipeline_name]['pubsub_topic']

    query = f'SELECT raw_payload FROM `{constants.PROJECT}.{dataset_name}.{table_name}`'

    logging.info(f"Reading from Bigquery, query: {query}, Pubsub Topic name: {pubsub_topic}, "
                 f"pipeline name: {pipeline_name}")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        get_data_from_bq = (pipeline | f"ReadDataFrom{table_name}" >> beam.io.ReadFromBigQuery(query=query,
                                                                                               use_standard_sql=True,
                                                                                               project=constants.PROJECT,
                                                                                               gcs_location=constants.
                                                                                               TEMP_LOCATION)
                            | "GetRawPayload" >> beam.Map(lambda x: x['raw_payload'])
                            )

        input_data_in_batches = get_data_from_bq | "BatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                         max_batch_size=1000
                                                                                         )
        failed_pubsub_messages = input_data_in_batches | 'PublishToPubSub' >> beam.ParDo(PublishToPubSub(pubsub_topic))

        _ = failed_pubsub_messages | 'WriteFailedPubSubMessageToFile' >> beam.io.WriteToText(
            f'{PUBSUB_FAILURE_FILE_PATH}/{current_date}/{table_name}', file_name_suffix='.txt')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()

    parser.add_argument("--pipeline_name",
                        dest="pipeline_name",
                        required=True,
                        help="Pipeline name for which data needs to be read and processed",
                        )

    parser.add_argument("--mpn",
                        dest="mpn",
                        required=True,
                        help="Mpn(Marketing program number) for which pipeline should run."
                        )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args.pipeline_name,
        known_args.mpn,
        pipeline_args)
