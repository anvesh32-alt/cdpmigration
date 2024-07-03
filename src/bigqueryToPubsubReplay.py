import argparse
import logging
import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from dofns.replayToPubSub import ReplayToPubSub
from apache_beam.io import BigQueryDisposition
from helpers import constants, bigqueryDatasetsConfig

PUBSUB_FAILURE_FILE_PATH = 'gs://dbce-c360-isl-preprod-d942-migration-88fakc29/bigquery_to_pubsub_failures'


def run():
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    logging.info(f"Starting pipeline with Options: {pipeline_options}")

    current_date = datetime.date.today()

    query = f"SELECT message_id,mpn,trace_id,raw_payload,replay_topic FROM `{constants.PROJECT}.Migration_DLQ.dlq_auto_replay_table` where replay_status='ToBeReplayed'"

    logging.info(f"Reading from Bigquery, query: {query}")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        get_data_from_bq = (pipeline | f"ReadDataFrom" >> beam.io.ReadFromBigQuery(query=query,
                                                                                               use_standard_sql=True,
                                                                                   project=constants.PROJECT,
                                                                                   gcs_location=constants.
                                                                                   TEMP_LOCATION)
                            | "GetRawPayload" >> beam.Map(lambda x: x)
                            )

        input_data_in_batches = get_data_from_bq | "BatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                         max_batch_size=1000
                                                                                         )
        replay_pubsub_messages = input_data_in_batches | 'PublishToPubSub' >> beam.ParDo(ReplayToPubSub())

        _ = replay_pubsub_messages | 'WriteReplayedPubSubMessageToBigQ' >> beam.io.WriteToBigQuery(
                        table="dlq_auto_replay_status",
                        dataset="Migration_DLQ",
                        project=constants.PROJECT,
                        custom_gcs_temp_location=constants.TEMP_LOCATION,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE
                    )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    run()
