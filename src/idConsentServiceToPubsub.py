import datetime
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils import preprodPipelineOptions
from helpers import constants, legalEntityConfig
from dofns import createIdServicePayload, createConsentServicePayload, publishToPubSub

PUBSUB_FAILURE_PATH = 'gs://dbce-c360-isl-preprod-d942-migration-88fakc29/id_consent_pubsub_failure'


def run():
    """
    Main Entry point of the ID Service Dataflow pipeline
    id-service : For each trace_id from traces table pull raw_payload from staging_traces
    consent-service: For each trace_id from profile_consents table pull raw_payload from consents
    """

    pipeline_options = PipelineOptions(save_main_session=True)
    options = pipeline_options.view_as(preprodPipelineOptions.PreprodPipelineOptions)

    logging.info(f"Begin pipeline with options: {options}")

    current_date = datetime.datetime.now().strftime("%d-%m-%Y")

    # Get the legal entity from config file
    legal_entity = legalEntityConfig.legal_entity[options.mpn]['legal_entity']

    # Get the folder name from which avro data will be read for consent or id service pipeline
    avro_export_folder_name = {
        "id_service": {"trace_id": "NA", "raw_payload": "staging_traces_id_service"},
        "consent_service": {"trace_id": "profile_consents", "raw_payload": "consents"}
    }
    get_trace_id = avro_export_folder_name[options.pipeline_name]['trace_id']
    get_raw_payload = avro_export_folder_name[options.pipeline_name]['raw_payload']

    # Build variables for pipeline names
    format_trace_id_pipeline_name = ''.join(x.capitalize() or '_' for x in get_trace_id.split('_'))
    format_raw_payload_pipeline_name = ''.join(x.capitalize() or '_' for x in get_raw_payload.split('_'))

    # Get pubsub topic name from constant file
    pubsub_topic_name = constants.ID_CONSENT_PUBSUB[options.pipeline_name]

    logging.info(f"Reading trace Id from: {get_trace_id}, Raw Payload: {get_raw_payload}, "
                 f"pubsub topic name: {pubsub_topic_name} ")

    with beam.Pipeline(options=pipeline_options) as pipeline:

        if options.pipeline_name == 'consent_service':
            # Get all trace_id from Avro export
            get_trace_id = (
                    pipeline | f'GetTraceIdFrom{format_trace_id_pipeline_name}' >> beam.io.ReadFromAvro(
                                                                                f"{options.input_bucket}/{get_trace_id}/*")
                    | 'ExtractTraceID' >> beam.Map(lambda x: (x['trace_id'], x['trace_id']))
            )

            # Extract raw_payload and other details from Avro export
            get_raw_payload = (
                    pipeline | f'GetRawPayloadFrom{format_raw_payload_pipeline_name}' >> beam.io.ReadFromAvro(
                                                                            f"{options.input_bucket}/{get_raw_payload}/*")
                    | 'ExtractTraceIDFromRawPayload' >> beam.Map(lambda x: (x['trace_id'], x))
            )

            # Get the common trace_id present
            matched_trace_id, unmatched_trace_id = (({'trace_id': get_trace_id, 'raw_payload': get_raw_payload})
                                                    | 'MapCommonTraceId' >> beam.CoGroupByKey()
                                                    | 'PartitionMatchingTraceId|UnmatchedTraceId' >> beam.Partition(
                                                    lambda element, num_elements: 0 if len(element[1]['trace_id'])
                                                    > 0 and len(element[1]['raw_payload']) > 0 else 1, 2)
                                                    )

            # Create a payload corresponding to publish to downstream for consent service
            flatten_raw_payload = matched_trace_id | 'RawPayload' >> beam.Map(lambda x: x[1]['raw_payload'])
            payload_to_pubsub = flatten_raw_payload | 'GenerateConsentServicePayload' >> beam.ParDo(
                createConsentServicePayload.CreateConsentServicePayload())

        # Transform the payload based on id_service or consent_service
        elif options.pipeline_name == 'id_service':

            get_raw_payload = (
                    pipeline | f'GetRawPayloadFrom{format_raw_payload_pipeline_name}' >> beam.io.ReadFromAvro(
                                                                        f"{options.input_bucket}/{get_raw_payload}/*")
            )

            # Create a payload corresponding to publish to downstream for id service
            payload_to_pubsub = get_raw_payload | 'GenerateIdServicePayload' >> beam.ParDo(
                createIdServicePayload.CreateIdServicePayload(legal_entity))

        # Send payloads to pubsub topic
        (
            payload_to_pubsub | "BatchElements" >> beam.BatchElements(min_batch_size=100, max_batch_size=1000)
            | 'PublishToPubSub' >> beam.ParDo(publishToPubSub.PublishToPubSub(pubsub_topic_name))
            | 'WriteFailedPubSubMessageToFile' >> beam.io.WriteToText(f'{PUBSUB_FAILURE_PATH}/{current_date}/'
                                                                      f'{options.pipeline_name}/'
                                                                      f'failed_pubsub_messages',
                                                                      file_name_suffix='.txt')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
