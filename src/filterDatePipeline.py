import logging
import datetime
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils import  userPipelineOptions
from helpers import constants
from dofns import filterBackdatedRecords,publishToPubSub,parseTimestamp,addEmailToExternalIds,addMpnToExternalIds
# from datetime import datetime

date_fields =['originalTimestamp', 'timestamp', 'postalConsumerChoiceDate', 'socialConsumerChoiceDate', 'emailConsumerChoiceDate', 'phoneConsumerChoiceDate', 'regulatoryConsumerChoiceDate']
def run():
    pipeline_options = PipelineOptions(save_main_session=True)
    options = pipeline_options.view_as(userPipelineOptions.UserPipelineOptions)
    logging.info(f"Begin pipeline with options: {options}")
    current_date = datetime.date.today()

    with beam.Pipeline(options=options) as pipeline:
        clickstream_data =(
            pipeline | "ReadClickstreamData" >> beam.io.ReadFromText(options.input_bucket + '/externalid-output-clickstream/'
                                                                         + '*.txt')
                    | "MapClickstreamData" >> beam.Map(lambda x : x)
        )
        clt_data =(
            pipeline | "ReadCltData" >> beam.io.ReadFromText(options.input_bucket + '/externalid-output-clt/'
                                                                 + '*.txt')
                    | "MapCltData" >> beam.Map(lambda x : x)
                    
        )
        other_events_data =(
            pipeline | "ReadOtherData" >> beam.io.ReadFromText(options.input_bucket + '/externalid-output-other-events/'
                                                                 + '*.txt')
                    | "MapOtherData" >> beam.Map(lambda x : x)
                    
        )
        filter_back_dated_clickstream_records = clickstream_data | "FilterBackDatedCLKRecords" >> beam.ParDo(
            filterBackdatedRecords.FilterBackDatedRecords(current_date, filter_anonymous_id=True))

        clickstream_parse_timestamp = (
                filter_back_dated_clickstream_records | "ParsetimeCLKData" >> beam.ParDo(parseTimestamp.validateTimestamp(date_fields))
                                                      | "AddMpnToExternalIdToCLKData" >> beam.ParDo(addMpnToExternalIds.AddMpnToExternalIds())
        )

        filter_back_dated_clt_records = clt_data | "FilterBackDatedCLTRecords" >> beam.ParDo(
            filterBackdatedRecords.FilterBackDatedRecords(current_date))

        clt_parse_email = (
                filter_back_dated_clt_records | "ParsetimeCLTData" >> beam.ParDo(parseTimestamp.validateTimestamp(date_fields))
                | "AddEmailToExternalIdToCLTData" >> beam.ParDo(addEmailToExternalIds.AddEmailToExternalIds(data_type='clt_data'))
                | "AddMpnToExternalIdToCLTData" >> beam.ParDo(addMpnToExternalIds.AddMpnToExternalIds())
        )

        other_events_parse_timestamp = (
                other_events_data | "ParsetimeOtherData" >> beam.ParDo(parseTimestamp.validateTimestamp(date_fields))
                                
        )

        time_stamp = current_date

        (

            clickstream_parse_timestamp | f'WriteOutFilesClickstreamData' >> beam.io.WriteToText(
                                                                            f'{options.output_bucket}'
                                                                            f'/filter-date-clickstream/'
                                                                            f'filterdate-clkstream-{time_stamp}',
                                                                            file_name_suffix='.txt')

        )
        (

            clt_parse_email | f'WriteOutFilesCLTData' >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                         f'/filter-date-clt/'
                                                                         f'filterdate-clt-{time_stamp}',
                                                                         file_name_suffix='.txt')

        )


        clickstream_data_in_batches =  clickstream_parse_timestamp | \
                                      "ClickstreamBatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                                max_batch_size=1000)
        failed_clickstream_pubsub_messages = clickstream_data_in_batches | \
                                             'PublishToPubSubClickstream' >> beam.ParDo(
                                                                                        publishToPubSub.PublishToPubSub
                                                                                        (constants.CLK_PUBSUB_TOPIC))
        failed_clickstream_pubsub_messages | 'WriteFailedPubSubMessageToClickstreamFile' >> beam.io.WriteToText(
                                                                                            f'{options.output_bucket}'
                                                                                            f'/filter-date-clickstream'
                                                                                            f'/failed_clk_pubsub'
                                                                                            'failed_pubsub_messages',
                                                                                            file_name_suffix='.txt')
        clt_data_in_batches = clt_parse_email | "CLTBatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                         max_batch_size=1000
                                                                                         )
        failed_clt_pubsub_messages = clt_data_in_batches | 'PublishToPubSubCLTData' >> beam.ParDo(
                                                                                        publishToPubSub.PublishToPubSub
                                                                                        (constants.CLT_PUBSUB_TOPIC))
        failed_clt_pubsub_messages | 'WriteFailedPubSubMessageToCLTFile' >> beam.io.WriteToText(f'{options.output_bucket}'
                                                                                                f'/filter-date-clt'
                                                                                                f'/failed_clt_pubsub'
                                                                                                'failed_pubsub_messages',
                                                                                                file_name_suffix='.txt')

        # --------------------------------------------------------------------------------------------------------------

        other_events_data_in_batches = other_events_parse_timestamp | "OtherEventsBatchElements" >> beam.BatchElements(
                                                                                min_batch_size=100,max_batch_size=1000)
        failed_other_events_pubsub_messages = other_events_data_in_batches | \
                                              'PublishToPubSubOtherEventData' >> beam.ParDo(
                                                                                        publishToPubSub.PublishToPubSub
                                                                                        (constants.OTHER_PUBSUB_TOPIC))
        failed_other_events_pubsub_messages | 'WriteFailedPubSubMessageToOtherEventFile' >> beam.io.WriteToText(
                                                                                            f'{options.output_bucket}'
                                                                                            f'/filter-date-other-events'
                                                                                            f'/failed_other_pubsub'
                                                                                            'failed_pubsub_messages',
                                                                                            file_name_suffix='.txt')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()