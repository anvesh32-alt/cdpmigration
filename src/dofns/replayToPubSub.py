import logging
import json
import apache_beam as beam
from google.cloud import pubsub_v1
from concurrent import futures
import datetime


class ReplayToPubSub(beam.DoFn):

    """
    Extension of beam DoFn to Publish messages to Pubsub topic.
    """

    def __init__(self):
        """
        Constructs all the necessary attributes for the PublishToPubSub object.
        """
        self.success_count = beam.metrics.Metrics.counter(self.__class__, 'TotalRecordsProcessed')

    def setup(self):
        """ Initialize the PubSub client """

        logging.info(f"Initializing the PubSub Client for topic")
        self.publisher = pubsub_v1.PublisherClient()

    def process(self, element):
        """
        Publishes the messages of the pcollection to pubsub topic by keeping a track of failure messages
        :param element: batches of pcollection should be between 100-1000 as higher batches may result in the client to
                        be overloaded which will degraded the performance, payloads should be in json format.

                        Sample code to call PublishToPubSub ParDo in batches and write failed records:
                        input_data_in_batches = input_data | "BatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                 max_batch_size=1000
                                                                                 )
                        replay_pubsub_messages = input_data_in_batches | 'PublishToPubSub' >> beam.ParDo(PublishToPubSub
                                                                                            (topic_name))
                        _ = replay_pubsub_messages | 'WriteReplayedPubSubMessageToBigQ' >> beam.io.WriteToBigQuery(
                        table="dlq_auto_replay_status",
                        dataset="Migration_DLQ",
                        project=constants.PROJECT,
                        custom_gcs_temp_location=constants.TEMP_LOCATION,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE)

        :return: element in case if the message is not published to the pubsub topic which can be written to a file to
                 process again.
        """

        # list to keep a track of all the futures of the messages
        track_payload_futures = []
        current_timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Iterate over the data and push to pubsub since we are receiving batches of pcollection
            for data in element:
                payload=data['raw_payload']
                topic_name=data['replay_topic']
                final_data = json.loads(payload)
                servicePayload=final_data['C360ServicePayload']
                json_string = json.dumps(servicePayload)
                json_data_bytes = json_string.encode('utf-8')
                track_payload_futures.append(
                    self.publisher.publish(
                        topic_name, data=json_data_bytes
                    )
                )
        except Exception as err:
            logging.exception(f"Failed To Publish messages to pubsub topic: {str(err)}")

        # iterate over the future, data of each message and check if it was posted successfully or not, each pubsub
        # message returns a message id(future) if it was successfully posted to the topic
        for future, data in zip(track_payload_futures, element):
            try:
                future.result()
                self.success_count.inc()
                yield {"message_id":data['message_id'], "mpn": data['mpn'],"trace_id":data['trace_id'],
                       "raw_payload":data['raw_payload'],"replay_topic":data['replay_topic'],"replay_status":"Replayed","replay_time":current_timestamp}
            except Exception as err:
                # return failed record to the pipeline which was not published to the pubsub
                logging.exception('Failed to load to Pubsub--' + str(err))
                yield {"message_id":data['message_id'], "mpn": data['mpn'],"trace_id":data['trace_id'],
                       "raw_payload":data['raw_payload'],"replay_topic":data['replay_topic'],"replay_status":"ToBeReplayed"}

        futures.wait(track_payload_futures, return_when=futures.ALL_COMPLETED)

    def teardown(self):
        """ Log the count of messages published """
        logging.info(f"Total Published Messages: {self.success_count}")
