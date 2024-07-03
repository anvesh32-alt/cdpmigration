import logging

import apache_beam as beam
from google.cloud import pubsub_v1
from concurrent import futures


class PublishToPubSub(beam.DoFn):
    """
    Extension of beam DoFn to Publish messages to Pubsub topic.
    """

    def __init__(self, topic_name):
        """
        Constructs all the necessary attributes for the PublishToPubSub object.
        :param topic_name: str -> topic name of the pubsub for which payloads needs to pushed
        """

        self.topic_name = topic_name
        self.success_count = beam.metrics.Metrics.counter(self.__class__, 'TotalRecordsProcessed')
        self.publisher = None

    def setup(self):
        """ Initialize the PubSub client """

        logging.info(f"Initializing the PubSub Client for topic {self.topic_name}")
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
                        failed_pubsub_messages = input_data_in_batches | 'PublishToPubSub' >> beam.ParDo(PublishToPubSub
                                                                                            (topic_name))
                        _ = failed_pubsub_messages | 'WriteFailedPubSubMessageToFile' >> beam.io.WriteToText(
                                                                                         'failed_pubsub_messages',
                                                                                         file_name_suffix='.txt')

        :return: element in case if the message is not published to the pubsub topic which can be written to a file to
                 process again.
        """

        # list to keep a track of all the futures of the messages
        track_payload_futures = []

        try:
            # Iterate over the data and push to pubsub since we are receiving batches of pcollection
            for data in element:
                track_payload_futures.append(
                    self.publisher.publish(
                        self.topic_name, data=data.encode("utf-8")
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
            except Exception:
                # return failed record to the pipeline which was not published to the pubsub
                yield data

        futures.wait(track_payload_futures, return_when=futures.ALL_COMPLETED)

    def teardown(self):
        """ Log the count of messages published """
        logging.info(f"Total Published Messages: {self.success_count}, to Topic: {self.topic_name} ")


