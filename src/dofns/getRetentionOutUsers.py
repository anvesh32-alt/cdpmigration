import apache_beam as beam
import logging
import sys
from google.cloud import bigquery

from helpers import constants


class GetRetentionOutUsers(beam.DoFn):
    """
    Extension of beam DoFn filtering pcollection to get the retention users from bigquery.
    """

    def __init__(self, retention_out_query):
        """
        Constructs all the necessary attributes for the GetRetentionOutUsers object.
        :param retention_out_query : str
                query to get retention users
        """

        self.retention_out_query = retention_out_query
        self.client = None

    def start_bundle(self):
        """
        Initialize the BigQuery client with the project the pipeline is running on
        """

        logging.info(f"Calling Bigquery API to get retention user, query:{self.retention_out_query}")
        self.client = bigquery.Client(project=constants.PROJECT)

    def process(self, element):
        """
        Gets the retention users from bigquery
        :param element: pcollection
        return :pcollection with mapping of {"user_id": row['user_id']}
        """

        try:
            query = self.client.query(self.retention_out_query)
            result = query.result()
            for row in result:
                yield {"user_id": row['user_id']}

        except Exception as err:
            logging.error(f"Failed to get retention users due to error: {str(err)} exiting..")
            sys.exit(1)
