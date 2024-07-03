import logging
import json

from google.cloud import storage
from helpers import constants


class ReadConfig:
    """
    Reads the config json file and return values accordingly based on mpn passed
    """

    def __init__(self, mpn):
        """
        Constructs all the necessary attributes for the ReadConfig object.
        :param mpn : str
            marketing program number
        """

        self.mpn = mpn

    def read_config_file(self):
        """
        Reads the json file and return the configuration based on mpn
        :return dict: Configuration values to be used for a given mpn
        """

        try:
            # todo: better handling of error
            client = storage.Client(constants.PROJECT)
            bucket = client.get_bucket(constants.DAG_BUCKET)
            config = json.loads(
                bucket.get_blob(constants.CONFIG_FILE_PATH + constants.RETENTION_CONFIG_FILE).download_as_string()
            )

            return config[self.mpn]

        except Exception as err:
            logging.error(f'Unable to get configuration, err: {str(err)}')
