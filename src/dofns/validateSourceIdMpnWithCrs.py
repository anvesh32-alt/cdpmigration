import datetime
import json
import logging
import apache_beam as beam
from google.cloud import bigquery

from helpers import constants
from utils.getValueFromTraitsOrProperties import check_key_exists


class ValidateSourceIdMpnWithCrs(beam.DoFn):
    """
    Extension of beam DoFn to validate mpn/source id combination.
    """

    def __init__(self):
        self.source_id_mpn_validation_data_from_crs = None

    def setup(self):
        self.source_id_mpn_validation_data_from_crs = self.get_source_id_mpn_validation_data_from_crs()

    def get_source_id_mpn_validation_data_from_crs(self):
        """
        Gets values from table dbce-c360-segglblws-prod-b902.CDS.pgdatasourcempn to validate mpn/source id combination.
        """

        source_id_mpn_query = 'select pg_data_source_id,marketing_program_id ' \
                              'from `dbce-c360-segglblws-prod-b902.CDS.pgdatasourcempn`'
        try:
            logging.info(f"Getting values from CDS table for SourceId/MPN validation query: {source_id_mpn_query}")
            client = bigquery.Client(project=constants.PROJECT)
            self.source_id_mpn_validation_data_from_crs = client.query(source_id_mpn_query). \
                to_dataframe().to_dict(orient='index')

            logging.info(f"Values from CDS table for SourceId/MPN validation fetched successfully,"
                         f"record count: {len(self.source_id_mpn_validation_data_from_crs)}")
            return self.source_id_mpn_validation_data_from_crs

        except Exception as err:
            logging.exception(f"Error while getting records from CDS table error for SourceId/MPN validation: {err}")

    def process(self, element, *args, **kwargs):

        data = json.loads(element)

        try:
            external_id_payload = check_key_exists(data['raw_payload'], 'context', 'externalIds')
            if external_id_payload:
                source_id_block = next((item for item in external_id_payload
                                        if item["type"] == "sourceId"), {})

                mpn_block = next((item for item in external_id_payload
                                  if item["type"] == "marketingProgramNumber"), {})

                if source_id_block or mpn_block:
                    source_id = source_id_block.get('id', None)
                    mpn = mpn_block.get('id', None)
                    if mpn != '9999':
                        payload = self.validate_source_id_mpn_combination(data, source_id, mpn)
                        yield json.dumps(payload)
                    else:
                        yield element
                else:
                    yield element
            else:
                yield element
        except KeyError:
            yield element

        except Exception as err:
            logging.exception(f"Unable to Validate SourceId/MPN combination err: {err}")

    def validate_source_id_mpn_combination(self, data, source_id, mpn):
        """
        Iterates through data fetched from table to validate mpn/source id combination
        """

        if source_id or mpn:
            for row_index, row_value in self.source_id_mpn_validation_data_from_crs.items():
                if source_id == str(row_value['pg_data_source_id']) and mpn == str(row_value['marketing_program_id']):
                    return data

            failure_status = {'message_id': data['raw_payload']['messageId'],
                              'mpn': mpn,
                              'dq_validation_id': "VLD9",
                              'dq_validation_description': 'Validate if Source Id MPN Combination exists in CRS',
                              'error_details': json.dumps({'sourceId': source_id, 'marketingProgramNumber': mpn}),
                              'reported_at': datetime.datetime.now().isoformat()
                              }

            data.setdefault('validation_status', []).append(failure_status)

            return data
