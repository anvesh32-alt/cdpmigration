import apache_beam as beam
import json
import logging
from google.cloud import bigquery

from helpers import constants


class AddSourceIdToPayloads(beam.DoFn):
    def __init__(self):
        self.source_id_validation_data_from_cds = None

    def setup(self):
        self.source_id_validation_data_from_cds = self.get_source_id_validation_data_from_cds()
    
    
    def get_source_id_validation_data_from_cds(self):

        source_id_query = '''SELECT
         distinct segment_source_id,pg_data_source_id,	
        FROM
        `dbce-c360-segglblws-prod-b902.CDS.pg_data_source_segment_source`'''
        try:
            logging.info(f"Getting values from CDS table for SourceId/MPN validation query: {source_id_query}")
            client = bigquery.Client(project=constants.PROJECT)
            self.source_id_validation_data_from_cds = client.query(source_id_query). \
                to_dataframe().to_dict(orient='index')

            logging.info(f"Values from CDS table for SourceId/MPN validation fetched successfully,"
                         f"record count: {len(self.source_id_validation_data_from_cds)}")
            return self.source_id_validation_data_from_cds

        except Exception as err:
            logging.exception(f"Error while getting records from CDS table error for SourceId/MPN validation: {err}")

    def add_source_id_for_cot(self, data):
        try:
            source_id = {
                "collection": "users",
                "encoding": "none",
                "id": "1174",
                "type": "sourceId"
            }
            count_failed=0

            external_id = data['context']['externalIds']
            for field in external_id:
                if (field['type'] == 'sourceId'):
                    field['id'] = "1174"
                else:
                    count_failed = count_failed + 1

            if count_failed == len(external_id):
                external_id.append(source_id)
        except Exception as e:
            logging.info("exception at externalId")

    def validate_and_add_protocol_source_id(self, data):
        check_source_id=0
        external_id = data['context']['externalIds']
        for field in external_id:
            if (field['type'] == 'sourceId'):
                check_source_id=check_source_id+1
        if check_source_id == 0 and 'protocols' in data['context']:
            protocol_source_id = data['context']['protocols']['sourceId']
            for row, col in self.source_id_validation_data_from_cds.items():
                if protocol_source_id == col['segment_source_id']:
                    source_id = {
                        "collection": "users",
                        "encoding": "none",
                        "id": str(col['pg_data_source_id']),
                        "type": "sourceId"
                    }
            external_id.append(source_id)

    def process(self, element):
        try:
            data = json.loads(element)
            external_id = data['context']['externalIds']
            marketing_program_number=''

            for field in external_id:
                if (field['type'] == 'marketingProgramNumber'):
                    marketing_program_number = field['id']
            for key,value in data['properties'].items():
                if marketing_program_number=='9999' and 'OptIndicator' in key:
                    if data['type']=='track' and value=='false':
                            self.add_source_id_for_cot(data)
                    elif (data['type']=='track' and value=='true') or data['type']=='identify':
                        self.validate_and_add_protocol_source_id(data)
            yield json.dumps(data)
        except KeyError:
            yield element


