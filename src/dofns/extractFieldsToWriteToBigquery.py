import datetime
import json

import apache_beam as beam
from utils.getValueFromTraitsOrProperties import check_key_exists


class ExtractFieldsToWriteToBigquery(beam.DoFn):

    def __init__(self):
        self.KEY_ERROR_MESSAGE = "KEY_NOT_FOUND"

    def process(self, element, *args, **kwargs):

        data = json.loads(element)

        timestamp = self.validate_and_update_payload(data.get('timestamp', self.KEY_ERROR_MESSAGE))
        event_type = self.validate_and_update_payload(data.get('type', self.KEY_ERROR_MESSAGE))
        message_id = self.validate_and_update_payload(data.get('messageId', self.KEY_ERROR_MESSAGE))
        raw_payload = element

        marketing_program_number, source_id = self.KEY_ERROR_MESSAGE, self.KEY_ERROR_MESSAGE

        external_id_payload = check_key_exists(data, 'context', 'externalIds')
        if external_id_payload:
            marketing_program_number = next((item for item in external_id_payload
                                             if item["type"] == 'marketingProgramNumber'), self.KEY_ERROR_MESSAGE)

            if isinstance(marketing_program_number, dict):
                marketing_program_number = self.validate_and_update_payload(
                    marketing_program_number.get('id', self.KEY_ERROR_MESSAGE))

            source_id = next((item for item in external_id_payload if item["type"] == 'sourceId'),
                             self.KEY_ERROR_MESSAGE)

            if isinstance(source_id, dict):
                source_id = self.validate_and_update_payload(source_id.get('id', self.KEY_ERROR_MESSAGE))

        final_payload = {"raw_payload": raw_payload, 'event_type': event_type,
                         'marketing_program_number': marketing_program_number,
                         'source_id': source_id, 'message_id': message_id, 'payload_timestamp': timestamp,
                         'loaded_date': datetime.datetime.utcnow()}

        yield final_payload

    def validate_and_update_payload(self, data):

        if isinstance(data, int):
            return str(data)

        elif isinstance(data, str) and (not data or data.isspace()):
            return None

        return data
