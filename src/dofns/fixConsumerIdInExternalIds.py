import hashlib
import json
import logging
import re

import apache_beam as beam


class FixConsumerIdInExternalIds(beam.DoFn):

    def __init__(self):
        self.event_types = ['CRM Profile Email Signup', 'Signed Up for Promotion', 'Signed Up for Waitlist - CLT']

    def check_key_exists(self, element, *keys):
        """
        Check if *keys (nested) exists in `element` (dict).
        """

        if len(keys) == 0:
            raise AttributeError('keys_exists() expects at least two arguments, one given.')

        _element = element
        for key in keys:
            try:
                _element = _element[key]
            except Exception:
                return False
        return _element

    def check_keys_in_traits_and_properties(self, element, key):

        key_value = self.check_key_exists(element, 'traits', key)
        if not key_value:
            key_value = self.check_key_exists(element, 'properties', key)

        if not key_value:
            return False
        return True

    def validate_and_remove_consumer_id(self, data, consumer_id_payload):

        date_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
        if re.search(date_pattern, consumer_id_payload['id']):
            data['context']['externalIds'].remove(consumer_id_payload)
            return data, True

        return data, False

    def create_consumer_id(self, data, email_id_from_external_id):

        _consumer_id = {
                "collections": "users",
                "encoding": "none",
                "id": '',
                "type": "consumerId"
            }

        if email_id_from_external_id:
            email_sha = hashlib.sha256(email_id_from_external_id['id'].encode()).hexdigest()
            _consumer_id['id'] = email_sha + data['originalTimestamp']
            data['context']['externalIds'].append(_consumer_id)

        return data

    def check_offline_consumer_id_exists(self, data, external_id_payload):

        if not self.check_keys_in_traits_and_properties(data, 'offlineConsumerId'):
            email_id_from_external_id = next((item for item in external_id_payload if item["type"] == "email"), False)
            data = self.create_consumer_id(data, email_id_from_external_id)

        return data

    def main(self, data, external_id_payload):

        consumer_id_payload = next((item for item in external_id_payload if item["type"] == "consumerId"), False)

        if consumer_id_payload and data.get('event', None) in self.event_types:
            _payload, status = self.validate_and_remove_consumer_id(data, consumer_id_payload)
        elif consumer_id_payload:
            _payload, status = self.validate_and_remove_consumer_id(data, consumer_id_payload)
            if status:
                _payload = self.check_offline_consumer_id_exists(_payload, external_id_payload)
        elif not consumer_id_payload:
            _payload = self.check_offline_consumer_id_exists(data, external_id_payload)

        return _payload

    def process(self, element, *args, **kwargs):

        data = json.loads(element)

        try:
            external_id_payload = self.check_key_exists(data, 'context', 'externalIds')
            if external_id_payload:
                updated_payload = self.main(data, external_id_payload)
                yield json.dumps(updated_payload)
            else:
                yield element
        except KeyError:
            yield element

        except Exception as err:
            logging.exception(f"Unable to Fix ConsumerId to External Id field err: {err}")