import hashlib
import json
import logging
import apache_beam as beam

from utils.getValueFromTraitsOrProperties import check_key_exists, extract_value_from_traits_and_properties


class AddConsumerIdInExternalIds(beam.DoFn):

    def __init__(self):
        self.event_types = ['CRM Profile Email Signup', 'Signed Up for Promotion', 'Signed Up for Waitlist - CLT']

    def create_consumer_id(self, payload, consumer_id):

        _consumer_id = {
                "collections": "users",
                "encoding": "none",
                "id": str(consumer_id),
                "type": "consumerId"
            }

        try:
            payload['context']['externalIds'].append(_consumer_id)
            return payload

        except KeyError:
            _ = {'externalIds': [_consumer_id]}
            if 'context' not in payload.keys():
                payload['context'] = _
            else:
                payload['context'].update(_)
            return payload

    def check_offline_consumer_id_exists(self, data, external_id_payload):

        if not extract_value_from_traits_and_properties(data, 'offlineConsumerId'):
            data = self.create_consumer_id_with_email_or_phone_sha(data, external_id_payload)

        return data

    def create_consumer_id_with_email_or_phone_sha(self, data, external_id_payload):

        # Creates the consumer id from email sha + source id, if source id is not present then from originalTimestamp
        email_id_from_external_id = next((item for item in external_id_payload if item["type"] == "email"), False)
        if email_id_from_external_id and email_id_from_external_id['id'] and \
                not email_id_from_external_id['id'].isspace():
            _email_sha = hashlib.sha256(email_id_from_external_id['id'].encode()).hexdigest()

            source_id_from_external_id = next((item for item in external_id_payload if item["type"] == "sourceId"), False)
            if source_id_from_external_id and source_id_from_external_id['id'] and \
                    not source_id_from_external_id['id'].isspace():
                email_sha = _email_sha + str(source_id_from_external_id['id'])
            else:
                email_sha = _email_sha + data['originalTimestamp']

            data = self.create_consumer_id(data, email_sha)

        else:
            # Creates the consumer id from phone sha + source id,if source id is not present then from originalTimestamp
            phone_id_from_external_id = next((item for item in external_id_payload if item["type"] == "phoneNumber"), False)
            if phone_id_from_external_id and phone_id_from_external_id['id'] and \
                    not phone_id_from_external_id['id'].isspace():
                _phone_sha = hashlib.sha256(phone_id_from_external_id['id'].encode()).hexdigest()

            else:
                phone_number_from_traits_or_properties = extract_value_from_traits_and_properties(data, "phoneNumber")
                if phone_number_from_traits_or_properties:
                    _phone_sha = hashlib.sha256(phone_number_from_traits_or_properties.encode()).hexdigest()
                    source_id_from_external_id = next((item for item in external_id_payload if item["type"] == "sourceId"),
                                                      False)
                    if source_id_from_external_id and source_id_from_external_id['id'] and \
                            not source_id_from_external_id['id'].isspace():
                        phone_sha = _phone_sha + str(source_id_from_external_id['id'])
                    else:
                        phone_sha = _phone_sha + data['originalTimestamp']

                    data = self.create_consumer_id(data, phone_sha)

        return data

    def extract_and_update_consumer_id(self, payload):

        value_from_traits_or_properties = extract_value_from_traits_and_properties(payload, 'consumerId')
        if value_from_traits_or_properties:
            payload = self.create_consumer_id(payload, value_from_traits_or_properties)
            return payload, True

        return payload, False

    def main(self, data, external_id_payload):

        consumer_id_payload = next((item for item in external_id_payload if item["type"] == "consumerId"), False)

        if not consumer_id_payload and data.get('event', None) in self.event_types:
            data, status = self.extract_and_update_consumer_id(data)
        elif not consumer_id_payload:
            data, status = self.extract_and_update_consumer_id(data)
            if not status:
                data = self.check_offline_consumer_id_exists(data, external_id_payload)
        elif 'id' in consumer_id_payload :
            if (consumer_id_payload['id'] is None or (isinstance(consumer_id_payload['id'], str) and
                                                      consumer_id_payload['id'].isspace()) or
                    consumer_id_payload['id'] == ""):
                data, status = self.extract_and_update_consumer_id(data)
                data['context']['externalIds'].remove(consumer_id_payload)
                if not status:
                    data = self.check_offline_consumer_id_exists(data, external_id_payload)

        return data

    def process(self, element, *args, **kwargs):

        data = json.loads(element)

        try:
            external_id_payload = check_key_exists(data, 'context', 'externalIds')
            if external_id_payload:
                updated_payload = self.main(data, external_id_payload)
                yield json.dumps(updated_payload)
            else:
                updated_payload = self.extract_and_update_consumer_id(data)
                yield json.dumps(updated_payload)
        except KeyError:
            yield element

        except Exception as err:
            logging.exception(f"Unable to Fix ConsumerId to External Id field err: {err}")