import hashlib
import json
import logging

import apache_beam as beam


class AddEmailToExternalIds(beam.DoFn):
    """
    Extension of beam DoFn to Add email id to External id field.
    """

    def __init__(self, data_type='registration_data'):
        """
        Constructs all the necessary attributes for the AddEmailToExternalIds object.
        :param data_type:  registration_data or clt_data by default runs for registration_data
        """

        self.data_type = data_type

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

    def update_external_id_with_email_id(self, payload, email_id):
        """
        Update the external id field with corresponding email id.
        :param payload:
        :param email_id: email id to be updated in the external id block
        :return: updated payload with email id field
        """

        _email_base_payload = {
            "collection": "users",
            "encoding": "none",
            "id": email_id,
            "type": "email"
        }

        payload['context']['externalIds'].append(_email_base_payload)

        return payload

    def create_hash_email(self, payload):
        """
        Creates a md5 hash of the entire payload
        :param payload:
        :return: md5 hash of the payload
        """

        _email_id_domain = '@c360tocdp2.com'

        md5_hash = hashlib.md5(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()
        _ = self.update_external_id_with_email_id(payload, f'{md5_hash}{_email_id_domain}')

        return _

    def check_email_in_properties(self, data):

        email_id_from_properties = self.check_key_exists(data, 'properties', 'email')
        if email_id_from_properties:
            updated_payload = self.update_external_id_with_email_id(data, email_id_from_properties)
            return updated_payload

        return None

    def process(self, element, *args, **kwargs):

        data = json.loads(element)

        try:
            if self.data_type == 'registration_data':
                email_id_from_traits = self.check_key_exists(data, 'traits', 'email')

                if email_id_from_traits:
                    updated_payload = self.update_external_id_with_email_id(data, email_id_from_traits)
                    yield json.dumps(updated_payload)

                else:
                    payload_with_email_id_from_properties = self.check_email_in_properties(data)
                    if payload_with_email_id_from_properties:
                        yield json.dumps(payload_with_email_id_from_properties)
                    else:
                        updated_payload = self.create_hash_email(data)
                        yield json.dumps(updated_payload)

            elif self.data_type == 'clt_data':

                payload_with_email_id_from_properties = self.check_email_in_properties(data)
                if payload_with_email_id_from_properties:
                    yield json.dumps(payload_with_email_id_from_properties)
                else:
                    updated_payload = self.create_hash_email(data)
                    yield json.dumps(updated_payload)

        except KeyError:
            yield element

        except Exception as err:
            logging.exception(f"Unable to Add email to External Id field err: {err}")


