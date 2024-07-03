import copy
import json
import logging

import apache_beam as beam


class AddMpnToExternalIds(beam.DoFn):
    """
    Extension of beam DoFn to Add mpn to External id field.
    """

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

    def update_external_id_with_mpn_number(self, payload, mpn):
        """
        Update the external id field with corresponding marketing program number.
        :param payload:
        :param mpn: mpn to be updated in the external id block
        :return: updated payload with mpn field
        """

        _mpn_base_payload = {
            "collection": "users",
            "encoding": "none",
            "id": mpn,
            "type": "marketingProgramNumber"
        }
        try:
            payload['context']['externalIds'].append(_mpn_base_payload)
            return payload

        except KeyError:
            _ = {'externalIds': [_mpn_base_payload]}
            if 'context' not in payload.keys():
                payload['context'] = _
            else:
                payload['context'].update(_)
            return payload

    def extract_mpn_from_traits_or_properties(self, payload):
        """
        Checks if mpn exists in traits/properties block or not, if present then updates the external id block.
        :param payload:
        :return: updated payload with mpn field in external id block
        """

        extracted_mpn = self.check_key_exists(payload, 'traits', 'marketingProgramNumber')
        if extracted_mpn:
            payload = self.update_external_id_with_mpn_number(payload, extracted_mpn)

        else:
            extracted_mpn = self.check_key_exists(payload, 'properties', 'marketingProgramNumber')
            if extracted_mpn:
                payload = self.update_external_id_with_mpn_number(payload, extracted_mpn)

        return payload, extracted_mpn

    def process(self, element, *args, **kwargs):

        data = json.loads(element)
        copy_of_data = copy.deepcopy(data)

        try:
            external_id_payload = self.check_key_exists(data, 'context', 'externalIds')
            if external_id_payload:
                external_id_payload_mpn = next((item for item in external_id_payload
                                                if item["type"] == "marketingProgramNumber"), False)

                if not external_id_payload_mpn:
                    updated_payload, extracted_mpn = self.extract_mpn_from_traits_or_properties(data)
                    yield json.dumps(updated_payload)
                else:
                    _, extracted_mpn = self.extract_mpn_from_traits_or_properties(data)
                    if extracted_mpn:
                        copy_of_data['context']['externalIds'].remove(external_id_payload_mpn)
                        updated_payload = self.update_external_id_with_mpn_number(copy_of_data, extracted_mpn)
                        yield json.dumps(updated_payload)
                    else:
                        yield element
            else:
                updated_payload, extracted_mpn = self.extract_mpn_from_traits_or_properties(data)
                yield json.dumps(updated_payload)

        except KeyError:
            yield element

        except Exception as err:
            logging.exception(f"Unable to Add Mpn to External Id field err: {err}")