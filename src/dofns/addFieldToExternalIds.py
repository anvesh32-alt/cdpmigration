import json
import logging
import apache_beam as beam

from utils.getValueFromTraitsOrProperties import check_key_exists, extract_value_from_traits_and_properties


class AddFieldToExternalIds(beam.DoFn):
    """
    Extension of beam DoFn to Add different fields to External id field.
    """

    def __init__(self, country_code):
        """
         Constructs all the necessary attributes for the AddFieldToExternalIds object.
        :param country_code: default country code to populated for a given mpn
        """
        self.country_code = country_code
        self.key_to_update = ['email', 'sourceId', 'marketingProgramNumber', 'countryCode']

    def process(self, element, *args, **kwargs):
        """
        Entry point for AddFieldToExternalIds class which validates if corresponding field exists in external id
        block or not. If it doesn't exist then it checks in traits/properties block and update accordingly.
        At last if it's not found there then its check in bigquery lookup and update accordingly.
        :param element: records in dict format
        :param args:
        :param kwargs:
        :return: pcollection in json format to be passed for next step in pipeline.
        """

        bigquery_lookup_value = {key: value for data in element['map_bq_data'] for key, value in data.items()}
        payload = self.remove_empty_dicts(element['map_gcs_data'])

        try:
            for key_to_update in self.key_to_update:
                updated_payload = self.add_field_to_external_id(payload,
                                                                bigquery_lookup_value.get(key_to_update, None),
                                                                key_to_update)
            yield json.dumps(updated_payload)
        except KeyError:
            logging.exception(f"Unable to Add/Update fields to External Id field err at Keyerror: {element}")
            yield json.dumps(element['map_gcs_data'])

        except Exception as err:
            logging.exception(f"Unable to Add/Update fields to External Id field err: {err}")
            yield json.dumps(element['map_gcs_data'])

    def add_field_to_external_id(self, payload, bigquery_lookup_value, key_to_update):
        """
        Validates if specific key exists in externalIds block and calls corresponding functions.
        """

        external_id_payload = check_key_exists(payload, 'context', 'externalIds')
        if external_id_payload:
            external_id_field = next((item for item in external_id_payload
                                      if item["type"] == key_to_update), False)
            if not external_id_field:
                payload = self.validate_and_update_payload(payload, bigquery_lookup_value, key_to_update)
            elif 'id' in external_id_field :
                if external_id_field['id'] is None or  external_id_field['id'] == "" or (isinstance(external_id_field['id'], str) and external_id_field['id'].isspace()) :
                    payload['context']['externalIds'].remove(external_id_field)
                    payload = self.validate_and_update_payload(payload, bigquery_lookup_value, key_to_update)
        else:
            payload = self.validate_and_update_payload(payload, bigquery_lookup_value, key_to_update)

        return payload

    def validate_and_update_payload(self, payload, bigquery_lookup_value, key_to_update):
        """
        Validates if key exists in traits or properties and update accordingly in external id block
        If key doesn't exist then checks and populates from big query lookup value.
        """

        value_from_traits_or_properties = extract_value_from_traits_and_properties(payload, key_to_update)

        if value_from_traits_or_properties and isinstance(value_from_traits_or_properties, str) and not value_from_traits_or_properties.isspace():
            payload = self.update_external_id_field(payload, value_from_traits_or_properties, key_to_update)

        elif value_from_traits_or_properties and isinstance(value_from_traits_or_properties, str) and value_from_traits_or_properties.isspace():
            payload = self.add_value_from_bigquery_personas(payload, bigquery_lookup_value, key_to_update,
                                                            empty_string=True)

        elif key_to_update == 'countryCode':
            payload = self.add_value_from_bigquery_personas(payload, self.country_code, key_to_update)

        else:
            payload = self.add_value_from_bigquery_personas(payload, bigquery_lookup_value, key_to_update)

        return payload

    def update_external_id_field(self, payload, value, key_to_update):
        """
        Update the external id field with corresponding value passed.
        :param payload:
        :param value:
        :param key_to_update:
        :return: updated payload with updated field in external id block
        """

        _base_payload = {
            "collection": "users",
            "encoding": "none",
            "id": str(value),
            "type": key_to_update
        }

        try:
            payload['context']['externalIds'].append(_base_payload)
            return payload

        except KeyError:
            _ = {'externalIds': [_base_payload]}
            if 'context' not in payload.keys():
                payload['context'] = _
            else:
                payload['context'].update(_)
            return payload

    def add_value_from_bigquery_personas(self, payload, value, key_to_update, empty_string=None):
        """
        Updates the payload from bigquery lookup value.
        """

        if value:
            payload = self.update_external_id_field(payload, value, key_to_update)

        return payload

    def remove_empty_dicts(self, payload):
        """
        Removes the empty dictionary from the external id block.
        """

        external_id_payload = check_key_exists(payload, 'context', 'externalIds')
        if external_id_payload:
            # Use list comprehension to filter out empty dictionaries
            _external_id = [item for item in external_id_payload if item]
            payload['context']['externalIds'] = _external_id
            return payload
        else:
            return payload
