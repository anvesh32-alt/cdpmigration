import json
import logging
import sys

from google.cloud import secretmanager

from helpers import constants


def check_key_exists(element, *keys):
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


def extract_value_from_traits_and_properties(element, key):
    """
    Check if key exists in traits or properties block and extracts corresponding value of it.
    :param element: dict -> payload
    :param key: str -> key to be checked and validated in traits/properties block
    :return: value from traits/properties block if found else False
    """
    value_from_traits = check_key_exists(element, 'traits', key)
    if value_from_traits and isinstance(value_from_traits, str) and not value_from_traits.isspace():
        return value_from_traits
    elif value_from_traits and isinstance(value_from_traits, int):
        return value_from_traits
    else:
        value_from_properties = check_key_exists(element, 'properties', key)
        if value_from_properties and isinstance(value_from_properties, str) and not value_from_properties.isspace():
            return value_from_properties
        elif value_from_properties and isinstance(value_from_properties, int):
            return value_from_properties
    return False


def flatten_records(element):

    user_id, data = element
    gcs_data = data.get('map_gcs_data', None)
    bq_data = data.get("map_bq_data", None)

    output_val = [{'map_gcs_data': json.loads(data), 'map_bq_data': bq_data} for data in gcs_data]

    return output_val


def get_values_from_external_id(data, key):

    external_id_payload = check_key_exists(data, 'context', 'externalIds')
    if external_id_payload:
        external_id_values = {value['type']: value['id'] for value in external_id_payload}

        if key in external_id_values:
            return external_id_values[key]

    return ""


def get_authentication_key_from_secret_manager(key):
    """ Get the value from secret manager for that particular key. """

    try:
        logging.info(f"Getting Credentials From Secret Manager For Key: {key}")
        client = secretmanager.SecretManagerServiceClient()
        secret_url_name = constants.SECRET_URL.format(key)
        response = client.access_secret_version(name=secret_url_name)
        token = response.payload.data.decode("UTF-8")
        logging.info(f"Secret Key Successfully Fetched From Secret Manager")
        return token

    except Exception as err:
        logging.exception(f"Exception in Getting the Key From Secret Manager, err: {err}")
        sys.exit(1)
