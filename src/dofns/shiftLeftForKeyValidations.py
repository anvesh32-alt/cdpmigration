import json
import logging
import datetime
import apache_beam as beam

from utils.getValueFromTraitsOrProperties import check_key_exists, get_values_from_external_id


class AddLegalEntityToPayloads(beam.DoFn):

    def __init__(self, legal_entity):
        self.legal_entity = legal_entity
        self.service_names = ['emailSubscriptionServiceName', 'phoneSubscriptionServiceName',
                              'postalSubscriptionServiceName', 'socialSubscriptionServiceName',
                              'regulatorySubscriptionServiceName']

    def process(self, element, *args, **kwargs):

        data = json.loads(element)
        try:
            if data['type'] == 'track' and data['event'] == 'Changed Opt Status':
                payload_with_legal_entity = self.add_legal_entity_for_9999_mpn(data)
                yield json.dumps(payload_with_legal_entity)
            else:
                yield element
        except KeyError:
            yield element
        except Exception as err:
            logging.exception(f"Unable to Add Legal Entity For Change Opts Status call and MPN 9999, err: {err}")

    def add_legal_entity_for_9999_mpn(self, data):
        external_id_payload = check_key_exists(data, 'context', 'externalIds')
        if external_id_payload:
            external_id_field = next((item for item in external_id_payload
                                      if item["type"] == 'marketingProgramNumber'), {})

            marketing_program_number = external_id_field.get('id')
            # Add LE in case of MPN in external id block is 9999
            if marketing_program_number and marketing_program_number == '9999':
                properties_block = check_key_exists(data, 'properties')
                if properties_block:
                    properties_block.update({"legalEntityNumber": self.legal_entity})

            else:
                properties_block = check_key_exists(data, 'properties')
                # Add LE in case of ServiceName == Universal
                if properties_block:
                    for service_name in self.service_names:
                        service_name_value = check_key_exists(properties_block, service_name)
                        if service_name_value == 'Universal':
                            properties_block.update({"legalEntityNumber": self.legal_entity})
                            break

        return data


class AddFieldsToTraitsBlock(beam.DoFn):

    def __init__(self):
        self.fields_to_add = ['marketingProgramNumber', 'countryCode', 'sourceId']

    def process(self, element, *args, **kwargs):
        try:
            data = json.loads(element)
            validate_and_update_payload = self.validate_if_key_exists(data)
            yield json.dumps(validate_and_update_payload)
        except Exception as err:
            if isinstance(element, dict):
                yield json.dumps(element)
            else:
                yield element
            logging.exception(f"Unable To Add Values In Traits Block, err: {err}")

    def validate_if_key_exists(self, data):
        traits_block = check_key_exists(data, 'traits')
        if traits_block:
            for key in self.fields_to_add:
                if not check_key_exists(traits_block, key):
                    external_id_payload = check_key_exists(data, 'context', 'externalIds')
                    if external_id_payload:
                        external_id_field = next((item for item in external_id_payload
                                                  if item["type"] == key), {})
                        if external_id_field:
                            key_value = external_id_field.get('id')
                            traits_block.update({key: key_value})
        return data


class ValidateTimestampInPayloads(beam.DoFn):

    def process(self, element, *args, **kwargs):

        data = json.loads(element)
        get_original_timestamp = data['raw_payload'].get("originalTimestamp", None)
        get_timestamp = data['raw_payload'].get("timestamp", None)
        if get_original_timestamp and get_timestamp:
            yield json.dumps(data)
        else:
            payload = self.validate_timestamps_in_payload(data, get_original_timestamp,
                                                          get_timestamp)
            yield json.dumps(payload)

    def validate_timestamps_in_payload(self, data, original_timestamp, timestamp):

        mpn = get_values_from_external_id(data['raw_payload'], 'marketingProgramNumber')

        if not original_timestamp:

            failure_status = {'message_id': data['raw_payload']['messageId'],
                              'mpn': mpn,
                              'dq_validation_id': "VLD10",
                              'dq_validation_description': 'Validate if originalTimestamp or timestamp exists in Payload',
                              'error_details': "originalTimestamp Not Found",
                              'reported_at': datetime.datetime.now().isoformat()
                              }

            data.setdefault('validation_status', []).append(failure_status)

        elif not timestamp:
            failure_status = {'message_id': data['raw_payload']['messageId'],
                              'mpn': mpn,
                              'dq_validation_id': "VLD10",
                              'dq_validation_description': 'Validate if originalTimestamp or timestamp exists in Payload',
                              'error_details': "timestamp Not Found",
                              'reported_at': datetime.datetime.now().isoformat()
                              }

            data.setdefault('validation_status', []).append(failure_status)

        return data
