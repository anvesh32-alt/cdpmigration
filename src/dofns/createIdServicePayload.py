import json
import logging
import re

import apache_beam as beam


class CreateIdServicePayload(beam.DoFn):
    """
    Extension of beam DoFn to build a payload for ID service pubsub topic
    """

    def __init__(self, legal_entity):
        """
        Constructs all the necessary attributes for the CreateIdServicePayload object.
        :param legal_entity: int -> legal entity to be populated in the payload
        """

        self.legal_entity = legal_entity
        self.trait_names = ['firstName', 'lastName', 'fullName', 'birthDate']

    def get_trait_values(self, payload):
        """ Extract the corresponding value for traitName """

        trait_values = {}

        if 'traits' in payload:
            for trait in payload['traits']:
                trait_name = trait['traitName']
                if trait_name in self.trait_names:
                    trait_values[trait_name] = trait['values']

        return trait_values

    def replace_last_zeros_from_timestamp(self, timestamp):

        pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"
        if re.match(pattern, timestamp):
            return timestamp[:-1] + '.000' + "Z"

        return timestamp[:23] + 'Z'

    def process(self, element, *args, **kwargs):
        """
        Creates a payload in the desired format for id service pubsub topic making ['dependents', 'opts', 'traits']
        as blank
        :param element: list
                expected to be in format [trace_id, pipeline_origin, received_date, account_id, raw_payload]
        :return: id_service_payload
        """

        raw_payload_to_dict = json.loads(element['raw_payload'])
        try:
            # get the traits values from the trait object
            get_trait_values = self.get_trait_values(raw_payload_to_dict['data'])

            # make 'dependents', 'opts', 'traits' as blank
            keys_to_make_blank = ['dependents', 'opts', 'traits']
            raw_payload_to_dict['data'].update({key: [] for key in keys_to_make_blank})

            received_time_stamp = element['received_date'].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"

            if 'requestTimestamp' in raw_payload_to_dict.keys():
                raw_payload_to_dict['requestTimestamp'] = self.replace_last_zeros_from_timestamp(
                    raw_payload_to_dict['requestTimestamp'])

            # expected to be in format[trace_id, pipeline_origin, received_date, account_id, raw_payload]
            id_service_payload = {
                "traceId": element['trace_id'],
                "pipeline": element['pipeline_origin'],
                "receivedTimestamp": received_time_stamp,
                "accountId": element['account_id'],
                "legalEntity": str(self.legal_entity),
                "rawPayload": raw_payload_to_dict
            }

            id_service_payload.update(get_trait_values)
            yield json.dumps(id_service_payload)

        except KeyError as err:
            logging.info(f"Error in Processing Records due to key not found err :{err}")

        except Exception as err:
            logging.exception(f"Unable to create payload err: {str(err)}")
