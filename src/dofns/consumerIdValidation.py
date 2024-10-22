import json
import logging
import datetime
import re
import apache_beam as beam

from utils.getValueFromTraitsOrProperties import check_key_exists, get_values_from_external_id


class ConsumerIdValidation(beam.DoFn):
    """
    Extension of beam DoFn to validate of payload contains consumer id/offline consumer id for last 30 days.
    """

    def __init__(self):
        """
        Constructs all the necessary attributes for the ConsumerIdValidation object.
        """
        self.current_date = datetime.date.today()

    def process(self, element, *args, **kwargs):
        """
        Entry point for ConsumerIdValidation class which validates if payload contains consumer id and offline
        consumer or not. If not then it returns the source id from the payload
        Failure records from this pipeline is not dropped and considered for further processing
        :param element: records in json format
        :param args:
        :param kwargs:
        :return: pcollection in json format to be passed for next step in pipeline.
        """

        data = json.loads(element)
        timestamp = data['raw_payload'].get('timestamp', None)

        try:
            if timestamp is not None:
                timestamp_date = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").date()
                one_month_backdate = self.current_date - datetime.timedelta(days=30)

                if timestamp_date >= one_month_backdate:
                    payload = self.validate_consumer_id(data)
                    yield json.dumps(payload)
                else:
                    yield element
            else:
                yield element
        except (KeyError, ValueError):
            yield element
        except Exception as err:
            logging.exception(f"Unable to Validate ConsumerId/Offline ConsumerId field err: {err}")

    def validate_consumer_id(self, data):
        """
        Method to validate if consumer id/offline consumer is present in payload or not, it checks
        if consumer id is generated by migration pipeline.
        """

        external_id_payload = check_key_exists(data['raw_payload'], 'context', 'externalIds')
        mpn = get_values_from_external_id(data['raw_payload'], 'marketingProgramNumber')
        if external_id_payload:
            consumer_id_from_external_id = next((item for item in external_id_payload
                                                 if item["type"] == "consumerId"), False)

            if consumer_id_from_external_id:
                date_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'
                if re.search(date_pattern, consumer_id_from_external_id['id']):
                    source_id_from_payload = self.extract_source_id_from_payload(external_id_payload)

                    failure_status = {'message_id': data['raw_payload']['messageId'],
                                      'mpn': mpn,
                                      'dq_validation_id': "VLD8",
                                      'dq_validation_description': 'Validate if Consumer ID exists in Payload or '
                                                                   'not(Payloads wont be blocked)',
                                      'error_details': str(source_id_from_payload),
                                      'reported_at': datetime.datetime.now().isoformat()
                                      }

                    data.setdefault('validation_status', []).append(failure_status)

            elif not consumer_id_from_external_id:
                source_id_from_payload = self.extract_source_id_from_payload(external_id_payload)

                failure_status = {'message_id': data['raw_payload']['messageId'],
                                  'mpn': mpn,
                                  'dq_validation_id': "VLD8",
                                  'dq_validation_description': 'Validate if Consumer ID exists in Payload or not'
                                                               '(Payloads wont be blocked)',
                                  'error_details': str(source_id_from_payload),
                                  'reported_at': datetime.datetime.now().isoformat()
                                  }

                data.setdefault('validation_status', []).append(failure_status)

        return data

    def extract_source_id_from_payload(self, external_id_payload):
        """
        Extracts the source id for failure records if found else returns "Source Id Not Found In Payload".
        """

        source_id_block = next((item for item in external_id_payload if item["type"] == "sourceId"), False)

        if source_id_block:
            return source_id_block['id']

        return "Source Id Not Found In Payload"
