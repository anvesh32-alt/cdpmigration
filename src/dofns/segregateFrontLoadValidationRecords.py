import json
import logging

import apache_beam as beam


class SegregateFrontLoadValidationRecords(beam.DoFn):

    def process(self, element, *args, **kwargs):

        data = json.loads(element)

        try:
            check_validation_status = data.get('validation_status', None)
            if check_validation_status is not None:
                validate_consumer_id_failure = self.segregate_consumer_id_failure(check_validation_status)
                if validate_consumer_id_failure:
                    yield beam.pvalue.TaggedOutput('front_load_validation_passed', json.dumps(data['raw_payload']))
                else:
                    for payload in data['validation_status']:
                        payload.update({'raw_payload': json.dumps(data['raw_payload'])})
                        yield beam.pvalue.TaggedOutput('front_load_validation_failed', payload)
            else:
                yield beam.pvalue.TaggedOutput('front_load_validation_passed', json.dumps(data['raw_payload']))
        except Exception as err:
            logging.exception(f"Unable to segregate front load validation records err: {err}")

    def segregate_consumer_id_failure(self, validation_status_payload):

        for value in validation_status_payload:
            if value['dq_validation_id'] == 'VLD8' and len(validation_status_payload) == 1:
                return True

        return False
