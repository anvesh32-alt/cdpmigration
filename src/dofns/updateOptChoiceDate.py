import json

import apache_beam as beam


class MappOptChoiceDateWithTimestamp(beam.DoFn):

    def process(self, element, *args, **kwargs):

        try:
            element = json.loads(element)
            properties = element["properties"]
            timestamp = element['timestamp']

            for key, value in properties.items():
                if 'ChoiceDate' in key:
                    properties[key] = timestamp
            yield json.dumps(element)

        except Exception:
            yield element
