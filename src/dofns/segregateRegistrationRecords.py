import apache_beam as beam
import logging
import json


class SegregateRecordsByChangedOpt(beam.DoFn):

    def process(self, element):
        try:
            event_type = str(element['event']).lower()
            if event_type == "changed opt status":
                yield beam.pvalue.TaggedOutput('changed_opt', json.dumps(element))
            else:
                yield beam.pvalue.TaggedOutput('identify', json.dumps(element))
        except Exception as e:
            yield beam.pvalue.TaggedOutput('identify', json.dumps(element))
