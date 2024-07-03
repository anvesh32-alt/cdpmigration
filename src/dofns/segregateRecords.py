import apache_beam as beam
import sys
import json


class SegregateRecords(beam.DoFn):
    def __init__(self, event_types):
        self.event_types = event_types
        
    def process(self, element):
        
        try:
            event_type = str(element['event']).lower()
            matched = False
            for index_type, event_type_list in enumerate(self.event_types):
                if event_type in event_type_list:
                    matched = True
                    yield beam.pvalue.TaggedOutput(str(index_type), json.dumps(element))
            if not matched:
                yield beam.pvalue.TaggedOutput('unmatched', json.dumps(element))
        except Exception as e:
            event_type = element['type']
            matched = False
            for index_type, event_type_list in enumerate(self.event_types):
                if event_type in event_type_list:
                    matched = True
                    yield beam.pvalue.TaggedOutput(str(index_type), json.dumps(element))
            if not matched:
                yield beam.pvalue.TaggedOutput('unmatched', json.dumps(element))
