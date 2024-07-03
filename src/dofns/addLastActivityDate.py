import apache_beam as beam
import json
import requests
import copy
import sys
import logging
import re


class AddLastActivityDate(beam.DoFn):

    def validate_lastactivity_date(self, record):
        try:
            if 'traits' in record and record['type'] == 'identify' and record['traits'] != None:
                if 'lastActivityDate' not in record['traits']:
                    record['traits']['lastActivityDate'] = record['originalTimestamp']
                    score_keys = [key for key in record['traits'] if 'score' in key]
                    for delete_key in score_keys: del record['traits'][delete_key]
                    return record
                else:
                    return record
            elif 'traits' in record and record['traits'] is None:
                record['traits'] = {'lastActivityDate': record['originalTimestamp']}
                return record
            else:
                return record

        except Exception as e:
            logging.exception('inside except last activity date' + str(e))
    
    def process(self, element):

        data = json.loads(element)
        updated_record = self.validate_lastactivity_date(data)
        final_record = json.dumps(updated_record)
        yield final_record