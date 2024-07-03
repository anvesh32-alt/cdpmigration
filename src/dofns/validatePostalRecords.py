
import apache_beam as beam
import json
import requests
import copy
import sys
import logging
import re


class PostalCheck(beam.DoFn):

    def validate_postal_exclude_data(self,record):
        data = copy.deepcopy(record)
        if record['type'] == 'track' and record['event'] == 'Changed Opt Status':
            try:
                if record['properties'].get("email") in ["null", None] and record['properties'].get('phoneNumber') in ["null", None]:
                    for fields in record['properties'].keys():
                        if 'postal' in fields:
                            data['postal'] = 'exclude'
                            return data
                        else:
                            return record
                elif record['properties'].get('phoneNumber') not in ["null", None] or record['properties'].get("email") not in ["null", None]:
                    return record
                else:
                    return record
            except KeyError:
                logging.info('key error in postal exclude')       
        else:
            return record

    def process(self, element):

        try:
            data = json.loads(element)
            updated_record = self.validate_postal_exclude_data(data)
            if 'postal' in updated_record:
                yield beam.pvalue.TaggedOutput('postal_exclude', json.dumps(updated_record))
            else:
                yield beam.pvalue.TaggedOutput('proper', json.dumps(updated_record))
        except Exception as err:
            yield element
                
    