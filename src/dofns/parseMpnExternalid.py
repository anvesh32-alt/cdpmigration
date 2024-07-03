
import apache_beam as beam
import json
import sys
import logging


class ValidateMpnStringExternalId(beam.DoFn):

    
    def check_key_exists_external_id(self, element, key):
        """
        Check if *keys (nested) exists in `element` (dict).
        """
        try :
            input_data = element
            if 'externalIds' in element['context']:
                group = {record['type']: record['id'] for record in input_data['context']['externalIds'] if 'type' in record}
                if key in group.keys():
                    return element
                else :
                    return False
        except Exception as e:
            return False
            logging.info('exception raised while checking externalid')
    
    def parse_mpn_external_id(self, data, key):
        try:
            for i in range(len(data['context']['externalIds'])):
                if data['context']['externalIds'][i]['type'] == key :
                    data['context']['externalIds'][i]['id'] = str(data['context']['externalIds'][i]['id'])
            return data
        except Exception as e:
            logging.info("exception at mpn as string")
        
    
    def process(self, element):

        data = json.loads(element)
        try:
                mpn_from_externalid = self.check_key_exists_external_id(data,'marketingProgramNumber')
                if mpn_from_externalid :
                    updated_payload = self.parse_mpn_external_id(data,'marketingProgramNumber')
                    yield json.dumps(updated_payload)
                else:
                    yield json.dumps(data)
        except KeyError:
            yield element
 
