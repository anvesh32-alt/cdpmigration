
import apache_beam as beam
import json
import logging
from utils.getValueFromTraitsOrProperties import check_key_exists


class ValidateMpnString(beam.DoFn):
    
    def parse_mpn_string(self, data, key):
        try:
            data[key]['marketingProgramNumber'] = str(data[key]['marketingProgramNumber'])
            return data
        except Exception as e:
            logging.info("exception at mpn as string")
        
    
    def process(self, element):

        data = json.loads(element)
        try:
                mpn_from_traits = check_key_exists(data,'traits','marketingProgramNumber')
                if mpn_from_traits :
                    updated_payload = self.parse_mpn_string(data,'traits')
                    yield json.dumps(updated_payload)
                else:
                    mpn_from_properties = check_key_exists(data, 'properties', 'marketingProgramNumber')
                    if mpn_from_properties:
                        updated_payload = self.parse_mpn_string(data,'properties')
                        yield json.dumps(updated_payload)
                    else:
                        yield json.dumps(data)
        except KeyError:
            yield element
 