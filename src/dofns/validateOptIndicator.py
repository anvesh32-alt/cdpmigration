
import apache_beam as beam
import json
import sys
import logging


class ValidateOptIndicator(beam.DoFn):

        
    def add_opt_indicator(self,record):
        try:
            if 'traits' in record and record['traits'] is not None and len(record['traits'])!= 0:
                indicator_field_list = [opt_field for opt_field in record['traits'].keys() if 'OptIndicator' in opt_field or 'OptNumber' in opt_field]
                try:
                     for indicator_field in indicator_field_list:
                        record['traits'][indicator_field]=str(record['traits'][indicator_field]).lower()
                except Exception as e:
                    logging.info("Exception at opt indicator parsing record--" + str(type(e)))
                return record
            elif 'properties' in record :
                indicator_field_list = [opt_field for opt_field in record['properties'].keys() if 'OptIndicator' in opt_field or 'OptNumber' in opt_field]
                try:
                    for indicator_field in indicator_field_list:
                        record['properties'][indicator_field]=str(record['properties'][indicator_field]).lower()
                except Exception as e:
                    logging.info("Exception at properties opt indicator parsing record--" + str(type(e)))
                return record
            else:
                return record
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.info("Exception at parsing record--" + str(type(e)) + str(e) + str(exc_tb.tb_lineno))

    def process(self, element):

        data = json.loads(element)
        updated_record = self.add_opt_indicator(data)
        final_record = json.dumps(updated_record)
        yield final_record
