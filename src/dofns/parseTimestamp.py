
import apache_beam as beam
import json
from datetime import datetime
import sys
import logging
import re
from dateutil import parser


class validateTimestamp(beam.DoFn):

    def __init__(self, date_fields):
        self.date_fields_list = date_fields

    def validate_opt_choice_fields(self,record):
        timestamp_fields_list= ['originalTimestamp', 'timestamp']
        pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z" 
        try :
            if 'traits' in record and record['traits'] is not None and len(record['traits'])!= 0:
                field_present = [key for key in self.date_fields_list if key in record['traits']]
                try:
                    for field_index in range(len(field_present)):
                        if re.match(pattern,record['traits'][field_present[field_index]]):
                            continue
                        else:
                            parsed_date = parser.parse(record['traits'][field_present[field_index]])
                            record['traits'][field_present[field_index]] = parsed_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                    for timestamp_field  in timestamp_fields_list:
                        if re.match(pattern,record[timestamp_field]):
                            continue
                        else:
                            parsed_timestamp_date = parser.parse(record[timestamp_field])
                            record[timestamp_field] = parsed_timestamp_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                except Exception as e:
                    logging.info("Exception at traits date parsing:--" +str(e))
                return record
            elif 'properties' in record and 'timestamp' in record :
                field_present = [key for key in self.date_fields_list if key in record['properties']]
                try:
                    for field_index in range(len(field_present)):
                        if re.match(pattern,record['properties'][field_present[field_index]]):
                            continue
                        else:
                            parsed_date = parser.parse(record['properties'][field_present[field_index]])
                            record['properties'][field_present[field_index]] = parsed_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                    for timestamp_field  in timestamp_fields_list:
                        if re.match(pattern,record[timestamp_field]):
                            continue
                        else:
                            parsed_timestamp_date = parser.parse(record[timestamp_field])
                            record[timestamp_field] = parsed_timestamp_date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"                    
                except Exception as e:
                    logging.info("Exception at properties date parsing:--" +str(e))
                return record
            else:
                return record
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.info("Exception at timestamp parsing record--" + str(type(e)) + str(e) + str(exc_tb.tb_lineno))
        

    def process(self, element):

        data = json.loads(element)
        updated_record = self.validate_opt_choice_fields(data)
        final_record = json.dumps(updated_record)
        yield final_record
