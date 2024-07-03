
import apache_beam as beam
import json
import requests
import copy
import sys
import logging


class FeaturesCheckOther(beam.DoFn):

    def add_trait_user_id(self,record):
        try:
            if 'traits' in record  and 'userId' in record and record['traits'] is not None and len(record['traits'])!= 0:
                record['traits']['userId'] = record['userId']
                return record
            elif 'traits' in record and 'userId' in record and (record['traits'] is None or len(str(record['traits'])) == 0):
                record['traits'] = {'userId': record['userId']}
                return record
            elif 'properties' in record and 'userId' in record :
                record['properties']['userId'] = record['userId']
                return record
            else:
                return record
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.exception("Exception at userid trait record--" + str(type(e)) + str(e) + str(exc_tb.tb_lineno))

    def add_trait_anonymous_id(self,record):
        try:
            if 'traits' in record and 'anonymousId' in record and record['traits'] is not None and len(record['traits'])!= 0:
                record['traits']['anonymousId'] = record['anonymousId']
                return record
            elif 'traits' in record and 'anonymousId' in record and (record['traits'] is None or len(str(record['traits'])) == 0):
                record['traits']={'anonymousId': record['anonymousId']}
                return record
            elif 'properties' in record and 'anonymousId' in record :
                record['properties']['anonymousId'] = record['anonymousId']
                return record
            else:
                return record
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.exception("Exception at anonymousid record--" + str(type(e)) + str(e) + str(exc_tb.tb_lineno))

    def process(self, element):

        data = json.loads(element)
        updated_record = self.add_trait_anonymous_id(data)
        updated_user_id_record = self.add_trait_user_id(updated_record)
        final_record = json.dumps(updated_user_id_record)
        yield final_record
