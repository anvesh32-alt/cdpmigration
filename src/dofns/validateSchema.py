import apache_beam as beam
import json
import re
import logging
import datetime

from utils.getValueFromTraitsOrProperties import extract_value_from_traits_and_properties


class SchemaValidation(beam.DoFn):

    def check_properties_id(self, data, message_id, validation_list, mp, current_timestamp, properties):
        list_check=['OptNumber','optId','optChoiceDate','collectedDate','optStatus','initialOptStatus']

        for type in list_check:
            re_pattern = ''
            for key,value in properties.items():
                if type in key:
                    if type== 'OptNumber':
                        re_pattern=re.compile('^\\d+$')

                    elif type == 'optId' and 'type' in data:
                        if data['type']=='track':
                            re_pattern=re.compile('^(\\d+)_(\\d+)$')
                        else:
                            break
                    elif type == 'optChoiceDate':
                        re_pattern=re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$')
                    elif type == 'optStatus':
                        re_pattern=re.compile('^(?i)(true|false)$')

                    elif type == 'initialOptStatus' and 'type' in data:
                        if data['type'] == 'track':
                            re_pattern = re.compile('^(?i)(true|false)$')
                        else:
                            break

                    if isinstance(value,str) and re.fullmatch(re_pattern, value):
                        break
                    else:
                        validation_list.append({"message_id": message_id, "raw_payload": json.dumps(data), "mpn": mp,
                                                "dq_validation_id": "OCPC",
                                                "dq_validation_description": "Validate If " + key + " is in valid format in Payload",
                                                "error_details": "INVALID_FORMAT: " + key + " with value " + str(value) + " is not expected value for this field",
                                                "reported_at": current_timestamp})


    def check_external_id(self, external_id, message_id, payload, validation_list, mp, current_timestamp):
        list_check = ['marketingProgramNumber', 'countryCode', 'sourceId','consumerId','loyaltyAccountNumber']

        for types in list_check:
            count_failed = 0
            count_null = 0
            count_mismatch = 0
            count_invalid_data_type=0
            for field in external_id:
                if types == field['type']:
                    value = field['id']
                    if isinstance(value,str):
                        if len(value) != 0:
                            if types in ('marketingProgramNumber', 'sourceId'):
                                re_pattern = re.compile('^\\d+$')
                            elif types in ('consumerId'):
                                re_pattern=re.compile('^[a-zA-Z0-9.:-]+$')
                            elif types in ('loyaltyAccountNumber'):
                                re_pattern=re.compile('^[A-Za-z0-9-]*$')
                            else:
                                re_pattern = re.compile(r'^[a-zA-Z\s\d]+$')
                            if re.fullmatch(re_pattern, value):
                                break
                            else:
                                count_mismatch = count_mismatch + 1

                        else:
                            count_null = count_null + 1
                    else:
                        count_invalid_data_type=count_invalid_data_type+1

                else:
                    count_failed = count_failed + 1

            if count_mismatch >=1:
                validation_list.append({"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp,
                                        "dq_validation_id": "MCPC",
                                        "dq_validation_description": "Validate If " + types + " is in valid format in Payload",
                                        "error_details": "INVALID_FORMAT: " + types + " with value " + value + " is not expected value for this field",
                                        "reported_at": current_timestamp})

            if count_null >=1:
                validation_list.append({"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp,
                                        "dq_validation_id": "MCPC",
                                        "dq_validation_description": "Validate If " + types + " is null in Payload",
                                        "error_details": "FIELD_NULL: " + types + " is null", "reported_at": current_timestamp})

            if count_invalid_data_type >=1:
                validation_list.append({"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp,
                                        "dq_validation_id": "MCPC",
                                        "dq_validation_description": "Validate If " + types + " is having valid datatype in Payload",
                                        "error_details": "INVALID DATATYPE: " + types + " is having invalid datatype",
                                        "reported_at": current_timestamp})

            if count_failed == len(external_id) and (types!='loyaltyAccountNumber' and types!='countryCode' and types!="consumerId") :
                validation_list.append({"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp,
                                        "dq_validation_id": "MCPC",
                                        "dq_validation_description": "Validate if " + types + " exists in Payload",
                                        "error_details": "FIELD_NOT_FOUND: " + types + " Missing in External Id Block", "reported_at": current_timestamp})

            elif (count_failed == len(external_id) and types == "consumerId" and
                  not extract_value_from_traits_and_properties(payload, "offlineConsumerId")):
                validation_list.append({"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp,
                                        "dq_validation_id": "MCPC",
                                        "dq_validation_description": "Validate if " + types + " exists in Payload",
                                        "error_details": "FIELD_NOT_FOUND: " + types + " Missing in External Id Block",
                                        "reported_at": current_timestamp})

    def check_string(self, payload, message_id, validation_list, mp, current_timestamp):

        list_check=['event','timestamp']
        for type in list_check:
            if type in payload:
                event = payload[type]
                if isinstance(event,str):
                    if len(event) != 0:
                        if type=='timestamp':
                            re_pattern=re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$')
                        else:
                            re_pattern = re.compile(r'^[a-zA-Z\s\d]+$')
                        if re.fullmatch(re_pattern, event):
                            pass
                        else:
                            validation_list.append(
                                {"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp, "dq_validation_id": "MCPC",
                                 "dq_validation_description": "Validate If " + type + " is in valid format in Payload",
                                 "error_details": "INVALID_FORMAT: " + type + " name is not present in proper string format", "reported_at": current_timestamp})
                    else:
                        validation_list.append(
                            {"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp, "dq_validation_id": "MCPC",
                             "dq_validation_description": "Validate If " + type + " is null in Payload",
                             "error_details": "FIELD_NULL " + type +" name is null or not with string datatype", "reported_at": current_timestamp})
                else:
                    validation_list.append(
                        {"message_id": message_id, "raw_payload": json.dumps(payload), "mpn": mp, "dq_validation_id": "MCPC",
                         "dq_validation_description": "Validate If " + type + " is having valid datatype in Payload",
                         "error_details": "INVALID_DATATYPE: " + type + " is having invalid datatype",
                         "reported_at": current_timestamp})

    def process(self, element):
        try:
            validation_list = []
            data = json.loads(element)
            message_id = data['messageId']
            external_id = data['context']['externalIds']
            marketing_program_number=''
            for mp in external_id:
                if (mp['type'] == 'marketingProgramNumber') and isinstance(mp['id'],str):
                    marketing_program_number = mp['id']
                    break
                else:
                    marketing_program_number = None

            current_timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")

            if marketing_program_number !='9999':
                # Validate Event
                self.check_string(data, message_id, validation_list,marketing_program_number,current_timestamp)

                # for external_ids fields in payload
                self.check_external_id(external_id, message_id, data, validation_list, marketing_program_number, current_timestamp)

                # Validate properties
                properties = data.get('properties', None)
                if properties:
                    self.check_properties_id(data, message_id, validation_list, marketing_program_number, current_timestamp, properties)
            if len(validation_list)==0:
                yield beam.pvalue.TaggedOutput('proper_schema', json.dumps(data))
            else:
                for raw_data in validation_list:
                    yield beam.pvalue.TaggedOutput('invalid_schema', json.dumps(raw_data))
        except Exception as e:
            logging.exception('exception in payload for the field :',str(e))
            yield beam.pvalue.TaggedOutput('proper_schema', element)
