import logging
import json
import re

import apache_beam as beam


class SplitChannelProperties(beam.DoFn):
    """
    Extension of beam DoFn to split multiple channel properties to multiple records.
    """

    def split_channel_properties(self, channel_properties):
        """
        Uses a regex pattern match and splits the channel properties and creates a sub dictionary
        with key as each pattern with values as per regex match
        :param channel_properties: dict with key,values of properties object from the main payload
        :return: tuple of (sub_channel_properties, common_payload)
        """

        if not isinstance(channel_properties, dict):
            raise TypeError(f'Input passed in not a dictionary, input:{channel_properties}')

        # Create a list of patterns that needs to be matched
        patterns = [r'^email', r'^postal', r'^phone', r'^social', r'^regulatory']

        # Create a sub dictionary to store the value of each channel property with key as pattern
        sub_channel_properties = {pattern: {} for pattern in patterns}
        common_payload = {}

        for key, value in channel_properties.items():
            for pattern in patterns:
                if re.match(pattern, key):
                    sub_channel_properties[pattern][key] = value
                    break
            else:
                common_payload[key] = value

        validate_payload_for_opts_split = self.validate_payload_for_opts_split(sub_channel_properties)
        return validate_payload_for_opts_split, common_payload

    def process(self, element, *args, **kwargs):
        """
        If a single message has multiple channel properties for payload with 'event' == 'Changed Opt Status
        it would be transformed to multiple records each corresponding to the channel.
        :param element:pcollection
        :return: pcollection element to send to next step in pipeline
        """

        data = json.loads(element)

        try:
            if data['event'] == 'Changed Opt Status' and 'properties' in data:

                # call the split_channel_properties method with properties payload
                sub_channel_properties, common_payload = self.split_channel_properties(data['properties'])
                all_empty_sub_channel_properties = all(len(value) == 0 for value in sub_channel_properties.values())
                get_message_id_for_payloads = self.get_message_id_for_payloads(sub_channel_properties,
                                                                               data['messageId'])

                # check if any sub dictionary of channel property has value or not
                if not all_empty_sub_channel_properties:
                    for channel_property_key, channel_property_value in sub_channel_properties.items():
                        if channel_property_value:
                            # make a copy of input data and update values for "property" key with new values
                            input_data_updated = data.copy()
                            channel_property_value.update(common_payload)
                            input_data_updated['properties'] = channel_property_value
                            input_data_updated['messageId'] = get_message_id_for_payloads[channel_property_key]
                            yield json.dumps(input_data_updated, separators=(',', ':'), ensure_ascii=False)
                else:
                    yield json.dumps(data, separators=(',', ':'), ensure_ascii=False)
            else:
                yield element
        except KeyError:
            yield element
        except Exception as err:
            logging.exception(f"Error While Processing Records in Split Channel Properties, Error: {str(err)}")

    def get_message_id_for_payloads(self, sub_channel_properties, message_id):
        """ Generate the message id based on the number of opts splits or else return the original message id """

        custom_message_id = {}
        counter = 1
        get_length_of_split_channel = len({key: value for key, value in sub_channel_properties.items()
                                           if value})
        for key, value in sub_channel_properties.items():
            if value and get_length_of_split_channel > 1:
                _custom_message_id = f'{message_id}-split{counter}'
                counter += 1
                custom_message_id[key] = _custom_message_id
            else:
                custom_message_id[key] = message_id

        return custom_message_id

    def validate_payload_for_opts_split(self, sub_channel_properties):
        """ Validate if the opt split made is valid or not """

        extract_email_from_email_opts = sub_channel_properties['^email'].get('email')
        if extract_email_from_email_opts:
            keys_to_update = ['^postal', '^social', '^regulatory']
            for key_to_update in keys_to_update:
                sub_channel_properties[key_to_update].update({'email': extract_email_from_email_opts})

        try:
            for opt_key, opt_values in sub_channel_properties.items():
                format_opt_key = ''.join(key for key in opt_key if key.isalpha())
                choice_date = opt_values.get(f'{format_opt_key}ConsumerChoiceDate')
                opt_indicator = opt_values.get(f'{format_opt_key}SubscriptionOptIndicator')
                opt_number = opt_values.get(f'{format_opt_key}SubscriptionOptNumber')
                service_name = opt_values.get(f'{format_opt_key}SubscriptionServiceName')
                contact_type = opt_values.get(f'{format_opt_key}ContactType')
                contact_category = opt_values.get(f'{format_opt_key}ContactCategory')

                # todo: duplicate lines
                if format_opt_key == 'phone':
                    # validation steps for phone opts
                    extract_phone_number = opt_values.get('phoneNumber')
                    if all([extract_phone_number, choice_date, contact_type, contact_category]):
                        continue
                    elif extract_phone_number and choice_date and not opt_number and (opt_indicator or service_name):
                        continue
                    else:
                        sub_channel_properties[opt_key] = {}

                elif format_opt_key == 'postal':
                    # validation steps for postal opts
                    if opt_number and choice_date and contact_type and contact_category:
                        continue
                    elif choice_date and not opt_number and (opt_indicator or service_name):
                        continue
                    else:
                        sub_channel_properties[opt_key] = {}

                else:
                    # validation steps for email, regulatory and social opts
                    if all([extract_email_from_email_opts, choice_date, contact_type, contact_category]):
                        continue
                    elif extract_email_from_email_opts and choice_date and not opt_number and \
                            (opt_indicator or service_name):
                        continue
                    else:
                        sub_channel_properties[opt_key] = {}

            return sub_channel_properties

        except Exception as err:
            logging.exception(f"Unable to Split the Opts, Error: {err} ")

class ContactProperties(beam.DoFn):
    def process(self, element, *args, **kwargs):

        try:
            element=json.loads(element)
            patterns = [r'^emailSubscriptionOptIndicator', r'^postalSubscriptionOptIndicator', r'^phoneSubscriptionOptIndicator', r'^socialSubscriptionOptIndicator', r'^regulatorySubscriptionOptIndicator']
            indicator = {
                "^emailSubscriptionOptIndicator": {
                    "contactType": "E",
                    "contactCategory": "EP"
                },
                "^phoneSubscriptionOptIndicator": {
                    "contactType": "P",
                    "contactCategory": "PM"
                },
                "^postalSubscriptionOptIndicator": {
                    "contactType": "A",
                    "contactCategory": "AR"
                },
                "^socialSubscriptionOptIndicator": {
                    "contactType": "S",
                    "contactCategory": "OT"
                },
                "^regulatorySubscriptionOptIndicator": {
                    "contactType": "R",
                    "contactCategory": "PS"
                },
            }
            count=0
            channel_properties= element["properties"]
            properties=dict(channel_properties)

            for key, value in channel_properties.items():
                if 'SubscriptionOptIndicator' in key:
                    count=count+1
            if count == 1:
                for key, value in channel_properties.items():
                    for pattern in patterns:
                        if re.match(pattern, key):
                            properties.update(indicator[pattern])
                            break

            element["properties"]=properties
            yield json.dumps(element)
        except:
            yield json.dumps(element)