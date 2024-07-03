import datetime
import json
import logging
from google.cloud import bigquery
import apache_beam as beam

from helpers import constants
from utils.getValueFromTraitsOrProperties import check_key_exists, get_values_from_external_id


class MpnOptsValidationWithCRS(beam.DoFn):
    """
    Extension of beam DoFn to validate mpn/opts combination.
    """

    def __init__(self):
        self.service_names = ['emailSubscriptionServiceName', 'phoneSubscriptionServiceName',
                              'postalSubscriptionServiceName', 'socialSubscriptionServiceName']

        self.opt_numbers = ['emailSubscriptionOptNumber', 'phoneSubscriptionOptNumber',
                            'postalSubscriptionOptNumber', 'socialSubscriptionOptNumber']

        self.contact_point_type = ['emailSubscriptionContactPointType', 'phoneSubscriptionContactPointType',
                                   'postalSubscriptionContactPointType', 'socialSubscriptionContactPointType']

        self.contact_category = ['emailSubscriptionContactCategory', 'phoneSubscriptionContactCategory',
                                 'postalSubscriptionContactCategory', 'socialSubscriptionContactCategory']

        self.validation_data_from_cds_table = None

    def setup(self):
        self.validation_data_from_cds_table = self.get_validation_data_from_cds_table()

    def get_validation_data_from_cds_table(self):
        """ Get the values from CDS table to validate mpn/opt validation """

        cds_sql_query = 'SELECT * FROM `dbce-c360-segamaws-prod-9669.CDS.marketing_program_opt_map`'
        try:
            logging.info(f"Getting values from CDS table for mpn/opt validation, query: {cds_sql_query}")
            client = bigquery.Client(project=constants.PROJECT)
            self.validation_data_from_cds_table = client.query(cds_sql_query).to_dataframe().to_dict(orient='index')

            logging.info(f"Values from CDS table for mpn/opt validation fetched successfully, "
                         f"record count: {len(self.validation_data_from_cds_table)}")
            return self.validation_data_from_cds_table

        except Exception as err:
            logging.exception(f"Error while getting records from CDS table error: {err}")

    def process(self, element, *args, **kwargs):

        data = json.loads(element)

        try:
            event_type = data.get('event', None)
            if event_type == 'Changed Opt Status':
                properties_block = check_key_exists(data, 'properties')

                if properties_block:
                    mpn = data['properties'].get('marketingProgramNumber', None)
                    opt_id = data['properties'].get('optId', None)
                    service_name = next((data['properties'][key] for key in self.service_names if key in
                                         data['properties']), None)
                    opt_number = next((data['properties'][key] for key in self.opt_numbers if key in
                                       data['properties']), None)
                    contact_point_type = next((data['properties'][key] for key in self.contact_point_type if key in
                                               data['properties']), None)
                    contact_category = next((data['properties'][key] for key in self.contact_category if key in
                                             data['properties']), None)

                    if all(opts is None for opts in
                           [opt_id, service_name, opt_number, contact_point_type, contact_category]):
                        yield json.dumps({'raw_payload': data})

                    elif mpn == '9999' or opt_number == '9999':
                        yield json.dumps({'raw_payload': data})

                    else:
                        filter_cds_data_on_mpn = self.filter_cds_data_on_mpn(mpn)
                        if filter_cds_data_on_mpn:
                            payload = self.validate_payload_from_cds_data(data, opt_id, service_name, opt_number,
                                                                          contact_point_type, contact_category,
                                                                          filter_cds_data_on_mpn)
                            yield json.dumps(payload)
                        else:
                            yield json.dumps({'raw_payload': data})
                else:
                    yield json.dumps({'raw_payload': data})
            # if not change opts status pass it as validation passed
            else:
                yield json.dumps({'raw_payload': data})

        except Exception as err:
            logging.exception(f"Unable to Validate Mpn/Opts combination err: {err}")

    def filter_cds_data_on_mpn(self, mpn):

        matched_mpn_data_with_cds_dataset = []
        for row_index, row_value in self.validation_data_from_cds_table.items():
            if str(row_value['cdp_marketing_program_number']) == mpn:
                matched_mpn_data_with_cds_dataset.append(row_value)

        return matched_mpn_data_with_cds_dataset

    def validate_payload_from_cds_data(self, data, opt_id, service_name, opt_number, contact_point_type,
                                       contact_category_code, matched_mpn_data_with_cds_dataset):

        validation_parameters_mapping = {'ciam_opt_id': opt_id, 'service_name': service_name,
                                         'subscription_opt_number': opt_number,
                                         'contact_point_category_code': contact_point_type,
                                         'contact_point_type_code': contact_category_code}

        # Minimum number of keys to match
        opts_without_none_values = {key: value for key, value in validation_parameters_mapping.items() if
                                    value is not None}
        count_valid_opts_to_validate = len(opts_without_none_values)
        matched_unmatched_mapping = []

        # Iterate through expected values
        for index, mpn_data in enumerate(matched_mpn_data_with_cds_dataset):
            matched_keys = [key for key, expected_value in mpn_data.items()
                            if key in validation_parameters_mapping and
                            validation_parameters_mapping[key] == str(expected_value)]

            not_matched_keys = list(set(opts_without_none_values.keys()) - set(matched_keys))
            matched_unmatched_mapping.append(
                {index: {'matched_values': matched_keys, "unmatched_values": not_matched_keys}})

            if len(matched_keys) >= count_valid_opts_to_validate >= 1:
                # On validation passed just return data to next step
                return {'raw_payload': data}

        # In case of validation failed
        matched_unmatched_values = self.get_opts_matched_unmatched_value(matched_unmatched_mapping)
        mpn = get_values_from_external_id(data, 'marketingProgramNumber')
        failure_status = {'message_id': data['messageId'],
                          'mpn': mpn,
                          'dq_validation_id': "VLD7",
                          'dq_validation_description': 'Validate if MPN Opts combination exists In CRS',
                          'error_details': json.dumps(matched_unmatched_values),
                          'reported_at': datetime.datetime.now().isoformat()
                          }

        final_payload = {'raw_payload': data, 'validation_status': [failure_status]}
        return final_payload

    def get_opts_matched_unmatched_value(self, opts_matched_unmatched_values):

        default_max_value = len(opts_matched_unmatched_values[0][0]['matched_values'])
        default_opts_matched_unmatched_value = opts_matched_unmatched_values[0][0]

        for opts_matched_unmatched_value in opts_matched_unmatched_values:
            for index, matched_unmatched_keys in opts_matched_unmatched_value.items():
                if len(matched_unmatched_keys['matched_values']) > default_max_value:
                    default_max_value = len(matched_unmatched_keys['matched_values'])
                    default_opts_matched_unmatched_value = matched_unmatched_keys

        return default_opts_matched_unmatched_value
