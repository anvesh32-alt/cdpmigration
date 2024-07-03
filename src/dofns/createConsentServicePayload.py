import json
import logging
import datetime

import apache_beam as beam
from google.cloud import bigquery
from helpers import constants, crsQuery


class CreateConsentServicePayload(beam.DoFn):
    """
    Extension of beam DoFn to build a payload for Consent service pubsub topic
    """

    def __init__(self):
        self.values_from_crs = None
        self.current_timestamp = datetime.datetime.utcnow().isoformat()[:-3] + "Z"

    def setup(self):
        self.values_from_crs = self.get_values_from_crs()

    def get_values_from_crs(self):
        """ Get the values from CRS table to populate in opts payload """

        try:
            logging.info("Getting values from CRS table")
            client = bigquery.Client(project=constants.PROJECT)
            self.values_from_crs = client.query(crsQuery.crs_sql_query).to_dataframe().to_dict(orient='index')

            return self.values_from_crs

        except Exception as err:
            logging.exception(f"Error while getting records from CRS table error: {err}")

    def filter_values_from_crs_data(self, mpn, service_name, contact_point_type_name):

        matched_crs_records = []

        for row_index, row_data in self.values_from_crs.items():
            if row_data['service_name'] == service_name and row_data['contact_point_type_name'] == \
                    contact_point_type_name and row_data['marketing_program_number'] == mpn:
                matched_crs_records.append(row_data)

        if matched_crs_records:
            for key in matched_crs_records:
                if key['trait_id_opt_indicator'] is not None and "Cdp" not in key['trait_id_opt_indicator']:
                    return key['contact_point_category_code'], key['contact_point_type_code'], \
                        key['trait_id_opt_indicator']

            return key['contact_point_category_code'], key['contact_point_type_code'], None

        return None, None, None

    def build_payload_for_opts(self, mpn, payload_from_consents):

        opts_payload = []
        for index, value in enumerate(payload_from_consents['contact_point_type_name']):
            contact_category, contact_type, trait_id_opt_indicator = self.filter_values_from_crs_data(
                mpn, payload_from_consents['service_name'][index], payload_from_consents['contact_point_type_name'][index])

            format_opt_choice_date = payload_from_consents['opt_date'][index].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"

            _base_opts_payload = {
                "contactCategory": contact_category,
                "sourceSystem": payload_from_consents['source_system'][index],
                "optIpAddress": "",
                "contactType": contact_type,
                "unsubscribeURL": payload_from_consents['unsubscribe_url'][index],
                "contactPointTypeName": payload_from_consents['contact_point_type_name'][index],
                "optStatus": payload_from_consents['opt_status'][index],
                "optReason": payload_from_consents['opt_reason'][index],
                "initialOptStatus": payload_from_consents['initial_opt_status'][index],
                "traitIdOptIndicator": trait_id_opt_indicator,
                "optNumber": str(payload_from_consents['opt_number'][index]),
                "optText": payload_from_consents['opt_text'][index],
                "optChoiceDate": format_opt_choice_date,
                "optId": payload_from_consents['opt_id'][index],
                "optServiceName": payload_from_consents['service_name'][index]
            }

            opts_payload.append(_base_opts_payload)

        return opts_payload

    def build_consent_service_payload(self, payload, max_time_stamp):

        _base_consent_service_payload = {
            "traceId": payload['trace_id'][0],
            "pipeline": "cdp-api-registrations",
            "accountId": "",
            "receivedTimestamp": self.current_timestamp,
            "rawPayload": {
                "sourceId": str(payload['source_id'][0]),
                "transmitterSourceId": str(payload['source_id'][0]),
                "specVersion": "1",
                "data": {
                    "traits": [{
                        "traitName": "lastActivityDate",
                        "values": max_time_stamp,
                        "collectedDate": max_time_stamp,
                        "collectedBy": ""}],
                    "ids": {
                        "sourceId": str(payload['source_id'][0]),
                        "consumerId": payload['consumer_id'][0],
                        "marketingProgramNumber": str(payload['marketing_program_number'][0]),
                        "phoneNumber": payload['phone_number'][0],
                        "originalAddress": {
                            "territoryName": payload['territory_name'][0],
                            "streetComplement": payload['street_complement'][0],
                            "poBoxNumber": payload['po_box_number'][0],
                            "streetNumber": payload['street_number'][0],
                            "subBuilding": payload['sub_building'][0],
                            "houseNumber": payload['house_number'][0],
                            "box": payload['po_box'][0],
                            "building": payload['building'][0],
                            "streetName": payload['street_name'][0],
                            "cityName": payload['city_name'][0],
                            "cityComplement": payload['city_complement'][0],
                            "addressLine1": payload['address_line1'][0],
                            "postalAreaCode": payload['postal_area_code'][0],
                            "addressLine2": payload['address_line2'][0],
                            "addressLine3": payload['address_line3'][0],
                            "apartmentNumber": payload['apartment_number'][0]
                        },
                        "countryCode": payload['country_code'][0],
                        "email": payload['email'][0]
                    },
                    "dependents": []
                },
                "requestTimestamp": max_time_stamp,
                "requestId": "data-migration",
                "marketingProgramNumber": str(payload['marketing_program_number'][0]),
                "eventName": ""
            },
            "legalEntity": str(payload['legal_entity_id'][0])
        }

        return _base_consent_service_payload

    def process(self, element, *args, **kwargs):

        map_elements_with_schema = {key: tuple(value[key] for value in element if key in value)
                                    for data in element for key in data}
        get_payload_for_opts = self.build_payload_for_opts(map_elements_with_schema['marketing_program_number'][0],
                                                           map_elements_with_schema)

        max_time_stamp = max(map_elements_with_schema['opt_date']).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "Z"

        consent_service_payload = self.build_consent_service_payload(map_elements_with_schema, max_time_stamp)
        consent_service_payload['rawPayload']['data']['opts'] = get_payload_for_opts

        yield json.dumps(consent_service_payload)
