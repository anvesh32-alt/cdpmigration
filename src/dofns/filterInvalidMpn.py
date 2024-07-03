import apache_beam as beam
import json
import logging


class FilterInvalidMpn(beam.DoFn):
    """
    Extension of beam DoFn filtering pcollection to Filter input data with invalid Mpn.
    """

    def check_key_exists(self, element, *keys):
        """
        Check if *keys (nested) exists in `element` (dict).
        """

        if len(keys) == 0:
            raise AttributeError('keys_exists() expects at least two arguments, one given.')

        _element = element
        for key in keys:
            try:
                _element = _element[key]
            except Exception:
                return False
        return _element

    def process(self, element, invalid_mpns):
        """
        Filter the input data based on valid/invalid mpn, based on 4 checks and the values of invalid mpns passed
        :param element: pcollection of input data
        :param invalid_mpns: list -> all the invalid mpn to be filtered out
        :return: 0 in case of mpn matches with invalid mpn else 1
        """

        data = json.loads(element)

        try:
            # checks if mpn is present in data['properties']['marketingProgramNumber']
            if self.check_key_exists(data, 'properties', 'marketingProgramNumber') in invalid_mpns:
                yield beam.pvalue.TaggedOutput('invalid_mpn_data', element)

            # checks if mpn is present in data['traits']['marketingProgramNumber']
            elif self.check_key_exists(data, 'traits', 'marketingProgramNumber') in invalid_mpns:
                yield beam.pvalue.TaggedOutput('invalid_mpn_data', element)

            # checks if mpn is present in data['context']['traits']['marketingProgramNumber']
            elif self.check_key_exists(data, 'context', 'traits', 'marketingProgramNumber') in invalid_mpns:
                yield beam.pvalue.TaggedOutput('invalid_mpn_data', element)

            # checks if mpn is present in data['context']['externalIds']
            elif self.check_key_exists(data, 'context', 'externalIds'):
                external_id_payload = self.check_key_exists(data, 'context', 'externalIds')
                if external_id_payload:
                    external_id_payload_mpn = next((item for item in external_id_payload
                                                    if item["type"] == "marketingProgramNumber"), False)
                    if external_id_payload_mpn and external_id_payload_mpn['id'] in invalid_mpns:
                        yield beam.pvalue.TaggedOutput('invalid_mpn_data', element)

                    else:
                        yield beam.pvalue.TaggedOutput('valid_mpn_data', element)
            else:
                yield beam.pvalue.TaggedOutput('valid_mpn_data', element)

        except Exception as err:
            logging.exception(f"Failed to Filter records, err: {str(err)}, {element}")
