import json
import logging
import apache_beam as beam
from google.cloud import bigquery

from helpers import constants
from utils.getValueFromTraitsOrProperties import check_key_exists


class HandleCrsTraitAliases(beam.DoFn):

    def __init__(self):
        self.validation_data_for_aliases_from_cds_table = None

    def setup(self):
        self.validation_data_for_aliases_from_cds_table = self.get_values_for_trait_aliases()

    def get_values_for_trait_aliases(self):
        """ Get the values from CDS table to validate aliases"""

        cds_sql_query = """
            SELECT traitId, traitName, aliases AS alias FROM `dbce-c360-isl-preprod-d942.cdp_migration.crs_aliases`
            WHERE traitName != aliases
        """

        try:
            logging.info(f"Getting values from CDS table to validate aliases, query: {cds_sql_query}")
            client = bigquery.Client(project=constants.PROJECT)
            self.validation_data_for_aliases_from_cds_table = (client.query(cds_sql_query).to_dataframe().
                                                               to_dict(orient='index'))

            logging.info(f"Values from CDS table to validate aliases, fetched successfully, "
                         f"record count: {len(self.validation_data_for_aliases_from_cds_table)}")

            return self.validation_data_for_aliases_from_cds_table
        except Exception as err:
            logging.exception(f"Error while getting records from CDS table error: {err}")

    def process(self, element, *args, **kwargs):

        data = json.loads(element)
        try:
            traits_block = check_key_exists(data, 'traits')
            if traits_block:
                remove_empty_traits = self.remove_empty_traits(traits_block)
                updated_traits_block = self.validate_trait_name_and_aliases(remove_empty_traits)
                data['traits'] = updated_traits_block
                yield json.dumps(data)
            else:
                yield element
        except KeyError:
            yield element
        except Exception as err:
            logging.exception(f"Unable to handle CRS trait aliases, err: {err}")

    def remove_empty_traits(self, traits_block):

        updated_trait_block = {}
        for trait_key, trait_value in traits_block.items():
            if isinstance(trait_value, str) and trait_key == 'stdAddress' and trait_value.strip() != "":
                updated_trait_block[trait_key] = trait_value
            elif isinstance(trait_value, str) and trait_value and trait_key != 'stdAddress':
                updated_trait_block[trait_key] = trait_value
            elif isinstance(trait_value, list) or isinstance(trait_value, bool) or isinstance(trait_value, dict)\
                    or isinstance(trait_value, int):
                updated_trait_block[trait_key] = trait_value

        return updated_trait_block

    def validate_trait_name_and_aliases(self, traits_block):

        for index, value in self.validation_data_for_aliases_from_cds_table.items():
            trait_name = value.get('traitName')
            alias = value.get('alias')

            if trait_name and alias in traits_block:
                check_trait_name_in_payload = traits_block.get(trait_name)
                check_alias_in_payload = traits_block.get(alias)
                if check_trait_name_in_payload == check_alias_in_payload:
                    del traits_block[alias]

        return traits_block
