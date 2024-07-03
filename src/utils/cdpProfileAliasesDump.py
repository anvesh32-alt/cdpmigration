from google.cloud import bigquery
import logging
from helpers import constants


def cdp_profile_aliases_dumps(**kwargs):

  dataset_region = kwargs['dataset_region']
  env = kwargs['env']
  region = kwargs['region']
  client = bigquery.Client(constants.ISL_PROD_PROJECT)
  logging.info("Initializing with Bigquery and bucket completed Successfully.")

  #Query for creating CDP Profile Aliases Dumps Query
  profile_aliases_dump_query = f"""
  CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.profile_aliases_{env}` AS
  select * from EXTERNAL_QUERY ("{constants.PROJECT_TABLES[region]}.{dataset_region}.cdp-profiles{constants.CONNECTION_TYPE[env]}_cdp-profiles_pii",
    "SELECT canonical_pg_id,pg_id,last_updated_date FROM profile_aliases");
  """

  print("CDP Profile Aliases Dumps Query :",profile_aliases_dump_query)
  _ = client.query(profile_aliases_dump_query).to_dataframe()
