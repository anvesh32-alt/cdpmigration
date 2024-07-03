from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging
import sys
# sys.path.append('/home/airflow/gcs/dags/cdp2.0-migration/src/')
from helpers import constants, dqValidationQueries,bigqueryDatasetsConfig


def generate_opts_validation_report(mpn,validation_id):

    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")
    # get table name
    failure_table_name=bigqueryDatasetsConfig.bigquery_table_config[mpn]['front_load_validation_failure_table']
    destination_table_name=bigqueryDatasetsConfig.bigquery_table_config[mpn]['destination_registration_table']
    # Validate if mpn and country combination exists in CRS
    if validation_id == 'vld2':
        query = dqValidationQueries.vld2_query.format(failure_table_name,destination_table_name,destination_table_name,mpn)
    # Validate if mpn is Active from CRS
    elif validation_id == 'vld3':
        query = dqValidationQueries.vld3_query.format(failure_table_name,destination_table_name, mpn)
    # Loyalty account number can be alphanumeric and opt Ip address is optionalfield
    elif validation_id == 'vld4':
        query = dqValidationQueries.vld4_query.format(failure_table_name,mpn ,destination_table_name)
    # Either of optId, opt number or opt service name is required field
    elif validation_id == 'vld5':
        query = dqValidationQueries.vld5_query.format(failure_table_name,mpn ,destination_table_name)
    # The Universal request with mpn 9999 should have legalEntity trait
    elif validation_id == 'vld6':
        query = dqValidationQueries.vld6_query.format(failure_table_name,mpn,destination_table_name)
    logging.info(f"Dq Validation for mpn: {mpn}, query: {query}")

    try:
        result=client.query(query)
        logging.info(f"Dq Validation query for mpn: {mpn}, is completed : {result}")
    except Exception as err:
        logging.exception("Error Occurred While Running the Opts Validation Report:", err)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # Read the command line arguments
    validation_id=sys.argv[1]
    mpn = sys.argv[2]
    generate_opts_validation_report(mpn,validation_id)
