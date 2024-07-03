from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging
import sys
# sys.path.append('/home/airflow/gcs/dags/cdp2.0-migration/src/')
from helpers import constants, validationReportQueries


def generate_opts_validation_report(region, market, mpn):

    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        client_storage = storage.Client(constants.PROJECT)
        bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    base_opts_query = validationReportQueries.opt_validation_base_query
    query = base_opts_query.format(market, region, market, region, mpn, market, region, market)
    logging.info(f"Opts Reports Validation for mpn: {mpn}, query: {query}")

    try:
        opts_report_df = client.query(query).to_dataframe()
        opts_report_df['action'] = ''
        df_query = opts_report_df.drop_duplicates()
        opts_report = df_query.reset_index(drop=True)
        bucket.blob("migration_reports/opts_report/opts_reports_for_" + mpn + "_.csv").upload_from_string(
            opts_report.to_csv(index=False), 'text/csv')

    except Exception as err:
        logging.exception("Error Occurred While Running the Opts Validation Report:", err)

    # Load to bigquery table
    table_name = f"opts_validation_report_for_{mpn}"
    table_id = f"{constants.PROJECT}.{constants.MIGRATION_DATASET}.{table_name}"
    job_config = bigquery.job.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    bigquery_job = client.load_table_from_dataframe(opts_report, table_id, job_config=job_config).result()
    table = client.get_table(table_id)  # Make an API request.
    logging.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # Read the command line arguments
    dataset_region = sys.argv[1]
    market = sys.argv[2]
    mpn = sys.argv[3]
    generate_opts_validation_report(dataset_region, market, mpn)
