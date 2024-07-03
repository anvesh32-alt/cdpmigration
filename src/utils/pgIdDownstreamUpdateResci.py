import ast
import datetime
import os
import re
import sys
from google.cloud import bigquery
from google.cloud import storage
import paramiko
import logging
from zipfile import ZipFile

from helpers.sftpConfig import sftp_config
from helpers import constants
from utils.pgIdDownstreamUpdate import sftp_file_upload
from utils.getValueFromTraitsOrProperties import get_authentication_key_from_secret_manager

current_date = datetime.date.today().strftime("%Y-%m-%d")
replace_dash_from_current_date = current_date.replace("-", "")


def sftp_file_download_to_gcs(**kwargs):
    downstream_system = kwargs['downstream_system']
    mpn = kwargs['mpn']
    sftp_details = sftp_config[downstream_system]
    hostname = sftp_details['HOSTNAME']
    port = sftp_details['PORT']
    username = sftp_details['USERNAME']
    password = get_authentication_key_from_secret_manager(constants.RESCI_SFTP_SECRET_KEY)

    if not os.path.exists(constants.AIRFLOW_LOCAL_TMP_PATH):
        # if the "/tmp/migration_pg_id_update/" directory is not present then create it
        os.makedirs(constants.AIRFLOW_LOCAL_TMP_PATH)
    try:
        # create ssh client
        with paramiko.SSHClient() as ssh_client:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=hostname, port=port, username=username, password=password)
            logging.info(f"Connection Established Successfully For {downstream_system.upper()}, "
                         f"hostname: {hostname}, username: {username}, port: {port}")

            # create an SFTP client object
            sftp = ssh_client.open_sftp()
            list_files_in_dir = sftp.listdir(constants.RESCI_REMOTE_PATH)
            remote_file_name = [file for file in list_files_in_dir if re.search(fr'^{mpn}', file)][0]
            remote_file_path = f"{constants.RESCI_REMOTE_PATH}/{remote_file_name}"
            temp_file_path = f'{constants.AIRFLOW_LOCAL_TMP_PATH}{remote_file_name}'
            logging.info(f"Downloading File to local System")
            try:
                # Download a file from the remote server
                sftp.get(remote_file_path, temp_file_path)
                logging.info(f"Files has been downloaded from {downstream_system.upper()} SFTP Server, "
                             f"remote file path: {remote_file_path}, local_file_path: {temp_file_path}")
                # close the connection
                sftp.close()

                # Unzip the file loading the zip file and creating a zip object
                logging.info(f"Unzipping the File.")
                with ZipFile(temp_file_path, 'r') as zip_file:
                    zipped_file_name = zip_file.namelist()[0]
                    # Extracting all the members of the zip into a specific location.
                    zip_file.extractall(path=constants.AIRFLOW_LOCAL_TMP_PATH)
                    temp_zipped_file_path = f"{constants.AIRFLOW_LOCAL_TMP_PATH}{zipped_file_name}"
                    logging.info(f"Files have been unzipped to path: {temp_zipped_file_path}")

                # Moving the file from temp to gcs bucket
                logging.info(f"Moving the File to GCS bucket from local path")
                client_storage = storage.Client(constants.PROJECT)
                bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)
                resci_gcs_file_path = f"migration_pg_id_update/resci/{mpn}/{current_date}/downloads/{zipped_file_name}"
                blob = bucket.blob(resci_gcs_file_path)
                blob.upload_from_filename(temp_zipped_file_path)
                logging.info(f"Blob downloaded to gcs path {resci_gcs_file_path}")

            finally:
                # Always delete the file
                logging.info(f"Deleting temporary file: {temp_file_path}")
                os.remove(temp_file_path)
                logging.info(f"Deleting temporary file: {temp_zipped_file_path}")
                os.remove(temp_zipped_file_path)
        return f"gs://{constants.MIGRATION_BUCKET}/{resci_gcs_file_path}"

    except IndexError:
        logging.exception(f"File Pattern did not match with Mpn, Exiting..")
        sys.exit(1)

    except Exception as err:
        logging.exception(f"Unable to Download File From {downstream_system.upper()} SFTP server, err: {err}")
        sys.exit(1)

    finally:
        sftp.close()


def upload_resci_file_to_bigquery(**kwargs):
    gcs_file_uri = kwargs['file_path']
    persona = kwargs['persona'].replace("-", "_")
    try:
        client = bigquery.Client(constants.PROJECT)   # Construct a BigQuery client object.
        table_id = f"{constants.PROJECT}.{constants.MIGRATION_DATASET}.resci_email_account_mapping_{persona}"
        logging.info(f"Creating Load to Bigquery job, gcs path: {gcs_file_uri}, table id: {table_id}")
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("record_id", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("account_created_on", "DATE"),
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        load_job = client.load_table_from_uri(
            gcs_file_uri, table_id, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)  # Make an API request.
        logging.info(f"Loaded {destination_table.num_rows} rows to table, {table_id}")
        return table_id

    except Exception as err:
        logging.exception(f"Unable to Upload File to Bigquery, err: {err}")
        sys.exit(1)


def create_pg_id_mapping_file_for_resci(**kwargs):
    gcs_file_uri = kwargs['file_path']
    table_id = kwargs['table_id']
    mpns_for_query = kwargs['mpns_for_query']
    mpn = kwargs['mpn']
    country_code = kwargs['country_code']

    _base_query_to_get_pg_id_mapping = f"""
    WITH id_graphs AS (SELECT * FROM EXTERNAL_QUERY("dbce-c360-segamaws-prod-9669.us-east4.cdp-ids_cdp-ids_pii",
    "select id_value,pg_id from id_graphs where id_type='email' and marketing_program_number in {mpns_for_query}"))
    ,exclude_list AS (select email, count(distinct record_id) as counts,array_agg(record_id) from (
    select distinct idg.pg_id as record_id,lower(resci.email) as email,resci.account_created_on
    from `{table_id}` resci left join id_graphs idg on lower(idg.id_value)=lower(resci.email)) 
    group by 1 having counts > 1)
    select distinct idg.pg_id as record_id,lower(resci.email) as email,resci.account_created_on
    from `{table_id}` resci left join id_graphs idg on lower(idg.id_value)=lower(resci.email)
    where lower(resci.email) NOT IN (select email from exclude_list)
    and idg.pg_id is not null
    """
    try:
        logging.info(f"Executing query in Bigquery, query: {_base_query_to_get_pg_id_mapping}")
        client = bigquery.Client(project=constants.PROJECT)
        pg_id_mapping_for_resci = client.query(_base_query_to_get_pg_id_mapping).to_dataframe()
        logging.info(f"Query Executed in Bigquery, number of rows: {len(pg_id_mapping_for_resci)}")
        client_storage = storage.Client(constants.PROJECT)
        bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)
        site_id = gcs_file_uri.split("/")[-1].split("_")[1]

        resci_file_path = (
            f"migration_pg_id_update/resci/{mpn}/{current_date}/uploads/CDP2UpdatesResci_{country_code}"
            f"_{mpn}_{site_id}_{replace_dash_from_current_date}.tsv")

        bucket.blob(resci_file_path).upload_from_string(pg_id_mapping_for_resci.to_csv(sep='\t', index=False),
                                                        'text/tsv')

        logging.info(f"File Uploaded To Path: {resci_file_path}")
        # return full gsutil path
        return (resci_file_path, )

    except Exception as err:
        logging.exception(f"Error Occurred While Running the PG ID update process for Resci:, err: {err}")
        sys.exit(1)


def upload_resci_file_to_sftp_server(**kwargs):
    file_path = ast.literal_eval(kwargs['file_path'])
    downstream_system = kwargs['downstream_system']
    sftp_details = sftp_config[downstream_system]
    hostname = sftp_details['HOSTNAME']
    port = sftp_details['PORT']
    username = sftp_details['USERNAME']
    password = get_authentication_key_from_secret_manager(constants.RESCI_SFTP_SECRET_KEY)

    remote_path = "/uploads"
    input_file_name = file_path[0].split("/")[-1]
    sftp_file_upload(hostname, port, username, password, downstream_system, [file_path[0]], remote_path)

    return remote_path, input_file_name
