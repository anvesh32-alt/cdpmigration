import datetime
import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound
import pandas as pd
import logging
import sys
import paramiko
import ast

from helpers import constants, bigqueryDatasetsConfig, sftpConfig
from utils.apiCallsToLytics import get_auth_id_for_job_submission
from utils.getValueFromTraitsOrProperties import get_authentication_key_from_secret_manager


def create_mapping_table_for_segment_vs_pg_id(**kwargs):
    """
    Creates a mapping table of user_id vs pg_id for a specific mpn, if the table already exist then the same table
    will be used.
    The table created will be used for Mapp and Lytics both.
    """

    mpns_for_query = kwargs['mpns_for_query']
    mpn = kwargs['mpn']
    table_name = bigqueryDatasetsConfig.bigquery_table_config[mpn].get('pg_id_downstream_update_table')
    table_id = f'{constants.PROJECT}.{constants.MIGRATION_DATASET}.{table_name}'
    client = bigquery.Client(constants.PROJECT)
    try:
        client.get_table(table_id)  # Make an API request.
        logging.info(f'Table {table_id} already exists, Exiting the flow.')

    except NotFound:
        base_query = """
        WITH id_graphs AS (SELECT * FROM 
        EXTERNAL_QUERY("dbce-c360-segamaws-prod-9669.us-east4.cdp-ids_cdp-ids_pii", 
        "select id_value,pg_id from id_graphs where id_type='userId' and marketing_program_number IN {}")), 
        earliest_gen_id AS (SELECT pg_id, MIN(created_date) AS created_date FROM 
        EXTERNAL_QUERY("dbce-c360-segamaws-prod-9669.us-east4.cdp-ids_cdp-ids_pii", "select * from id_aliases") GROUP BY 
        pg_id) SELECT graphs.pg_id as pg_ids, graphs.id_value as user_ids FROM earliest_gen_id gen JOIN id_graphs graphs
        ON graphs.pg_id=gen.pg_id QUALIFY RANK() OVER (PARTITION BY graphs.id_value ORDER BY gen.created_date ASC) = 1
        """
        format_base_query = base_query.format(mpns_for_query)
        logging.info(f"Job Submitted to Bigquery query: {format_base_query}")
        segment_vs_pg_id_df = client.query(format_base_query).to_dataframe()

        # Load to bigquery table
        if len(segment_vs_pg_id_df.index) > 0:
            job_config = bigquery.job.LoadJobConfig()
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            bigquery_job = client.load_table_from_dataframe(segment_vs_pg_id_df, table_id, job_config=job_config).result()
            table = client.get_table(table_id)  # Make an API request.
            logging.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
        else:
            logging.exception(f"The query gave 0 result hence exiting the process, Please check things before "
                              f"proceeding")
            sys.exit(1)

    except Exception as err:
        logging.exception(f"Unable to Create Segment ID vs Pg ID Mapping Table: err, {err}")
        sys.exit(1)


def create_segment_vs_pg_id_file_for_mapp(**kwargs):
    """ Creates the email and ids file for Mapp PG id updates and dumps the same in GCS bucket. """

    country_code = kwargs['country_code']
    mpn = kwargs['mpn']

    current_date = datetime.date.today().strftime("%Y-%m-%d")
    try:
        logging.info(f"Reading Data From Bigquery For Mapp file creation")
        client = bigquery.Client(constants.PROJECT)
        client_storage = storage.Client(constants.PROJECT)
        bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)

        # Read the data from the table for pg_id vs user_id
        dataset_ref = bigquery.DatasetReference(constants.PROJECT, constants.MIGRATION_DATASET)
        table_name = bigqueryDatasetsConfig.bigquery_table_config[mpn].get('pg_id_downstream_update_table')
        table_ref = dataset_ref.table(table_name)
        logging.info(f"Reading Data From Table For MAPP PG ID Update: {table_ref}")
        table = client.get_table(table_ref)
        segment_vs_pg_id_df = client.list_rows(table).to_dataframe()
        segment_vs_pg_id_df.drop_duplicates(inplace=True)

        logging.info(f"Data read from Bigquery table, table name: {table_ref}, table row count: {table.num_rows}")

        # Update the Fields with MPN/ Identifier
        segment_vs_pg_id_df['user_ids'] = segment_vs_pg_id_df['user_ids'].astype(str) + '_' + mpn + '@fakedomain.com'
        segment_vs_pg_id_df['pg_ids'] = segment_vs_pg_id_df['pg_ids'].astype(str) + '_' + mpn + '@fakedomain.com'

        # Update the column names with headers for first file, id file (userid vs pg_id)
        segment_vs_pg_id_df.rename(columns={'user_ids': 'User.email'}, inplace=True)
        segment_vs_pg_id_df.rename(columns={'pg_ids': 'User.identifier'}, inplace=True)

        # Update the column names with headers for second file, email file (pg_id vs pg_id)
        pg_id_vs_pg_id_df = pd.DataFrame({'User.email': segment_vs_pg_id_df['User.identifier'].values,
                                          'User.identifier': segment_vs_pg_id_df['User.identifier'].values
                                          })

        replace_dash_from_current_date = current_date.replace("-", "")
        file_path_email_file = (
            f"migration_pg_id_update/mapp/{mpn}/{current_date}/CDP2Updates_{country_code}{mpn}_email_"
            f"{replace_dash_from_current_date}.csv")

        file_path_ids_file = (f"migration_pg_id_update/mapp/{mpn}/{current_date}/CDP2Updates_{country_code}{mpn}_ids_"
                              f"{replace_dash_from_current_date}.csv")
        # Upload Email file to GCS
        bucket.blob(file_path_email_file).upload_from_string(pg_id_vs_pg_id_df.to_csv(index=False), 'text/csv')
        # Upload Ids file to GCS
        bucket.blob(file_path_ids_file).upload_from_string(segment_vs_pg_id_df.to_csv(index=False), 'text/csv')
        logging.info(f"Email file Uploaded To Path: {file_path_email_file}, IDs File Uploaded to Path: "
                     f"{file_path_ids_file}")

        return file_path_email_file, file_path_ids_file

    except Exception as err:
        logging.exception("Error Occurred While Running the PG ID update process for MAPP:", err)
        sys.exit(1)


def create_segment_vs_pg_id_file_for_lytics(**kwargs):
    """ Creates the email and ids file for Lytics PG id updates and dumps the same in GCS bucket. """

    country_code = kwargs['country_code']
    mpn = kwargs['mpn']

    current_date = datetime.date.today().strftime("%Y-%m-%d")
    try:
        logging.info(f"Reading Data From Bigquery For Lytics file creation")
        client = bigquery.Client(constants.PROJECT)
        client_storage = storage.Client(constants.PROJECT)
        bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)

        # Read the data from the table for pg_id vs user_id
        dataset_ref = bigquery.DatasetReference(constants.PROJECT, constants.MIGRATION_DATASET)
        table_name = bigqueryDatasetsConfig.bigquery_table_config[mpn].get('pg_id_downstream_update_table')
        table_ref = dataset_ref.table(table_name)
        logging.info(f"Reading Data From Table Lytics Update: {table_ref}")
        table = client.get_table(table_ref)
        segment_vs_pg_id_df = client.list_rows(table).to_dataframe()
        segment_vs_pg_id_df.drop_duplicates(inplace=True)

        logging.info(f"Data read from Bigquery table, table name: {table_ref}, table row count: {table.num_rows}")

        # Update the column names with headers previousId, userId, type
        segment_vs_pg_id_df.rename(columns={'user_ids': 'previousId'}, inplace=True)
        segment_vs_pg_id_df.rename(columns={'pg_ids': 'userId'}, inplace=True)
        segment_vs_pg_id_df['type'] = 'alias'

        replace_dash_from_current_date = current_date.replace("-", "")
        lytics_file_path = (
            f"migration_pg_id_update/lytics/{mpn}/{current_date}/CDP2UpdatesLytics_{country_code}_"
            f"{replace_dash_from_current_date}.csv")

        # Upload file to GCS bucket
        bucket.blob(lytics_file_path).upload_from_string(segment_vs_pg_id_df.to_csv(index=False), 'text/csv')
        logging.info(f"File Uploaded To Path For PG ID Update For Lytics : {lytics_file_path}")

        return (lytics_file_path,)

    except Exception as err:
        logging.exception(f"Error Occurred While Running the PG ID update process for Lytics:, err: {err}")
        sys.exit(1)


def get_values_from_bigquery(query):

    logging.info(f"Getting values from Bigquery: {query}")
    client = bigquery.Client(project=constants.PROJECT)
    result = client.query(query).to_dataframe().to_dict(orient='records')
    return result


def get_aid_for_lytics_updates(mpn):
    """ Gets the Lytics Account id for a particular mpn, the same will be used were the SFTP will be used.  """

    _base_lytics_account_id_query = """
    SELECT a.ecoSystemMarketingProgramKey FROM `dbce-c360-isl-preprod-d942.CDS.ecosystemmpn` a 
    JOIN `dbce-c360-isl-preprod-d942.CDS.ecosystem` b ON a.ecoSystemId = b.ecoSystemId 
    WHERE b.description = 'lytics' AND a.marketProgramNumber = {}
    """
    try:
        format_base_query = _base_lytics_account_id_query.format(mpn)
        logging.info(f"Getting Account ID for Lytics for MPN: {mpn}, query: {format_base_query}")
        lytics_account_id = (get_values_from_bigquery(format_base_query))[0]['ecoSystemMarketingProgramKey']
        logging.info(f"Account ID for MPN: {mpn} is {lytics_account_id}")
        if lytics_account_id:
            return lytics_account_id
        else:
            raise Exception

    except Exception as err:
        logging.exception(f"Unable to Get Account ID for Lytics, err: {err}")
        sys.exit(1)


def get_instance_for_mapp_sftp(mpn):
    """ Gets the mapp instance for a particular mpn, the same will be used were the SFTP will be used. """

    _base_mapp_instance_query = """
        SELECT a.ecoSystemMarketingProgramInstance FROM `dbce-c360-isl-preprod-d942.CDS.ecosystemmpn` a 
        JOIN `dbce-c360-isl-preprod-d942.CDS.ecosystem` b ON a.ecoSystemId = b.ecoSystemId WHERE 
        b.description = 'mapp' and a.marketProgramNumber = {}
        """
    try:
        format_base_query = _base_mapp_instance_query.format(mpn)
        logging.info(f"Getting Instance for Mapp for MPN: {mpn}, query: {format_base_query}")
        mapp_instance = (get_values_from_bigquery(format_base_query))[0]['ecoSystemMarketingProgramInstance']
        logging.info(f"Mapp Instance for MPN: {mpn} is {mapp_instance}")
        if mapp_instance:
            return mapp_instance
        else:
            raise Exception

    except Exception as err:
        logging.exception(f"Unable to Get Mapp Instance, err: {err}")
        sys.exit(1)


def sftp_file_upload(hostname, port, username, password, downstream_system, input_paths, remote_path):

    client_storage = storage.Client(constants.PROJECT)
    bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)

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
            # Create a sub folder if it doesn't exist.
            if downstream_system == 'mapp':
                instance_name = remote_path.split("/")[-1]
                logging.info(f"Remote Path: {remote_path}, instance name: {instance_name}")
                if instance_name not in sftp.listdir("/".join(remote_path.split("/")[:-1])):
                    sftp.mkdir(remote_path)

            for input_file_path in input_paths:
                file_name = input_file_path.split("/")[-1]
                if remote_path:
                    remote_file_path = f"{remote_path}/{file_name}"
                else:
                    remote_file_path = file_name
                # upload a file from the remote server
                blob = bucket.blob(input_file_path)
                local_file_path = f'{constants.AIRFLOW_LOCAL_TMP_PATH}{file_name}'
                blob.download_to_filename(local_file_path)
                logging.info(f"Blob downloaded to local path {local_file_path}")

                try:
                    sftp.put(local_file_path, remote_file_path)
                    logging.info(f"Files has been uploaded to {downstream_system.upper()} SFTP Server, "
                                 f"input_file_path: {input_file_path}, remote file path: {remote_file_path}")

                finally:
                    # Always delete the file
                    logging.info(f"Deleting temporary file: {local_file_path}")
                    os.remove(local_file_path)

    except Exception as err:
        logging.exception(f"Unable to Upload File to {downstream_system.upper()} SFTP server, err: {err}")
        sys.exit(1)

    finally:
        # close the connection
        sftp.close()


def pg_id_update_in_downstream(**kwargs):
    downstream_system = kwargs['downstream_system']
    mpn = kwargs['mpn']
    file_path = ast.literal_eval(kwargs['file_path'])
    sftp_details = sftpConfig.sftp_config[downstream_system]

    logging.info(f"Details: {sftp_details}, Downstream system: {downstream_system}")
    hostname = sftp_details['HOSTNAME']
    port = sftp_details['PORT']

    if downstream_system == 'mapp':
        username = sftp_details['USERNAME']
        password = get_authentication_key_from_secret_manager(constants.MAPP_SFTP_SECRET_KEY)
        get_mapp_instance = get_instance_for_mapp_sftp(mpn)
        logging.info(f"Mpn: {mpn}, Instance ID: {get_mapp_instance}, file path: {file_path}")

        email_file_name = file_path[0].split("/")[-1]
        ids_file_name = file_path[1].split("/")[-1]
        remote_file_path = f"/data/CDP2Updates/{get_mapp_instance}"

        # Call the SFTP Method to Upload the SFTP File
        sftp_file_upload(hostname, port, username, password, downstream_system, file_path, remote_file_path)
        return email_file_name, ids_file_name, remote_file_path

    elif downstream_system == 'lytics' or 'lastmapp':
        get_account_id_for_lytics = get_aid_for_lytics_updates(mpn)
        get_values_from_secret_manager_to_authenticate = (ast.literal_eval(get_authentication_key_from_secret_manager(
            constants.LYTICS_SFTP_SECRET_KEY)))[get_account_id_for_lytics]
        auth_token_for_aid = get_values_from_secret_manager_to_authenticate['auth_token']
        auth_id_for_job_submission = get_auth_id_for_job_submission(auth_token_for_aid)

        if auth_id_for_job_submission:
            username = get_values_from_secret_manager_to_authenticate['username']
            password = get_values_from_secret_manager_to_authenticate['password']
            input_file_name = file_path[0].split("/")[-1]
            remote_file_path = ""   # file to be uploaded at base AID location
            logging.info(f"Mpn: {mpn}, Account ID: {get_account_id_for_lytics}, file path: {file_path}, "
                         f"file name: {input_file_name}")

            # Call the SFTP Method to Upload the SFTP File
            sftp_file_upload(hostname, port, username, password, downstream_system, [file_path[0]], remote_file_path)
            return get_account_id_for_lytics, input_file_name

        else:
            logging.exception(f"Auth ID Not Present, Please create one and re-run the process")
            sys.exit(1)


def create_segment_vs_pg_id_file_for_lastmapp(**kwargs):
    """ Creates the email and ids file for LastMapp PG id updates and dumps the same in GCS bucket. """

    country_code = kwargs['country_code']
    mpn = kwargs['mpn']

    current_date = datetime.date.today().strftime("%Y-%m-%d")
    try:
        logging.info(f"Reading Data From Bigquery For LastMapp file creation")
        client = bigquery.Client(constants.PROJECT)
        client_storage = storage.Client(constants.PROJECT)
        bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)

        # Read the data from the table for pg_id vs user_id
        dataset_ref = bigquery.DatasetReference(constants.PROJECT, constants.MIGRATION_DATASET)
        table_name = bigqueryDatasetsConfig.bigquery_table_config[mpn].get('pg_id_downstream_update_table')
        table_ref = dataset_ref.table(table_name)
        logging.info(f"Reading Data From LastMapp Lytics Update: {table_ref}")
        table = client.get_table(table_ref)
        segment_vs_pg_id_df = client.list_rows(table).to_dataframe()
        segment_vs_pg_id_df.drop_duplicates(inplace=True)

        logging.info(f"Data read from Bigquery table, table name: {table_ref}, table row count: {table.num_rows}")

        # Update the column names with headers userId, traits.mappemail
        segment_vs_pg_id_df.rename(columns={'pg_ids': 'userId'}, inplace=True)
        segment_vs_pg_id_df['traits.mappemail'] = segment_vs_pg_id_df['userId'].astype(str) + '_' + mpn + '@fakedomain.com'
        del segment_vs_pg_id_df['user_ids']

        replace_dash_from_current_date = current_date.replace("-", "")
        lastmapp_file_path = (
            f"migration_pg_id_update/lytics/{mpn}/{current_date}/CDP2UpdatesLyticsMapp_{country_code}_"
            f"{replace_dash_from_current_date}.csv")

        # Upload file to GCS bucket
        bucket.blob(lastmapp_file_path).upload_from_string(segment_vs_pg_id_df.to_csv(index=False), 'text/csv')
        logging.info(f"File Uploaded To Path For PG ID Update For LastMapp : {lastmapp_file_path}")

        return (lastmapp_file_path,)

    except Exception as err:
        logging.exception(f"Error Occurred While Running the PG ID update process for LastMapp:, err: {err}")
        sys.exit(1)
