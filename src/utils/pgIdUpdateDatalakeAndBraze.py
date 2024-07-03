import asyncio
import datetime
import itertools
import json
import logging
import sys
import aiohttp
from aiolimiter import AsyncLimiter
from google.cloud import bigquery
from requests import RequestException
from helpers import constants, bigqueryDatasetsConfig
from utils.getValueFromTraitsOrProperties import get_authentication_key_from_secret_manager

BQ_BATCH_SIZE = 50000
BRAZE_API_CHUNK_SIZE = 45
limiter = AsyncLimiter(54000)


def create_mapping_table_for_segment_vs_pg_id_for_datalake_or_braze(**kwargs):
    """
    Creates a mapping table of user_id vs pg_id for a specific mpn for datalake
    """
    mpn = kwargs['mpn']
    downstream_system = kwargs['downstream_system']
    table_name = bigqueryDatasetsConfig.bigquery_table_config[mpn].get('pg_id_downstream_update_table')

    if downstream_system == 'datalake':
        mapping_table_name = f'{table_name}_datalake'

    elif downstream_system == 'braze':
        mapping_table_name = f'{table_name}_braze'

    _base_datalake_mapping_table_query = f"""
    CREATE TABLE `{constants.PROJECT}.{constants.MIGRATION_DATASET}.{mapping_table_name}` AS 
    SELECT pg_ids as userId, user_ids as previousId, "alias" as type,
    "ToBeTriggered" AS status, "" AS response, 0 AS retry_count,
    CAST(NULL AS timestamp) AS triggered_date FROM
    `{constants.PROJECT}.{constants.MIGRATION_DATASET}.{table_name}`
    """
    try:
        logging.info(f"Creating another table for Datalake {mapping_table_name} from "
                     f"query: {_base_datalake_mapping_table_query}")
        client = bigquery.Client(constants.PROJECT)
        result = client.query(_base_datalake_mapping_table_query).to_dataframe()
        logging.info(f"Table for Datalake has been created, table name: {mapping_table_name}")
        return mapping_table_name

    except Exception as err:
        logging.exception(f"Unable To Create Datalake table exiting the flow, err: {err}")
        sys.exit(1)


def trigger_alias_call_for_datalake_or_braze(**kwargs):
    """ The Main method that reads data from bigquery and triggers alais calls to downstream. """
    table_name = kwargs['table_name']
    downstream_system = kwargs['downstream_system']
    mpn = kwargs['mpn']
    pg_id_vs_user_id_data = read_data_from_bigquery_from_mapping_table(table_name)

    if downstream_system == 'datalake':
        # todo: change key name to have single key name for multiple markets
        authentication_token = get_authentication_key_from_secret_manager(key=f'{mpn}_alias_Lytics_DL_writekey')
        asyncio.run(main(pg_id_vs_user_id_data, authentication_token, table_name))

    elif downstream_system == 'braze':
        authentication_token = get_authentication_key_from_secret_manager(key='brazil_braze_token')
        asyncio.run(main_braze(pg_id_vs_user_id_data, authentication_token, table_name))


def read_data_from_bigquery_from_mapping_table(table_name):
    query = f"""
    SELECT * FROM `{constants.PROJECT}.{constants.MIGRATION_DATASET}.{table_name}` WHERE status = "ToBeTriggered" 
    """
    try:
        client = bigquery.Client(project=constants.PROJECT)
        logging.info(f"Hitting Bigquery to read data,table name: {table_name}, query: {query}")
        query_job = client.query(query)
        query_results = list(query_job.result())
        if len(query_results) > 0:
            logging.info(f"Number of records fetched from Bigquery table: {len(query_results)}")
            return query_results
        else:
            logging.exception(f"No results while reading table {table_name}, Exiting the flow.")
            sys.exit(1)
    except Exception as err:
        logging.exception(f"Unable To Read data from table {table_name}, err: {err}")
        sys.exit(1)


async def main(bq_data_for_alias_calls, authentication_token, table_name):
    """ Method which will make async alais call for Datalake updates. """

    # 900 calls per seconds.
    semaphore = asyncio.Semaphore(value=900)  # todo: Check the limit
    # Create a co-routine of tasks based on the batch size
    tasks = [process_batch(list(records), semaphore, authentication_token, table_name) for records in
             [list(bq_data_for_alias_calls)[index:index + BQ_BATCH_SIZE]
              for index in range(0, len(bq_data_for_alias_calls), BQ_BATCH_SIZE)]]
    logging.info(f"The Total Batches created are: {len(tasks)}")
    await asyncio.gather(*tasks)


async def process_batch(records, rate_limit_semaphore, authentication_token, table_name):
    # Function to process a batch of BigQuery records make API calls in batches with rate limiting
    async with rate_limit_semaphore:
        response_list = []
        for record in records:
            api_data_payload = {"previousId": record["previousId"], "userId": record["userId"], "type": "alias"}
            response = await make_api_calls_with_retries(api_data_payload, rate_limit_semaphore, authentication_token)

            # for success change the status to success otherwise ToBeTriggered
            try:
                success_status = response['success']
                response_dict = {"previousId": record["previousId"], "userId": record["userId"], "type": "alias",
                                 "response": response, "retry_count": record["retry_count"] + 1,
                                 'status': "Success", 'triggered_date': datetime.datetime.utcnow()}
            except KeyError:
                response_dict = {"previousId": record["previousId"], "userId": record["userId"], "type": "alias",
                                 "response": response, "retry_count": record["retry_count"] + 1,
                                 'status': "ToBeTriggered", 'triggered_date': datetime.datetime.utcnow()}

            response_list.append(response_dict)
        update_response_status_in_bq(response_list, table_name)


async def make_api_calls_with_retries(data, semaphore, authentication_token):

    headers = {
        'Authorization': 'Bearer ' + f'{authentication_token}',
        'Content-Type': 'application/json'
    }
    segment_alias_url = "https://api.segment.io/v1/alias"
    retries = 3
    while retries > 0:
        try:
            async with aiohttp.ClientSession() as session:
                await semaphore.acquire()
                async with limiter:
                    async with session.post(segment_alias_url, data=json.dumps(data), headers=headers, verify=False) as response:
                        if response.status == 200:
                            content = await response.json()
                            semaphore.release()
                            return content
                        else:
                            logging.warning(f"API call failed. Status code: {response.status}")
                            retries -= 1
                            await asyncio.sleep(2 ** (3 - retries))  # Exponential backoff

        except RequestException as err:
            logging.error(f"Error making API call: {err}")
            retries -= 1
            await asyncio.sleep(2 ** (3 - retries))  # Exponential backoff
            logging.error("All retries exhausted for batch.")
            return False


def update_response_status_in_bq(api_responses, table_name):
    """ Update the status back in bigquery table to maitain the mapping. """
    batch_update_queries = []
    for record in api_responses:
        _base_update_query = f"""
        UPDATE `dbce-c360-isl-preprod-d942.cdp_migration.{table_name}`
        SET response = '{json.dumps(record['response'])}', retry_count = {record['retry_count']}, 
        status = '{record['status']}', triggered_date = timestamp('{record['triggered_date']}')
        where previousId = '{record['previousId']}'
        """
        batch_update_queries.append(_base_update_query)

    try:
        logging.info(f"Updating the Status in Bigquery")
        client = bigquery.Client()
        client.query(";".join(batch_update_queries)).result()

    except Exception as err:
        logging.exception(f"Unable to Update Status in Bigquery err: {err}")
        sys.exit(1)


# The modules used for Braze starts here.
async def main_braze(bq_data_for_alias_calls, authentication_token, table_name):
    """ Method which will make async alais call for Braze updates.
    For most APIs, Braze has a default rate limit of 250,000 requests per hour."""
    # 50 calls per seconds.
    semaphore = asyncio.Semaphore(value=50)  # todo: Check the limit,
    # Create a co-routine of tasks based on the batch size
    tasks = [process_batch_for_braze(list(records), semaphore, authentication_token, table_name) for records in
             [list(bq_data_for_alias_calls)[index:index + BQ_BATCH_SIZE]
              for index in range(0, len(bq_data_for_alias_calls), BQ_BATCH_SIZE)]]
    logging.info(f"The Total Batches created are: {len(tasks)}")
    await asyncio.gather(*tasks)


async def process_batch_for_braze(records, rate_limit_semaphore, authentication_token, table_name):
    # Function to process a batch of BigQuery records make API calls in batches with rate limiting for braze.
    async with rate_limit_semaphore:
        response_list, api_data_batches = [], []
        for record in records:
            api_data_batches.append({"current_external_id": record["previousId"], "new_external_id": record["userId"]})

        # Create a batch of 40 payloads in one nested list and trigger and API Call to braze.
        payload_chunks = [api_data_batches[start:start + BRAZE_API_CHUNK_SIZE] for start in
                          range(0, len(api_data_batches), BRAZE_API_CHUNK_SIZE)]
        input_data_chunks = [records[start:start + BRAZE_API_CHUNK_SIZE] for start in
                             range(0, len(records),BRAZE_API_CHUNK_SIZE)]

        for payload_chunk, record_chunk in zip(payload_chunks, input_data_chunks):
            braze_batch_payload = {"external_id_renames": payload_chunk}
            response = await make_api_calls_with_retries_for_braze(braze_batch_payload, rate_limit_semaphore,
                                                                   authentication_token)

            map_responses = await map_responses_to_payload_for_braze(response, record_chunk)
            response_list.append(map_responses)
        flatten_response_list = list(itertools.chain(*response_list))
        update_response_status_in_bq(flatten_response_list, table_name)


async def make_api_calls_with_retries_for_braze(data, semaphore, authentication_token):

    headers = {
        'Authorization': 'Basic ' + f'{authentication_token}',
        'Content-Type': 'application/json'
    }
    braze_url = "https://rest.fra-02.braze.eu/users/external_ids/rename"

    retries = 3
    while retries > 0:
        try:
            async with aiohttp.ClientSession() as session:
                await semaphore.acquire()
                async with limiter:
                    async with session.post(braze_url, data=json.dumps(data), headers=headers, verify=False) as response:
                        if response.status == 200:
                            content = await response.json()
                            semaphore.release()
                            return content
                        else:
                            logging.warning(f"API call failed. Status code: {response.status}")
                            retries -= 1
                            await asyncio.sleep(2 ** (3 - retries))  # Exponential backoff

        except RequestException as err:
            logging.error(f"Error making API call: {err}")
            retries -= 1
            await asyncio.sleep(2 ** (3 - retries))  # Exponential backoff
            logging.error("All retries exhausted for batch.")
            return False


async def map_responses_to_payload_for_braze(responses, records):
    """ Maps the failure record with respective payloads. """

    try:
        convert_to_dict = [dict(value) for value in records]
        response_list, index_tracking = [], []
        # Updates the payloads for failure records.
        check_for_errors = responses.get("rename_errors")
        if check_for_errors:
            for error in check_for_errors:
                index_pos, error_message = error
                index_tracking.append(index_pos)
                failure_record = convert_to_dict[index_pos]
                response_dict = {"previousId": failure_record["previousId"], "userId": failure_record["userId"],
                                 "type": "alias",
                                 "response": error_message, "retry_count": failure_record["retry_count"] + 1,
                                 'status': "ToBeTriggered", 'triggered_date': datetime.datetime.utcnow()}

                convert_to_dict[index_pos] = response_dict

        # Update the status for the success payloads
        for index, value in enumerate(convert_to_dict):
            if index not in index_tracking:
                convert_to_dict[index]['status'] = "Success"
                convert_to_dict[index]['retry_count'] = + 1
                convert_to_dict[index]['triggered_date'] = datetime.datetime.utcnow()

        return convert_to_dict

    except Exception as err:
        logging.exception(f"Unable to Parse Success/Failure for Braze, err: {err}")