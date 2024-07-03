from airflow.models import Variable
from airflow import models
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import bigquery_operator
from datetime import datetime
from helpers import bigqueryDatasetsConfig,dqValidationQueries

ARGS = Variable.get('dq_validation', deserialize_json=True)
DAGS_FOLDER = Variable.get("dags_folder")
GCS_TMP = Variable.get("gcs_temp")
GCS_STAGING = Variable.get("gcs_staging")
DATAFLOW_PROJECT = Variable.get("dataflow_project")
DATAFLOW_REGION = Variable.get("dataflow_region")
DATAFLOW_SERVICE_ACCOUNT = Variable.get("dataflow_service_account")
DATAFLOW_NETWORK = Variable.get("dataflow_network")
DATAFLOW_SUBNETWORK = Variable.get("dataflow_subnetwork")
BQ_CONN_ID='bigquery_default'
SRC_PATH = '/cdp2.0-migration/src/utils'
final_path = DAGS_FOLDER + SRC_PATH

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0,
}

def create_dynamic_dag(dag_id, job_config):
    logging.info(f"Starting the DAG with config: {job_config}")
    MPN=job_config['mpn']
    destination_registration_table = bigqueryDatasetsConfig.bigquery_table_config.get(MPN).get(
        'destination_registration_table')
    destination_registration_table_v2 = bigqueryDatasetsConfig.bigquery_table_config.get(MPN).get(
        'destination_registration_table_v2')
    front_load_table_name = bigqueryDatasetsConfig.bigquery_table_config.get(MPN).get(
        'front_load_validation_failure_table')

    create_registration_data_table_query = '''CREATE OR REPLACE TABLE
      `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` ( message_id STRING,
        raw_payload STRING )
    CLUSTER BY
      message_id'''.format(destination_registration_table_v2)

    final_insert_query = '''
    insert into `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
    select message_id,raw_payload from
    `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` a
    where message_id not in  ( select  distinct message_id from
    `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
    )'''.format(destination_registration_table_v2, destination_registration_table, front_load_table_name)

    failure_table_name = bigqueryDatasetsConfig.bigquery_table_config.get(MPN).get(
        'front_load_validation_failure_table')
    front_load_table_name = bigqueryDatasetsConfig.bigquery_table_config.get(MPN).get('destination_registration_table')
    # Validate if MPN and country combination exists in CRS
    vld2_query = dqValidationQueries.vld2_query.format(failure_table_name, front_load_table_name, front_load_table_name,
                                                       MPN)
    # Validate if MPN is Active from CRS
    vld3_query = dqValidationQueries.vld3_query.format(failure_table_name, front_load_table_name, MPN)
    # Loyalty account number can be alphanumeric and opt Ip address is optionalfield
    vld4_query = dqValidationQueries.vld4_query.format(failure_table_name, MPN, front_load_table_name)
    # Either of optId, opt number or opt service name is required field
    vld5_query = dqValidationQueries.vld5_query.format(failure_table_name, MPN, front_load_table_name)
    # The Universal request with MPN 9999 should have legalEntity trait
    vld6_query = dqValidationQueries.vld6_query.format(failure_table_name, MPN, front_load_table_name)

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['DLQ Validation'])

    with (dag):

        start_process = DummyOperator(task_id="start_process")

        create_table = bigquery_operator.BigQueryOperator(
            task_id='create_registration_data_table',
            sql=create_registration_data_table_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        final_insert = bigquery_operator.BigQueryOperator(
            task_id='final_insert_table',
            sql=final_insert_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        vld2_validation = bigquery_operator.BigQueryOperator(
            task_id='vld2_validation',
            sql=vld2_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        vld3_validation = bigquery_operator.BigQueryOperator(
            task_id='vld3_validation',
            sql=vld3_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        # vld4_validation = bigquery_operator.BigQueryOperator(
        #     task_id='vld4_validation',
        #     sql=vld4_query,
        #     use_legacy_sql=False,
        #     allow_large_results=True,
        #     gcp_conn_id=BQ_CONN_ID
        # )

        vld5_validation = bigquery_operator.BigQueryOperator(
            task_id='vld5_validation',
            sql=vld5_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        vld6_validation = bigquery_operator.BigQueryOperator(
            task_id='vld6_validation',
            sql=vld6_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        end_process = DummyOperator(task_id="end_process")

        start_process >> create_table >> vld2_validation >> vld3_validation >> vld5_validation >> \
        vld6_validation >> final_insert >> end_process
    return dag

# MPN to be passed
job_configs = [
    {"job_id": "bra-mult", "mpn": "147", },  # Brazil
    {"job_id": "gbr-beauty", "mpn": "550"},  # GBR Beauty Code
    {"job_id": "grc-ghh", "mpn": "292"},  # grc-ghh
    {"job_id": "fra-braun", "mpn": "507"},  # fra-braun
    {"job_id": "tur-being-girl", "mpn": "209"},  # tur-being-girl
    {"job_id": "fra-oral-b", "mpn": "415"},  # fra-oral-b
    {"job_id": "usa-oral-care", "mpn": "173"}, #usa-oral-care
    {"job_id": "pol-ghh", "mpn": "249"}, #pol-ghh
    {"job_id": "usa-braun", "mpn": "430"}, #usa-braun
    {"job_id": "afr-pampers", "mpn": "380"}, #afr-pampers
    {"job_id": "bgr-corporate", "mpn": "384"}, #bgr-corporate
    {"job_id": "twn-living-artist", "mpn": "59"}, #twn-living-artist
    {"job_id": "usa-always-discreet", "mpn": "494"}, #usa-always-discreet
    {"job_id": "can-dental-care", "mpn": "405"},# can-dental-care
    {"job_id": "usa-gillete", "mpn": "119"}# usa-gillete
]

# build a dag for each config
for job_config in job_configs:
    dag_id = f"dlq_validation_{job_config['job_id']}"

    globals()[dag_id] = create_dynamic_dag(dag_id, job_config,)