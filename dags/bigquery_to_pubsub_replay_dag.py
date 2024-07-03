from airflow import DAG
from airflow.models import Variable
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import bigquery_operator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.operators.python_operator import PythonOperator

import logging
from datetime import datetime

DAGS_FOLDER = Variable.get("dags_folder")
GCS_TMP = Variable.get("gcs_temp")
GCS_STAGING = Variable.get("gcs_staging")
DATAFLOW_PROJECT = Variable.get("dataflow_project")
DATAFLOW_REGION = Variable.get("dataflow_region")
DATAFLOW_SERVICE_ACCOUNT = Variable.get("dataflow_service_account")
DATAFLOW_NETWORK = Variable.get("dataflow_network")
DATAFLOW_SUBNETWORK = Variable.get("dataflow_subnetwork")
BQ_CONN_ID='bigquery_default'
PY_REQUIREMENTS = Variable.get("py_requirements", deserialize_json=True)['py_requirements']

# path for source code
SRC_PATH = '/cdp2.0-migration/src'

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0,
    'dataflow_default_options': {
        'project': DATAFLOW_PROJECT,
        'region': DATAFLOW_REGION,
        'temp_location': GCS_TMP,
        'stagingLocation': GCS_STAGING,
        'num_workers': 30,
        'max_num_workers': 100,
        'subnetwork': DATAFLOW_SUBNETWORK,
        'network': DATAFLOW_NETWORK,
        'service_account_email': DATAFLOW_SERVICE_ACCOUNT,
        'save_main_session': True,
        'setup_file': DAGS_FOLDER + SRC_PATH + '/setup.py',
        'runner': 'DataflowRunner',
    }
}

sqlQuery = '''MERGE
  `dbce-c360-isl-preprod-d942.Migration_DLQ.dlq_auto_replay_table` b
USING
  `dbce-c360-isl-preprod-d942.Migration_DLQ.dlq_auto_replay_status_view` a
ON
  ( a.message_id = b.message_id
    AND a.trace_id = b.trace_id
    AND b.replay_status ='ToBeReplayed')
  WHEN MATCHED
  THEN
UPDATE
SET
  b.replay_status= a.replay_status,
  b.replay_time=a.replay_time'''

with models.DAG(
        dag_id="bigquery_to_pubsub_replay",
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2024, 1, 29),
        catchup=False,
        schedule_interval="00 01,10,17 * * *",
        tags=['DLQ Common Error Replay']
) as dag_native_python:

        start_process = DummyOperator(task_id="start_process")

        dataflow_task = DataflowCreatePythonJobOperator(
            task_id="dlq_common_errors_replay",
            py_file=DAGS_FOLDER + SRC_PATH + '/bigqueryToPubsubReplay.py',
            py_options=[],
            job_name='{{task.task_id}}',
            options={
            },
            py_requirements=PY_REQUIREMENTS,
            py_interpreter='python3',
            py_system_site_packages=False,
            location=DATAFLOW_REGION
        )

        update_data = bigquery_operator.BigQueryOperator(
            task_id='update_data',
            sql=sqlQuery,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        end_process = DummyOperator(task_id="end_process")

        start_process >> dataflow_task >> update_data >> end_process
