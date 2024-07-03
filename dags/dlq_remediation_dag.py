"""
Kindly update timestamp config file before running the job:
File name : dlq_remediation_timestamp.json
File path: us-east4-isl-composer-2-7238166d-bucket/dags/dlq_timestamp_config
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
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
PY_REQUIREMENTS = Variable.get("py_requirements", deserialize_json=True)['py_requirements']

current_date = datetime.now().strftime("%d-%m-%Y")

# pipeline variables
OUTPUT_BUCKET = f'gs://dbce-c360-isl-preprod-d942-migration-88fakc29/dlq_replay/{current_date}'

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
        'num_workers': 10,
        'max_num_workers': 30,
        'subnetwork': DATAFLOW_SUBNETWORK,
        'network': DATAFLOW_NETWORK,
        'service_account_email': DATAFLOW_SERVICE_ACCOUNT,
        'save_main_session': True,
        'setup_file': DAGS_FOLDER + SRC_PATH + '/setup.py',
        'runner': 'DataflowRunner',
    }
}


def create_dynamic_dag(dag_id, job_config):

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['DLQ Remediation Pipeline'])

    def log_configs(*args):
        logging.info(f"Starting the DAG with config dag_id: {dag_id}, job_config: {job_config}")

    with dag:
        start_process = PythonOperator(task_id="start_process", python_callable=log_configs)

        dataflow_task = DataflowCreatePythonJobOperator(
            task_id=f"dql_to_pubsub-{job_config['job_id']}",
            py_file=DAGS_FOLDER + SRC_PATH + '/dlqRemediation.py',
            py_options=[],
            job_name='{{task.task_id}}',
            options={
                'pipeline_name': job_config['pipeline_name'],
                'output_bucket': OUTPUT_BUCKET
            },
            py_requirements=PY_REQUIREMENTS,
            py_interpreter='python3',
            py_system_site_packages=False,
            location=DATAFLOW_REGION
        )

        end_process = DummyOperator(task_id="end_process")

        start_process >> dataflow_task >> end_process

    return dag


# configs to be passed to the pipeline for each persona
job_configs = [
    {"job_id": "registration-dlq", "pipeline_name": "registration_dlq"},
    {"job_id": "registration-pillar-dlq", "pipeline_name": "registration_pillar_dlq"},
    {"job_id": "clickstream-dlq", "pipeline_name": "clickstream_dlq"},
    {"job_id": "clt-dlq", "pipeline_name": "clt_dlq"},
    {"job_id": "other-events-dlq", "pipeline_name": "other_events_dlq"},
]

# build a dag for each config
for job_config in job_configs:
    dag_id = f"dlq_to_pubsub_{job_config['job_id']}"

    globals()[dag_id] = create_dynamic_dag(dag_id, job_config,)
