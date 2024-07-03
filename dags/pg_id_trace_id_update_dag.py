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
        'num_workers': 8,
        'max_num_workers': 32,
        'subnetwork': DATAFLOW_SUBNETWORK,
        'network': DATAFLOW_NETWORK,
        'service_account_email': DATAFLOW_SERVICE_ACCOUNT,
        'save_main_session': True,
        'setup_file': DAGS_FOLDER + SRC_PATH + '/setup.py',
        'runner': 'DataflowRunner',
        'machine_type': 'n1-standard-2',
    }
}


def create_dynamic_dag(dag_id, job_config, pipeline_name):

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['PG Id Update Pipeline'])

    def log_configs(*args):
        logging.info(f"Starting the DAG with config dag_id: {dag_id}, job_config: {job_config}, pipeline_name: {pipeline_name}")

    with dag:
        start_process = PythonOperator(task_id="start_process", python_callable=log_configs)

        dataflow_task = DataflowCreatePythonJobOperator(
            task_id=f"{pipeline_name}-{job_config['job_id']}",
            py_file=DAGS_FOLDER + SRC_PATH + '/updatePgIdOrTraceidInPreprodSpanner.py',
            py_options=[],
            job_name='{{task.task_id}}',
            options={
                'mpn': job_config['mpn'],
                'pipeline_name': pipeline_name,
            },
            py_requirements=PY_REQUIREMENTS,
            py_interpreter='python3',
            py_system_site_packages=False,
            location=DATAFLOW_REGION
        )

        end_process = DummyOperator(task_id="end_task")

        start_process >> dataflow_task >> end_process

    return dag


# configs to be passed to the pipeline for each persona
job_configs = [
    {"job_id": "ita-braun", "mpn": "350"}  # ITA BRAUN
]

pipeline_names = ['pg_id_update', 'trace_id_update', 'profile_consents_update']

# build a dag for each config and each pipeline
for job_config in job_configs:
    for pipeline_name in pipeline_names:
        dag_id = f"{pipeline_name}_pipeline_{job_config['job_id']}"

        globals()[dag_id] = create_dynamic_dag(dag_id, job_config, pipeline_name)
