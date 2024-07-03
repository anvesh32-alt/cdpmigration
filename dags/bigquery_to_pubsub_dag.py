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


def create_dynamic_dag(dag_id, job_config, pipeline_name):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['BigQuery To Pubsub '])

    def log_configs(*args):
        logging.info(
            f"Starting the DAG with config dag_id: {dag_id}, job_config: {job_config}, pipeline_name: {pipeline_name}")

    with dag:
        start_process = PythonOperator(task_id="start_process", python_callable=log_configs)

        dataflow_task = DataflowCreatePythonJobOperator(
            task_id=f"bigquery_to_pubsub_{pipeline_name}-{job_config['job_id']}",
            py_file=DAGS_FOLDER + SRC_PATH + '/bigqueryToPubsub.py',
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

        end_process = DummyOperator(task_id="end_process")

        start_process >> dataflow_task >> end_process

    return dag


# configs to be passed to the pipeline for each persona
job_configs = [
    {"job_id": "usa-dental-care", "mpn": "401"}, # USA Dental Care
    {"job_id": "jpn-whisper","mpn":"16"}, # JPN Whisper
    {"job_id": "esp-beautycode","mpn":"543"}, # ESP Beautycode
    {"job_id": "che-growing-families", "mpn": "295"}, # CHE Growing Families
    {"job_id": "phl-corporate", "mpn": "24"}, # PHL Corporate
    {"job_id": "phl-pampers", "mpn": "44"}, # PHL Pampers
    {"job_id": "deu-growing-families", "mpn": "293"}, # DEU Growing Families
    {"job_id": "arb-ghh", "mpn": "363"},#ARB growing families everyday me arabia
	{"job_id": "irl-ghh", "mpn": "299"},#irl growing families
    {"job_id": "usa-old-spice", "mpn": "178"},#usa old spice
    {"job_id": "gbr-ghh", "mpn": "288"},# gbr growing families
    {"job_id": "ita-ghh", "mpn": "289"},# ita growing familes
    {"job_id": "gbr-oral-b", "mpn": "417"},  # gbr-oral-b
    {"job_id": "deu-oral-b", "mpn": "416"},  # deu-oral-b
    {"job_id": "esp-oral-b", "mpn": "412"},  # esp-oral-b
    {"job_id": "nld-oral-b", "mpn": "414"},  # nld-oral-b
    {"job_id": "bel-oral-b", "mpn": "524"},  # bel-oral-b
    {"job_id": "pol-oral-b", "mpn": "411"},  # pol-oral-b
    {"job_id": "swe-oral-b", "mpn": "410"},  # swe-oral-b
    {"job_id": "fin-oral-b", "mpn": "517"},  # fin-oral-b
    {"job_id": "vnm-corporate", "mpn": "39"}, #vnm corporate
    {"job_id": "ita-oral-b", "mpn": "413"},# ita-oral-b
    {"job_id": "dnk-oral-b", "mpn": "516"}, # dnk-oral-b
    {"job_id": "usa-olay", "mpn": "127"}, #usa-olay
    {"job_id": "fra-braun", "mpn": "507"},  # fra-braun
    {"job_id": "fra-jolly", "mpn": "472"},  # fra-jolly
    {"job_id": "esp-braun", "mpn": "518"},  # esp-braun
    {"job_id": "nld-gillette", "mpn": "424"},  # nld-gillette
    {"job_id": "tw-living-artist", "mpn": "59"},# tw-living-artist
    {"job_id": "ita-gillette", "mpn": "353"},#ita-gillette
    {"job_id": "esp-gillette", "mpn": "308"}, # esp-gillette
    {"job_id": "usa-gillette-v2", "mpn": "119"},# usa-gillette-v2
    {"job_id": "can-dental-care", "mpn": "405"}, #can-dental-care
    {"job_id": "grc-ghh", "mpn": "292"},  # grc-ghh
    {"job_id": "tur-being-girl", "mpn": "209"},  # tur-being-girl
    {"job_id": "fra-oral-b", "mpn": "415"},  # fra-oral-b
    {"job_id": "usa-oral-care", "mpn": "173"}, #usa-oral-care
    {"job_id": "pol-ghh", "mpn": "249"}, #pol-ghh
    {"job_id": "usa-braun", "mpn": "430"}, #usa-braun
    {"job_id": "afr-pampers", "mpn": "380"}, #afr-pampers
    {"job_id": "bgr-corporate", "mpn": "384"}, #bgr-corporate
    {"job_id": "usa-always-discreet", "mpn": "494"} #usa-always-discreet
]

pipeline_names = ['registration_data', 'clickstream_data', 'clt_data']

# build a dag for each config and each pipeline
for job_config in job_configs:
    for pipeline_name in pipeline_names:
        dag_id = f"bigquery_to_pubsub_{pipeline_name}_{job_config['job_id']}"

        globals()[dag_id] = create_dynamic_dag(dag_id, job_config, pipeline_name)
