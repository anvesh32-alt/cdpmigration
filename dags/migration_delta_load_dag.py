import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
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

# path for source code
SRC_PATH = '/cdp2.0-migration/src'

# Variables to be used for copy/moving files between todo/processed buckets
SOURCE_BUCKET = 'dbce-c360-isl-preprod-d942-pii-datalake-segment-7cb2hqjg'
DESTINATION_BUCKET = 'dbce-c360-isl-preprod-d942-migration-88fakc29'
# uses format segment-logs/{segment-id}/'
SOURCE_OBJECT_PATH = 'segment-logs/{}/'
DESTINATION_OBJECT_PATH_TODO = 'migration_data/output-data/{}/todo'
DESTINATION_OBJECT_PATH_PROCESSED = 'migration_data/output-data/{}/processed'

# Input/output bucket to read/write data
# GCS_INPUT_BUCKET = "gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration/output-data/{}/todo/**/*.gz"
GCS_INPUT_BUCKET = "gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_data/output-data/{}/todo/**/*.gz"
GCS_OUTPUT_BUCKET = "gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_pipeline/{}/{}/retention-pipeline-output"
GCS_SPLITTER_OUTPUT_BUCKET = "gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_pipeline/{}/{}/splitter-pipeline-output"
GCS_EXTERNAL_OUTPUT_BUCKET = "gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_pipeline/{}/{}/externalid-pipeline-output"

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0,
    'dataflow_default_options': {
        'project': DATAFLOW_PROJECT,
        'region': DATAFLOW_REGION,
        'temp_location': GCS_TMP,
        'stagingLocation': GCS_STAGING,
        'worker_disk_type': 'compute.googleapis.com/projects/dbce-c360-isl-preprod-d942/zones/us-east4-b/diskTypes/pd-ssd',
        'num_workers': 30,
        'max_num_workers': 100,
        'subnetwork': DATAFLOW_SUBNETWORK,
        'network': DATAFLOW_NETWORK,
        'service_account_email': DATAFLOW_SERVICE_ACCOUNT,
        'save_main_session': True,
        'setup_file': DAGS_FOLDER + SRC_PATH + '/setup.py',
        'runner': 'DataflowRunner',
        'machine_type':'n1-standard-2',
    }
}


def create_dynamic_dag(dag_id, job_config):
    logging.info(f"Starting the DAG with config: {job_config}")

    SOURCE_OBJECT_PATH = f'segment-logs/{job_config["segment_id"]}/'
    DESTINATION_OBJECT_PATH_TODO = f'migration_data/output-data/{job_config["segment_id"]}/todo/'

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['Migration Delta Load Pipeline'])

    with dag:
        start_process = DummyOperator(task_id="start_process")

        move_files = GCSToGCSOperator(
            task_id=f"move_files_for_{job_config['job_id']}",
            source_bucket=SOURCE_BUCKET,
            source_object=SOURCE_OBJECT_PATH + "*",
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PATH_TODO,
            move_object=True,
        )

        dataflow_retention_task = DataflowCreatePythonJobOperator(
            task_id=f"retention_pipeline_delta_load-{job_config['job_id']}",
            py_file=DAGS_FOLDER + SRC_PATH + '/retentionOutUser.py',
            py_options=[],
            job_name='{{task.task_id}}',
            options={
                'input_bucket': GCS_INPUT_BUCKET.format(job_config['segment_id']),
                'output_bucket': GCS_OUTPUT_BUCKET.format(job_config['segment_id'], current_date),
                'mpn': job_config['mpn'],
                'pipeline_name': "migration_pipeline_delta_load",
            },
            py_requirements=PY_REQUIREMENTS,
            py_interpreter='python3',
            py_system_site_packages=False,
            location=DATAFLOW_REGION
        )
        dataflow_splitter_task = DataflowCreatePythonJobOperator(
            task_id=f"splitter_pipeline_delta_load-{job_config['job_id']}",
            py_file=DAGS_FOLDER + SRC_PATH + '/splitterPipeline.py',
            py_options=[],
            job_name='{{task.task_id}}',
            options={
                'input_bucket': GCS_OUTPUT_BUCKET.format(job_config['segment_id'], current_date),
                'output_bucket': GCS_SPLITTER_OUTPUT_BUCKET.format(job_config['segment_id'], current_date),
                'mpn': job_config['mpn'],
                'pipeline_name': "migration_pipeline_delta_load",
            },
            py_requirements=PY_REQUIREMENTS,
            py_interpreter='python3',
            py_system_site_packages=False,
            location=DATAFLOW_REGION
        )

        dataflow_external_id_task = DataflowCreatePythonJobOperator(
            task_id=f"external_id_pipeline_delta_load-{job_config['job_id']}",
            py_file=DAGS_FOLDER + SRC_PATH + '/externalIdPipeline.py',
            py_options=[],
            job_name='{{task.task_id}}',
            options={
                'input_bucket': GCS_SPLITTER_OUTPUT_BUCKET.format(job_config['segment_id'], current_date),
                'output_bucket': GCS_EXTERNAL_OUTPUT_BUCKET.format(job_config['segment_id'], current_date),
                'mpn': job_config['mpn'],
                'pipeline_name': "migration_pipeline_delta_load",
            },
            py_requirements=PY_REQUIREMENTS,
            py_interpreter='python3',
            py_system_site_packages=False,
            location=DATAFLOW_REGION
        )
        move_files_from_todo_to_processed = GCSToGCSOperator(
            task_id=f"move_files_todo_processed_{job_config['job_id']}",
            source_bucket=DESTINATION_BUCKET,
            source_object=DESTINATION_OBJECT_PATH_TODO.format(job_config['segment_id']) + "*",
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PATH_PROCESSED.format(job_config['segment_id']),
            move_object=True,
        )

        end_process = DummyOperator(task_id="end_task")

        start_process >> move_files >> dataflow_retention_task >> dataflow_splitter_task >> dataflow_external_id_task >> \
        move_files_from_todo_to_processed >> end_process

    return dag


# configs to be passed to the pipeline for each persona
job_configs = [
    {"job_id": "arb-ghh", "mpn": "363", "segment_id": "MCcgOexQ41"},#ARB growing families everyday me arabia
    {"job_id": "grc-ghh", "mpn": "292", "segment_id": "4254sfoIMZ"},  # grc-ghh
    {"job_id": "fra-braun", "mpn": "507", "segment_id": "ugQ5f9kznxji7SmxgYpMD"},  # fra-braun
    {"job_id": "tur-being-girl", "mpn": "209", "segment_id": "pH12TfD8UcF7hdxcavFSJy"},  # tur-being-girl
    {"job_id": "fra-oral-b", "mpn": "415", "segment_id": "s71J9VY2mG"},  # fra-oral-b
    {"job_id": "usa-oral-care", "mpn": "173", "segment_id": "eRs2nPePzcbf8UE5ydrpDU"}, #usa-oral-care
    {"job_id": "pol-ghh", "mpn": "249", "segment_id": "bJiXj7XStD"}, #pol-ghh
    {"job_id": "usa-braun", "mpn": "430", "segment_id": "pT4rLszF5D"}, #usa-braun
    {"job_id": "afr-pampers", "mpn": "380", "segment_id": "b2HLll4uJR"}, #afr-pampers
    {"job_id": "bgr-corporate", "mpn": "384", "segment_id": "zvLBAHe2xD"}, #bgr-corporate
    {"job_id": "twn-living-artist", "mpn": "59", "segment_id": "cutgFGK7j7UM5GcBKYNzXQ"}, #twn-living-artist
    {"job_id": "usa-always-discreet", "mpn": "494", "segment_id": "5fDrVQHdhdMU4NC83fR2e4"}, #usa-always-discreet
]

# build a dag for each config
for job_config in job_configs:
    dag_id = f"migration_delta_load_pipeline_{job_config['job_id']}"
    globals()[dag_id] = create_dynamic_dag(dag_id, job_config,)