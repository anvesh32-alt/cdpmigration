from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import logging
from datetime import datetime

DAGS_FOLDER = Variable.get("dags_folder")
SRC_PATH = '/cdp2.0-migration/src/utils'
final_path = DAGS_FOLDER + SRC_PATH

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0
}


def create_dynamic_dag(dag_id, job_config):
    logging.info(f"Starting the DAG with config: {job_config}")
    dag = models.DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=datetime(2023, 8, 22, 6, 31),
        catchup=False,
        schedule_interval=None,
        tags=['Opts Report'])

    with dag:
        start_process = DummyOperator(task_id="start_process")
        opts_report_task = BashOperator(
            task_id=f"opts_validation_report_for_{job_config['job_id']}",
            bash_command='python {{params.path}}/crsValidationReport.py {{ params.region }} {{params.market}} '
                         '{{params.mpn}}',
            params={'region': job_config['dataset_region'], 'market': job_config['market'],
                    'mpn': job_config['mpn'], 'path': final_path},
        )
        end_process = DummyOperator(task_id="end_task")
        start_process >> opts_report_task >> end_process

    return dag


job_configs = [
    {"job_id": "aus-always-discreet", "mpn": "473", "dataset_region": "dbce-c360-segamaws-prod-9669",
     "market": "personas_aus_always_discreet"}
]

for job_config in job_configs:
    dag_id = f"opts_reports_for_{job_config['job_id']}_dag"
    globals()[dag_id] = create_dynamic_dag(dag_id, job_config)
