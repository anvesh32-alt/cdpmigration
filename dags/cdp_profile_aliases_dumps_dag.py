from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
import pendulum
from utils.cdpProfileAliasesDump import cdp_profile_aliases_dumps


default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 2
}


def create_dynamic_dag(dag_id, job_config,env):

    logging.info(f"Starting the DAG with config: {job_config}")

    dag = models.DAG(
    dag_id = dag_id,
    default_args = default_args,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup = False,
    schedule_interval = "0 0 * * *",
    tags=['Audience Counts'] )

    with dag:

        start_process = DummyOperator(task_id = "start_process")

        cdp_profile_aliases_dumps_task = PythonOperator(
            task_id = f"cdp_profile_aliases_dump_{job_config['dataset_region']}_region",
            python_callable = cdp_profile_aliases_dumps,
            op_kwargs = {'dataset_region': job_config['dataset_region'], 'env': env, 'region': job_config['region']},
        )

        end_process = DummyOperator(task_id = "end_task")

        start_process >> cdp_profile_aliases_dumps_task >> end_process

    return dag


job_configs = [
    {"dataset_region" : "us", "region": "US"},
    {"dataset_region" : "us-east4", "region": "AMA"}
]
environment = ["preprod","prod"]


for job_config in job_configs:
    for env in environment:
        dag_id = f"cdp_profile_aliases_dump_{env}_{job_config['dataset_region']}_region"
        globals()[dag_id] = create_dynamic_dag(dag_id,
                                            job_config,env
                                            )