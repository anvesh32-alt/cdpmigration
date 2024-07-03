from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
from datetime import datetime

from utils.pgIdDownstreamUpdate import (create_mapping_table_for_segment_vs_pg_id,
                                        create_segment_vs_pg_id_file_for_lytics, pg_id_update_in_downstream)
from utils.apiCallsToLytics import submit_export_job_in_lytics

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0
}


def create_dynamic_dag(dag_id, job_config):

    dag = models.DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=datetime(2023, 8, 22, 6, 31),
        catchup=False,
        schedule_interval=None,
        tags=['Lytics PG ID Update'])

    def log_configs(*args):
        logging.info(f"Starting the DAG with config dag_id: {dag_id}, job_config: {job_config}")

    with dag:
        start_process = PythonOperator(task_id="start_process", python_callable=log_configs)

        get_or_create_pg_id_mapping_table = PythonOperator(
            task_id=f"get_or_create_pgid_mapping_table_lytics_for_{job_config['job_id']}",
            python_callable=create_mapping_table_for_segment_vs_pg_id,
            op_kwargs={'mpns_for_query': job_config['mpns_for_query'], 'mpn': job_config['mpn']}
        )

        create_alias_file_for_pgid_updates = PythonOperator(
            task_id=f"create_alias_file_for_lytics_for_{job_config['job_id']}",
            python_callable=create_segment_vs_pg_id_file_for_lytics,
            op_kwargs={'country_code': job_config['country_code'], 'mpn': job_config['mpn']}
        )

        sftp_files_to_lytics = PythonOperator(
            task_id=f"sftp_files_to_lytics_for_{job_config['job_id']}",
            python_callable=pg_id_update_in_downstream,
            op_kwargs={'downstream_system': 'lytics', 'mpn': job_config['mpn'],
                       'file_path': "{{ task_instance.xcom_pull(task_ids=params.upstream_task_id) }}"},
            params={'upstream_task_id': f"create_alias_file_for_lytics_for_{job_config['job_id']}"}
        )

        create_export_job_for_lytics = PythonOperator(
            task_id=f"submit_export_job_for_lytics_for_{job_config['job_id']}",
            python_callable=submit_export_job_in_lytics,
            op_kwargs={'downstream_system': "lytics",
                       'aid_and_file_name': "{{ task_instance.xcom_pull(task_ids=params.upstream_task_id) }}"},
            params={'upstream_task_id': f"sftp_files_to_lytics_for_{job_config['job_id']}"}
        )

        end_process = DummyOperator(task_id="end_task")

        start_process >> get_or_create_pg_id_mapping_table >> create_alias_file_for_pgid_updates >> sftp_files_to_lytics \
            >> create_export_job_for_lytics >> end_process

    return dag


# Pass the mpns_for_query as tuple since markets can have multiple mpn, this parameter is used to fire a bq query to
# get user_id vs pg_id mapping.
job_configs = [
    {"job_id": "jpn-ghh", "mpn": "2", "mpns_for_query": "(2)", "country_code": "JPN"}
]

for job_config in job_configs:
    dag_id = f"lytics_pg_id_update_for_{job_config['job_id']}"
    globals()[dag_id] = create_dynamic_dag(dag_id, job_config)
