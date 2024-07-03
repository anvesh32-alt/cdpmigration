from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
from datetime import datetime
from utils.pgIdDownstreamUpdateResci import create_pg_id_mapping_file_for_resci, upload_resci_file_to_bigquery, \
     sftp_file_download_to_gcs, upload_resci_file_to_sftp_server
from utils.customEmail import email_notification_for_resci


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
        tags=['Resci PG ID Update'])

    with dag:
        start_process = DummyOperator(task_id="start_process")

        download_file_from_sftp_to_gcs = PythonOperator(
            task_id=f"download_file_from_sftp_to_gcs_{job_config['job_id']}",
            python_callable=sftp_file_download_to_gcs,
            op_kwargs={'downstream_system': 'resci', 'mpn': job_config['mpn']}
        )

        upload_sftp_file_to_bq = PythonOperator(
            task_id=f"upload_sftp_file_in_bq_for_{job_config['job_id']}",
            python_callable=upload_resci_file_to_bigquery,
            op_kwargs={'file_path': "{{ task_instance.xcom_pull(task_ids=params.upstream_task_id) }}",
                       'persona': job_config['job_id']},
            params={'upstream_task_id': f"download_file_from_sftp_to_gcs_{job_config['job_id']}"}
        )

        create_tsv_file_for_resci = PythonOperator(
            task_id=f"create_tsv_file_for_resci_for_{job_config['job_id']}",
            python_callable=create_pg_id_mapping_file_for_resci,
            op_kwargs={'mpn': job_config['mpn'], 'country_code': job_config['country_code'],
                       'mpns_for_query': job_config['mpns_for_query'],
                       'table_id': "{{ task_instance.xcom_pull(task_ids=params.upstream_task_id) }}",
                       'file_path': "{{ task_instance.xcom_pull(task_ids=params.file_path_task_id) }}"},
            params={'upstream_task_id': f"upload_sftp_file_in_bq_for_{job_config['job_id']}",
                    'file_path_task_id': f"download_file_from_sftp_to_gcs_{job_config['job_id']}"}
        )

        sftp_files_to_resci = PythonOperator(
            task_id=f"sftp_files_to_resci_for_{job_config['job_id']}",
            python_callable=upload_resci_file_to_sftp_server,
            op_kwargs={'downstream_system': 'resci',
                       'file_path': "{{ task_instance.xcom_pull(task_ids=params.upstream_task_id) }}"},
            params={'upstream_task_id': f"create_tsv_file_for_resci_for_{job_config['job_id']}"}
        )

        trigger_auto_email = PythonOperator(
            task_id=f"trigger_auto_email_to_resci_for_{job_config['job_id']}",
            python_callable=email_notification_for_resci,
            op_kwargs={'job_id': job_config['job_id'],
                       'file_path': "{{ task_instance.xcom_pull(task_ids=params.upstream_task_id) }}"},
            params={'upstream_task_id': f"sftp_files_to_resci_for_{job_config['job_id']}"}
        )
        end_process = DummyOperator(task_id="end_task")
        start_process >> download_file_from_sftp_to_gcs >> upload_sftp_file_to_bq >> create_tsv_file_for_resci \
            >> sftp_files_to_resci >> trigger_auto_email >> end_process

    return dag


# Pass the mpn as tuple since markets can have multiple mpn
job_configs = [
    {"job_id": "jpn-ghh", "mpn": "2", "mpns_for_query": "(2)", "country_code": "JPN"}
]

for job_config in job_configs:
    dag_id = f"resci_pg_id_update_for_{job_config['job_id']}"
    globals()[dag_id] = create_dynamic_dag(dag_id, job_config)
