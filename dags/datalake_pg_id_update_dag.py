from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
from datetime import datetime
from airflow.contrib.operators import bigquery_operator
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from utils.pgIdDownstreamUpdate import create_mapping_table_for_segment_vs_pg_id
from utils.pgIdUpdateDatalakeAndBraze import create_mapping_table_for_segment_vs_pg_id_for_datalake_or_braze, \
    trigger_alias_call_for_datalake_or_braze
from helpers.bigqueryDatasetsConfig import bigquery_table_config

BQ_CONN_ID = 'bigquery_default'

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0
}


def create_dynamic_dag(dag_id, job_config):
    logging.info(f"Starting the DAG with config: {job_config}")
    table_name = bigquery_table_config[job_config['mpn']].get('pg_id_downstream_update_table')
    datalake_table_name = f'{table_name}_datalake'
    sql_query_to_update_status = f"""
    UPDATE `dbce-c360-isl-preprod-d942.cdp_migration.{datalake_table_name}`
    SET status = 'ToBeAnalysed' WHERE retry_count >= 3
    """

    dag = models.DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=datetime(2023, 8, 22, 6, 31),
        catchup=False,
        schedule_interval=None,
        tags=['Datalake PG ID Update'])

    with dag:
        start_process = DummyOperator(task_id="start_process")

        get_or_create_pg_id_mapping_table = PythonOperator(
            task_id=f"get_or_create_pgid_mapping_table_for_{job_config['job_id']}",
            python_callable=create_mapping_table_for_segment_vs_pg_id,
            op_kwargs={'mpns_for_query': job_config['mpns_for_query'], 'mpn': job_config['mpn']}
        )

        create_mapping_table_for_pgid_updates = PythonOperator(
            task_id=f"create_mapping_table_for_datalake_for_{job_config['job_id']}",
            python_callable=create_mapping_table_for_segment_vs_pg_id_for_datalake_or_braze,
            op_kwargs={'mpn': job_config['mpn'], 'downstream_system': 'datalake'}
        )

        trigger_alias_call = PythonOperator(
            task_id=f"trigger_alias_call_for_{job_config['job_id']}",
            python_callable=trigger_alias_call_for_datalake_or_braze,
            op_kwargs={'downstream_system': 'datalake',
                       'table_name': "{{ task_instance.xcom_pull(task_ids=params.upstream_task_id) }}"},
            params={'upstream_task_id': f"create_mapping_table_for_datalake_for_{job_config['job_id']}"}
        )

        update_status_for_retry_counts = bigquery_operator.BigQueryOperator(
            task_id=f"update_status_for_retry_counts_for_{job_config['job_id']}",
            sql=sql_query_to_update_status, use_legacy_sql=False, allow_large_results=True, gcp_conn_id=BQ_CONN_ID
        )

        end_process = DummyOperator(task_id="end_task")
        start_process >> get_or_create_pg_id_mapping_table >> create_mapping_table_for_pgid_updates \
        >> trigger_alias_call >> update_status_for_retry_counts >> end_process

    return dag


# Pass the mpns_for_query as tuple since markets can have multiple mpn, this parameter is used to fire a bq query to
# get user_id vs pg_id mapping.
job_configs = [
    {"job_id": "jpn-ghh", "mpn": "2", "mpns_for_query": "(2)", "country_code": "JPN"},
    {"job_id": "jpn-ghh", "mpn": "293", "mpns_for_query": "(293)", "country_code": "DEU"}
]

for job_config in job_configs:
    dag_id = f"datalake_pg_id_update_for_{job_config['job_id']}"
    globals()[dag_id] = create_dynamic_dag(dag_id, job_config)
