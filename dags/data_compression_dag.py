import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators import bigquery_operator
from datetime import datetime
from helpers import compressionQueries

BQ_CONN_ID = 'bigquery_default'

current_date = datetime.now().strftime("%m%d")

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0,
}


def create_dynamic_dag(dag_id, job_config):
    logging.info(f"Starting the DAG with config: {job_config}")
    mpn = job_config['mpn']
    country_code = job_config['country_code']
    legal_entity = job_config['legal_entity']
    mpn_for_where_clause = ','.join(map("'{0}'".format, job_config['mpn_for_where_clause']))

    registration_input_table = job_config['registration_input_dataset']
    intermediate_traits_output_table = f'compressed_{mpn}_{registration_input_table}_intermediate_traits_{current_date}'
    intermediate_1_traits_output_table = f'compressed_{mpn}_{registration_input_table}_intermediate_1_traits_{current_date}'
    ids_output_table = f'compressed_{mpn}_{registration_input_table}_ids_{current_date}'
    traits_output_table = f'compressed_{mpn}_{registration_input_table}_traits_{current_date}'
    consents_output_table = f'compressed_{mpn}_{registration_input_table}_consents_{current_date}'
    final_table = f'compressed_{mpn}_{registration_input_table}_final_{current_date}'

    # Create intermediate traits table
    intermediate_traits = compressionQueries.intermediate_traits.format(intermediate_traits_output_table,
                                                                        registration_input_table, mpn,
                                                                        mpn_for_where_clause)
    # Create intermediate 1 traits table
    intermediate_1_traits = compressionQueries.intermediate_1_traits.format(intermediate_1_traits_output_table,
                                                                            intermediate_traits_output_table)
    # Create ids table
    ids_query = compressionQueries.ids_query.format(ids_output_table, mpn, intermediate_traits_output_table)
    # Create traits table
    traits_query = compressionQueries.traits_query.format(traits_output_table, intermediate_1_traits_output_table,
                                                          ids_output_table, int(mpn))
    # Create consents table
    consents_query = compressionQueries.consents_query.format(consents_output_table, country_code, legal_entity,
                                                              registration_input_table)
    # Create final table
    final_table_query = compressionQueries.final_table_query.format(final_table, registration_input_table,
                                                                    traits_output_table, consents_output_table)

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['Data Compression'])

    with dag:
        start_process = DummyOperator(task_id="start_process")

        _intermediate_traits = bigquery_operator.BigQueryOperator(
            task_id='create_intermediate_traits',
            sql=intermediate_traits,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        _intermediate_1_traits = bigquery_operator.BigQueryOperator(
            task_id='create_intermediate_1_traits',
            sql=intermediate_1_traits,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        _ids_table = bigquery_operator.BigQueryOperator(
            task_id='create_ids_table',
            sql=ids_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        _traits_table = bigquery_operator.BigQueryOperator(
            task_id='create_traits_table',
            sql=traits_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        _consents_table = bigquery_operator.BigQueryOperator(
            task_id='create_consents_table',
            sql=consents_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        _final_table = bigquery_operator.BigQueryOperator(
            task_id='create_final_table',
            sql=final_table_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        end_process = DummyOperator(task_id="end_process")

        start_process >> _intermediate_traits >> _intermediate_1_traits >> _ids_table >> _traits_table >> end_process
        start_process >> _consents_table >> end_process
        end_process >> _final_table

    return dag


# job configs should be passed in this format, pls follow the same datatypes.
job_configs = [
    {"job_id": "fra-braun", "mpn": "507", "mpn_for_where_clause": ["507", "123"], "country_code": "FRA",
     "legal_entity": 23, "registration_input_dataset": "fra_braun_registration_data"},
]

# build a dag for each config
for job_config in job_configs:
    dag_id = f"data_compression_{job_config['job_id']}"
    globals()[dag_id] = create_dynamic_dag(dag_id, job_config, )
