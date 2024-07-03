import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from helpers import ctasComparisonQueries
from google.cloud import bigquery


current_date = datetime.now().strftime("%m%d")
BQ_CONN_ID = 'bigquery_default'
GCP_PROJECT = "dbce-c360-isl-preprod-d942"
connection_type = {"dbce-c360-segamaws-prod-9669":"us-east4", "dbce-c360-segglblws-prod-b902" : "us", "dbce-c360-segamerws-prod-35ac" : "us"}
dataset_per_region = {"dbce-c360-segamaws-prod-9669":"seg_cdp_trait_comparsion_useast4", "dbce-c360-segglblws-prod-b902" : "seg_cdp_trait_comparsion_us", "dbce-c360-segamerws-prod-35ac" : "seg_cdp_trait_comparsion_us"}
environment = {"pre-prod":"-pre-prod","prod":""}
cdp_view_table = {"pre-prod":"_pre_prod","prod":""}

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0
}


def call_ctas_traits(**kwargs):

    client = bigquery.Client(GCP_PROJECT)
    region = kwargs['region']
    personas_table = kwargs['personas_table']
    table_name = kwargs['job_id']

    query_get_columns_ctas = f'''
    select case when data_type = 'STRING' then column_name 
    else concat('cast (',column_name,' ', ' as string ) ',column_name)
    end as list_main
    from `{region}`.{personas_table}.INFORMATION_SCHEMA.COLUMNS
    where table_name = 'canonical_user_ctas'
    and column_name not like 'context_%'
    and column_name <> 'user_id' and column_name not like 'score_%'
    order by column_name
    '''

    query_job = client.query(query_get_columns_ctas).to_dataframe()
    get_columns_ctas = ', '.join(map("{0}".format, query_job['list_main']))

    query_get_columns_ctas_unpivot = f'''
    SELECT column_name as list_main
    FROM `{region}`.{personas_table}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = 'canonical_user_ctas'
    AND column_name NOT LIKE 'context_%'
    AND column_name <> 'user_id'
    AND column_name NOT LIKE 'score_%'
    ORDER BY column_name
    '''

    query_job = client.query(query_get_columns_ctas_unpivot).to_dataframe()
    get_columns_ctas_unpivot = ', '.join(map("{0}".format, query_job['list_main']))

    ctas_traits = ctasComparisonQueries.ctas_traits.format(dataset_per_region[region],f"{table_name}_ctas_all_traits_{current_date}",get_columns_ctas,region,personas_table,get_columns_ctas_unpivot)
    logging.info("ctas_traits",ctas_traits)
    query_job = client.query(ctas_traits).to_dataframe()


def call_segment_traits(**kwargs):

    client = bigquery.Client(GCP_PROJECT)
    region = kwargs['region']
    table_name = kwargs['job_id']
    env = kwargs['env']
    mpn = ','.join(map("{0}".format, kwargs['mpn_for_where_clause']))

    segment_traits = ctasComparisonQueries.segment_traits.format(dataset_per_region[region],f"{table_name}_segment_traits_pg_id_{current_date}",region,cdp_view_table[env],mpn,
                                                                 dataset_per_region[region],f"{table_name}_segment_idgraph_pg_id_vs_user_id_{current_date}",region,connection_type[region],environment[env],mpn,
                                                                 dataset_per_region[region],f"{table_name}_segment_idgraph_one_pg_id_vs_user_id_{current_date}",
                                                                 dataset_per_region[region],f"{table_name}_segment_idgraph_pg_id_vs_user_id_{current_date}")
    logging.info("segment_traits",segment_traits)
    query_job = client.query(segment_traits).to_dataframe()


def call_ecosystem_trait(**kwargs):

    client = bigquery.Client(GCP_PROJECT)
    region = kwargs['region']
    table_name = kwargs['job_id']
    mpn = ','.join(map("{0}".format, kwargs['mpn_for_where_clause']))

    ecosystem_trait = ctasComparisonQueries.ecosystem_trait.format(dataset_per_region[region],f"{table_name}_ecosystem_trait_mapp_{current_date}",region,region,mpn)
    logging.info("ecosystem_trait",ecosystem_trait)
    query_job = client.query(ecosystem_trait).to_dataframe()


def call_seg_vs_cdp_trait(**kwargs):

    client = bigquery.Client(GCP_PROJECT)
    table_name = kwargs['job_id']
    region = kwargs['region']
    seg_vs_cdp_trait = ctasComparisonQueries.seg_vs_cdp_trait.format(dataset_per_region[region],f"{table_name}_seg_vs_cdp_trait_values_{current_date}",
                                                                     dataset_per_region[region],f"{table_name}_ctas_all_traits_{current_date}",
                                                                     dataset_per_region[region],f"{table_name}_segment_idgraph_pg_id_vs_user_id_{current_date}",
                                                                     dataset_per_region[region],f"{table_name}_segment_traits_pg_id_{current_date}",
                                                                     dataset_per_region[region],f"{table_name}_seg_vs_cdp_trait_values_{current_date}_results",
                                                                     dataset_per_region[region],f"{table_name}_seg_vs_cdp_trait_values_{current_date}")
    logging.info("seg_vs_cdp_trait",seg_vs_cdp_trait)
    query_job = client.query(seg_vs_cdp_trait).to_dataframe()


def create_dynamic_dag(dag_id, job_config):
    logging.info(f"Starting the DAG with config: {job_config}")

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['CTAS CDP Comparison'])

    with (dag):
        start_process = DummyOperator(task_id="start_process")

        _ctas_traits = PythonOperator(
            task_id = "create_ctas_traits",
            python_callable = call_ctas_traits,
            op_kwargs = job_config,
            dag = dag
        )

        _segment_traits = PythonOperator(
            task_id = "call_segment_traits",
            python_callable = call_segment_traits,
            op_kwargs = job_config,
            dag = dag
        )

        _ecosystem_trait = PythonOperator(
            task_id='create_ecosystem_trait',
            python_callable = call_ecosystem_trait,
            op_kwargs = job_config,
            dag = dag
        )

        _seg_vs_cdp_trait = PythonOperator(
            task_id='create_seg_vs_cdp_trait',
            python_callable = call_seg_vs_cdp_trait,
            op_kwargs = job_config,
            dag = dag
        )

        end_process = DummyOperator(task_id="end_process")

        start_process  >> _ctas_traits >> _segment_traits >> _ecosystem_trait >> _seg_vs_cdp_trait >> end_process

    return dag


# job configs should be passed in this format, pls follow the same datatypes.
job_configs = [
    {"job_id": "fra_braun", "mpn_for_where_clause": ["507","357"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_fra_braun","env":"prod"},
    {"job_id": "phl_corporate", "mpn_for_where_clause": ["24","256"],
     "region":"dbce-c360-segamaws-prod-9669","personas_table":"personas_phl_corporate","env":"prod"},
    {"job_id": "phl_pampers", "mpn_for_where_clause": ["44"],
     "region":"dbce-c360-segamaws-prod-9669","personas_table":"personas_phl_pampers","env":"prod"},
     {"job_id": "bgr_corporate", "mpn_for_where_clause": ["384"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_bgr_corporate","env":"pre-prod"},
     {"job_id": "che_ghh", "mpn_for_where_clause": ["295"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_che_growing_families","env":"prod"},
     {"job_id": "ita_braun", "mpn_for_where_clause": ["350"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_ita_braun","env":"prod"},
     {"job_id": "jpn-ghh", "mpn_for_where_clause": ["2"],
     "region":"dbce-c360-segamaws-prod-9669","personas_table":"personas_jpn_growing_families_myrepi","env":"prod"},
     {"job_id": "gbr_ghh", "mpn_for_where_clause": ["288","490"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_gbr_growing_families","env":"pre-prod"},
    {"job_id": "can-dental-care", "mpn_for_where_clause": ["405"],
     "region":"dbce-c360-segamerws-prod-35ac","personas_table":"personas_can_dental_care","env":"prod"},
    {"job_id": "usa-dental-care", "mpn_for_where_clause": ["401"],
     "region":"dbce-c360-segamerws-prod-35ac","personas_table":"personas_usa_dental_care","env":"prod"},
    {"job_id": "gbr_ghh_prod", "mpn_for_where_clause": ["288","490"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_gbr_growing_families","env":"prod"},
    {"job_id": "ita_oralb_prod", "mpn_for_where_clause": ["413"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_ita_oral_b","env":"prod"},
     {"job_id": "can-pgge", "mpn_for_where_clause": ["138"],
     "region":"dbce-c360-segamerws-prod-35ac","personas_table":"personas_can_growing_families_pgge","env":"pre-prod"},
     {"job_id": "deu_ghh_preprod", "mpn_for_where_clause": ["293"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_deu_growing_families","env":"pre-prod"},
     {"job_id": "swe_oralb_prod", "mpn_for_where_clause": ["410"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_swe_oral_b","env":"prod"},
     {"job_id": "fin_oralb_prod", "mpn_for_where_clause": ["517"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_fin_oral_b","env":"prod"},
     {"job_id": "pol_oralb_prod", "mpn_for_where_clause": ["411"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_pol_oral_b","env":"prod"},
     {"job_id": "fra_oralb_preprod", "mpn_for_where_clause": ["415"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_fra_oralb","env":"pre-prod"},
     {"job_id": "gbr_oralb_preprod", "mpn_for_where_clause": ["417"],
     "region":"dbce-c360-segglblws-prod-b902","personas_table":"personas_gbr_oral_b","env":"pre-prod"},
     {"job_id": "arb_ghh_preprod", "mpn_for_where_clause": ["363"],
     "region":"dbce-c360-segamaws-prod-9669","personas_table":"personas_arb_growing_families_everyday_me_arabia","env":"pre-prod"}
]


# build a dag for each config
for job_config in job_configs:
    dag_id = f"ctas_to_cdp_comparsion_{job_config['job_id']}"
    globals()[dag_id] = create_dynamic_dag(dag_id, job_config, )