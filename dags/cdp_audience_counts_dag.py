from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.contrib.operators import bigquery_operator
import logging
import pendulum
from google.cloud import bigquery,storage
import json
# from helpers import constants
from utils.cdpAudienceCounts import cdp_audience_counts


BQ_CONN_ID = 'bigquery_default'
default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 2
}
ISL_PROD_PROJECT = 'dbce-c360-isl-prod-8859'
DAG_BUCKET = 'us-east4-isl-composer-2-7238166d-bucket'
PROJECT_TABLES = {"AMA":"dbce-c360-segamaws-prod-9669", "EU":"dbce-c360-segglblws-prod-b902", "EU_corp":"dbce-c360-segglblws-prod-b902","US":"dbce-c360-segamerws-prod-35ac"}
CONNECTION_TYPE = {"preprod":"-pre-prod","preprodlive":"-pre-prod","prod":""}
RETENTION_DATASET = {"AMA":"CDP_Retention_AMA", "EU":"CDP_Retention", "EU_corp":"CDP_Retention", "US":"CDP_Retention"}
AUDIENCE_DUMPS_TABLE = {"preprod" : "preprod", "preprodlive" : "preprod", "prod" : "prod"}


def spanner_tables_dumps_creation(job_config):

    base_mpn = job_config['mpn']
    client = bigquery.Client(ISL_PROD_PROJECT)
    client_storage = storage.Client(ISL_PROD_PROJECT)
    bucket = client_storage.get_bucket(DAG_BUCKET)
    config = json.loads(bucket.get_blob("dags/cdp2.0-migration/src/configurations/audienceconfigfile.json").download_as_string())
    logging.info("Initializing with Bigquery and bucket completed Successfully.")

    mpn = config[base_mpn]['cdp_multiple_mpn']
    dataset_region = config[base_mpn]["dataset_region"]
    region = config[base_mpn]['region']
    cdp_dumps_dataset = RETENTION_DATASET[region]
    country_code = config[base_mpn]['country_code']

    #Query to get computed trait id and computed trait name from CRS
    crs_query = f'''
    select distinct ecoTraitName as traitName,tm.traitId as traitId from
    `dbce-c360-isl-preprod-d942.CDS.marketing_program_eco_system_trait_map` as tm
    join `dbce-c360-isl-preprod-d942.CDS.trait` AS tr
    on tm.traitId=tr.traitId
    join `dbce-c360-isl-preprod-d942.CDS.ecosystemtrait` est
    on tm.traitId=est.traitId
    and tm.ecoSystemId=est.ecoSystemId
    where tm.ecoSystemId =10 and tm.marketingProgramId in ({mpn}) and lower(tr.traitName) LIKE "email%opt%ind%"
    '''

    if 'cdp_trait_id' in config[base_mpn]:
        trait_id = config[base_mpn]['cdp_trait_id']

    else:
        traits_list_df = client.query(crs_query).to_dataframe()
        trait_id = str(list(traits_list_df['traitId'])).replace('[', '').replace(']', '')

    profile_traits_query = f"""
    CREATE OR REPLACE TABLE  `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_traits_{base_mpn}_{AUDIENCE_DUMPS_TABLE[env]}` AS
    select * from EXTERNAL_QUERY("{PROJECT_TABLES[region]}.{dataset_region}.cdp-profiles{CONNECTION_TYPE[env]}_cdp-profiles_pii", 
    "SELECT * FROM profile_traits where marketing_program_number in ({mpn}) and trait_id in (2774,2779,2709,2734,2714,4886,2735,{trait_id})");
    """ 

    id_graphs_query = f"""
    CREATE OR REPLACE TABLE  `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.id_graphs_{base_mpn}_{AUDIENCE_DUMPS_TABLE[env]}` AS
    select * from EXTERNAL_QUERY("{PROJECT_TABLES[region]}.{dataset_region}.cdp-ids{CONNECTION_TYPE[env]}_cdp-ids_pii", 
    "SELECT id_value,pg_id from id_graphs where id_type='userId' and marketing_program_number in ( {mpn} ) ");
    """

    profile_events_query = f"""
    CREATE OR REPLACE TABLE  `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_events_{base_mpn}_{AUDIENCE_DUMPS_TABLE[env]}` AS
    select * from EXTERNAL_QUERY("{PROJECT_TABLES[region]}.{dataset_region}.cdp-profiles{CONNECTION_TYPE[env]}_cdp-profiles_pii",    
    "SELECT trace_id,pg_id,marketing_program_number,event_name,event_date FROM profile_events where marketing_program_number in ( {mpn} ) and event_name in ('Email Opened','Receipt Verified')  ");
    """

    profile_event_properties_query = f"""
    CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_event_properties_{base_mpn}_{AUDIENCE_DUMPS_TABLE[env]}` as
    select * from EXTERNAL_QUERY("{PROJECT_TABLES[region]}.{dataset_region}.cdp-profiles{CONNECTION_TYPE[env]}_cdp-profiles_pii",    
    '''SELECT trace_id,property_name,property_value FROM profile_event_properties where  property_name='countryCode' and property_value  in ({country_code}) ''');
    """

    return profile_traits_query,id_graphs_query,profile_events_query,profile_event_properties_query,region

def create_dynamic_dag(dag_id, job_config, env):

    logging.info(f"Starting the Audience DAG with config: {job_config}")
    profile_traits_query,id_graphs_query,profile_events_query,profile_event_properties_query,region = spanner_tables_dumps_creation(job_config)

    dag = models.DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval = "0 0 * * *",
        tags=['Audience Counts'])

    with dag:

        start_process = DummyOperator(task_id = "start_process")

        _profile_traits_query = bigquery_operator.BigQueryOperator(
            task_id = f"call_profile_traits_query",
            sql=profile_traits_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        _id_graphs_query = bigquery_operator.BigQueryOperator(
            task_id = f"call_id_graphs_query",
            sql=id_graphs_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
        )

        dag_name = {"preprod":"preprod","prod":"prod","preprodlive":"preprod"}
        cdp_profile_aliases_dumps_task = ExternalTaskSensor(
                task_id = f"wait_for_cdp_profile_aliases_dump_{dag_name[env]}_to_finish",
                external_dag_id = f"cdp_profile_aliases_dump_{dag_name[env]}_{job_config['region']}_region",
                external_task_id = "end_task"
            )

        cdp_audience_counts_task = PythonOperator(
            task_id = f"cdp_audience_counts_for_{job_config['job_id']}",
            python_callable = cdp_audience_counts,
            op_kwargs = {'mpn': job_config['mpn'],'env':env},
        )

        end_process = DummyOperator(task_id = "end_task")

        if region == "EU" or region == "EU_corp":
            _profile_events_query = bigquery_operator.BigQueryOperator(
            task_id = f"call_profile_events_query",
            sql=profile_events_query,
            use_legacy_sql=False,
            allow_large_results=True,
            gcp_conn_id=BQ_CONN_ID
            )

            _profile_event_properties_query = bigquery_operator.BigQueryOperator(
                task_id = f"call_profile_event_properties_query",
                sql=profile_event_properties_query,
                use_legacy_sql=False,
                allow_large_results=True,
                gcp_conn_id=BQ_CONN_ID
            )

            start_process >> _profile_traits_query >> cdp_profile_aliases_dumps_task
            start_process >> _id_graphs_query >> cdp_profile_aliases_dumps_task
            start_process >> _profile_events_query >> cdp_profile_aliases_dumps_task
            start_process >> _profile_event_properties_query >> cdp_profile_aliases_dumps_task 

        if region == "US" or region == "AMA":
            start_process >> _profile_traits_query >> cdp_profile_aliases_dumps_task
            start_process >> _id_graphs_query >> cdp_profile_aliases_dumps_task
        
        cdp_profile_aliases_dumps_task >> cdp_audience_counts_task >> end_process

    return dag


job_configs = [
    {"job_id" : "arb_ghh", "mpn":"363", "region" :"us-east4"}, # ARB GHH
    {"job_id" : "gbr_oralb", "mpn":"417", "region" :"us"}, #GBR ORALB
    {"job_id" : "ita_ghh", "mpn":"289", "region" :"us"}, #ITA GHH
    {"job_id" : "nld_oralb", "mpn":"414", "region" :"us"},  #NLD ORALB
    {"job_id" : "deu_oralb", "mpn":"416", "region" :"us"},  #DEU ORALB
    {"job_id" : "usa_olay", "mpn":"127", "region" :"us"},  #USA OLAY
    {"job_id" : "usa_gillette", "mpn":"119", "region" :"us"},  #USA GILLETTE
    {"job_id" : "fra_braun", "mpn":"507", "region" :"us"},  #FRA BRAUN
    {"job_id" : "usa_oralcare", "mpn":"173", "region" :"us"},  #USA ORALCARE
    {"job_id" : "pol_ghh", "mpn":"249", "region" :"us"},  #POL GHH
    {"job_id" : "twn_living_artist", "mpn":"59", "region" :"us-east4"},  #TWN Living Artist
    {"job_id" : "fra_oralb", "mpn":"415", "region" :"us"},  #FRA OralB
    {"job_id" : "tur_being_girl", "mpn":"209", "region" :"us"}, #TUR BEING GIRL
    {"job_id" : "usa_always_discreet", "mpn":"494", "region" :"us"}, #USA ALWAYS DISCREET
    {"job_id" : "usa_braun", "mpn":"430", "region" :"us"},  #USA BRAUN
    {"job_id" : "usa_always_discreet", "mpn":"494", "region" :"us"}, #USA ALWAYS DISCREET
    {"job_id" : "nor_oralb", "mpn":"515", "region" :"us"},  #DEU FEMIBION
    {"job_id" : "deu_femibion", "mpn":"469", "region" :"us"},  #NOR ORALB
    {"job_id" : "grc_ghh", "mpn":"292", "region" : "us"},  #GRC GHH  
    {"job_id" : "usa_charmin", "mpn":"519", "region" :"us"},  #USA CHARMIN
]
environment = ["preprod","prod","preprodlive"]


for job_config in job_configs:
    for env in environment:
        dag_id = f"cdp_audience_{env}_counts_for_{job_config['job_id']}"
        globals()[dag_id] = create_dynamic_dag(dag_id,
                                            job_config,env
                                            )
