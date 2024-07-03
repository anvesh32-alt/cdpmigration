from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
import pendulum
from utils.segmentAudienceCounts import segment_audience_counts


default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 2
}


def create_dynamic_dag(dag_id, job_config, env):

    logging.info(f"Starting the Audience DAG with config: {job_config}")

    dag = models.DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval = "0 0 * * *",
        tags=['Audience Counts'])

    with dag:

        start_process = DummyOperator(task_id="start_process")

        segment_audience_counts_task = PythonOperator(
            task_id = f"segment_audience_counts_for_{job_config['job_id']}",
            python_callable = segment_audience_counts,
            op_kwargs = {'mpn': job_config['mpn'],'env':env},
        )

        end_process = DummyOperator(task_id="end_task")

        start_process  >> segment_audience_counts_task >> end_process

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
    {"job_id" : "usa_charmin", "mpn":"519", "region" :"us"},  #USA CHARMIN
    {"job_id" : "grc_ghh", "mpn":"292", "region" : "us"},  #GRC GHH  
]

environment = ["preprod","prod","preprodlive"]

for job_config in job_configs:
    for env in environment:
        dag_id = f"segment_audience_{env}_counts_for_{job_config['job_id']}"
        globals()[dag_id] = create_dynamic_dag(dag_id,
                                            job_config,env
                                            )
