from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery
import logging


default_args = {
    "retries": 0
}


spanner_tables = ["dependent_traits", "traits", "staging_traces","consents", "profile_events","profile_traits",
                  "profile_dependent_traits", "id_graphs", "id_aliases", "traces", "profiles",
                  "profile_consents", 'profile_aliases']


def export_data(**kwargs):

    client = bigquery.Client()

    for table in spanner_tables:
        mpn = kwargs['mpn']
        segment_id = kwargs['segment_id']

        try:
            bucket_name = "dbce-c360-isl-preprod-d942-migration-88fakc29/preprod_data_dump/{}/{}".format(segment_id,table)
            project = "dbce-c360-segamaws-prod-9669"
            dataset_id = "cdp_ops_pii"
            table_id = "cdp2migration_preprod_{}_{}".format(mpn,table)
            destination_uri = "gs://{}/{}".format(bucket_name, table_id+"*")
            
            job_config = bigquery.job.ExtractJobConfig()
            job_config.destination_format = bigquery.DestinationFormat.AVRO
            dataset_ref = bigquery.DatasetReference(project, dataset_id)
            table_ref = dataset_ref.table(table_id)

            extract_job = client.extract_table(
                table_ref,
                destination_uri,
                job_config=job_config,
                location="us-east4",
            )
            extract_job.result()

            logging.info("Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri))
        except Exception as err:
            logging.info("table not found: ",err)


def bigquery_gcs(config,dag_id):

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None,
        tags=['Pre-Prod to Prod Pipeline'])
    with dag:
        
        start_process = DummyOperator(task_id="start_process")

        export_job = PythonOperator(
            task_id=f"export_job_{config['mpn']}",
            python_callable=export_data,
            op_kwargs={
                'mpn':config['mpn'],
                'segment_id':config['segment_id']
            },
            dag=dag
        )
        end_process = DummyOperator(task_id="end_task")
        
        start_process >> export_job >> end_process
    return dag

# configs to be passed each pipeline
mpns=[
    {"job_id": "usa-dental-care", "mpn": "401", "segment_id": "uHF4gx1rXRnHxhpPdJW11A"}, # USA Dental Care
    {"job_id": "jpn-whisper","mpn":"16","segment_id":"0OIBkBp0SO"}, # JPN Whisper
    {"job_id": "esp-beautycode","mpn":"543","segment_id":"hxw1T763uXwhjiVxaXxqnf"}, # ESP Beautycode
    {"job_id": "che-growing-families", "mpn": "295", "segment_id": "1gzw4X2IzF"}, # CHE Growing Families
    {"job_id": "phl-corporate", "mpn": "24", "segment_id": "jzhs6CF79E"}, # PHL Corporate
    {"job_id": "phl-pampers", "mpn": "44", "segment_id": "mQH2ngAyAU"}, # PHL Pampers
    {"job_id": "deu-growing-families", "mpn": "293", "segment_id": "OlbQQtlUbN"},# DEU Growing Families
    {"job_id": "arb-ghh", "mpn": "363", "segment_id": "MCcgOexQ41"},#ARB growing families everyday me arabia
    {"job_id": "irl-ghh", "mpn": "299", "segment_id": "0DP6JncYre"},  # irl growing families
    {"job_id": "usa-old-spice", "mpn": "178", "segment_id": "sQt2a1ePtvWZrHPTXtYQzZ"},  # usa old spice
    {"job_id": "gbr-ghh", "mpn": "288", "segment_id": "dg8HRLgIrf"},  # gbr growing families
    {"job_id": "ita-ghh", "mpn": "289", "segment_id": "SZWWmdBG8R"},  # ita growing familes
    {"job_id": "gbr-oral-b", "mpn": "417", "segment_id": "tE6LZHuBT5"},  # gbr-oral-b
    {"job_id": "deu-oral-b", "mpn": "416", "segment_id": "drN6c1VhJF1TFvPBtNzVm2"}, # deu-oral-b
    {"job_id": "esp-oral-b", "mpn": "412", "segment_id": "BETb2gb5ZF"},  # esp-oral-b
    {"job_id": "nld-oral-b", "mpn": "414", "segment_id": "7omkTccaSsierHHJcfGvA3"},  # nld-oral-b
    {"job_id": "bel-oral-b", "mpn": "524", "segment_id": "bCas6yfwkKMKejhgvA7LU1"},  # bel-oral-b
    {"job_id": "pol-oral-b", "mpn": "411", "segment_id": "hjX61jf8c79XnMLynpdWJH"},  # pol-oral-b
    {"job_id": "swe-oral-b", "mpn": "410", "segment_id": "zW8JEjG1YQkMm9s4JXLQh"},  # swe-oral-b
    {"job_id": "fin-oral-b", "mpn": "517", "segment_id": "vpxp4GjvQGhrvdrqdyim12"}, # fin-oral-b
    {"job_id": "vnm-corporate", "mpn": "39", "segment_id": "d7gSfRhJmVGT8HuqXpHZUu"},#vnm corporate
    {"job_id": "ita-oral-b", "mpn": "413", "segment_id": "uSThcoSGKH6ZyFLik3p821"}, # ita-oral-b
    {"job_id": "dnk-oral-b", "mpn": "516", "segment_id": "6HCs1yXTni2mx2jTY2ePqw"},  # dnk-oral-b
    {"job_id": "usa-olay", "mpn": "127", "segment_id": "5a7Z7BrRdaJqjtjBvYknyU"}, #usa-olay
    {"job_id": "fra-braun", "mpn": "507", "segment_id": "ugQ5f9kznxji7SmxgYpMD"},  # fra-braun
    {"job_id": "fra-jolly", "mpn": "472", "segment_id": "pqXNv21wnoquueVd4qBYEC"},  # fra-jolly
    {"job_id": "esp-braun", "mpn": "518", "segment_id": "kZKGX53dCZG1eJipdLVZB7"},  # esp-braun
    {"job_id": "nld-gillette", "mpn": "424", "segment_id": "p1tozEgYtweTyHYNG4sc84"},  # nld-gillette
    {"job_id": "tw-living-artist", "mpn": "59", "segment_id": "cutgFGK7j7UM5GcBKYNzXQ"},  # tw-living-artist
    {"job_id": "ita-gillette", "mpn": "353", "segment_id": "5g67Lywe9qYHtc4i4tYwwb"},  # ita-gillette
    {"job_id": "esp-gillette", "mpn": "308", "segment_id": "4rXsMdrgZ8G1NZFmfMg5j7"},  # esp-gillette
    {"job_id": "usa-gillette-v2", "mpn": "119", "segment_id": "hA8niK3xyR3oqEWDAZ2BSk"},  # usa-gillette-v2
    {"job_id": "can-dental-care", "mpn": "405", "segment_id": "qrCjssbZuwNdKWrihbythY"}, # can-dental-care
    {"job_id": "grc-ghh", "mpn": "292", "segment_id": "4254sfoIMZ"},  # grc-ghh
    {"job_id": "tur-being-girl", "mpn": "209", "segment_id": "pH12TfD8UcF7hdxcavFSJy"},  # tur-being-girl
    {"job_id": "fra-oral-b", "mpn": "415", "segment_id": "s71J9VY2mG"},  # fra-oral-b
    {"job_id": "usa-oral-care", "mpn": "173", "segment_id": "eRs2nPePzcbf8UE5ydrpDU"}, #usa-oral-care
    {"job_id": "pol-ghh", "mpn": "249", "segment_id": "bJiXj7XStD"}, #pol-ghh
    {"job_id": "usa-braun", "mpn": "430", "segment_id": "pT4rLszF5D"}, #usa-braun
    {"job_id": "afr-pampers", "mpn": "380", "segment_id": "b2HLll4uJR"}, #afr-pampers
    {"job_id": "bgr-corporate", "mpn": "384", "segment_id": "zvLBAHe2xD"}, #bgr-corporate
    {"job_id": "usa-always-discreet", "mpn": "494", "segment_id": "5fDrVQHdhdMU4NC83fR2e4"} #usa-always-discreet
]

# build a dag for each mpn
for config in mpns:
    dag_id = f"bigquery_to_gcs_for_{config['job_id']}"
    globals()[dag_id] = bigquery_gcs(config,dag_id)