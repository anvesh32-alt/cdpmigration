from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import json
import logging


DAGS_FOLDER = Variable.get("dags_folder")
GCS_TMP = Variable.get("gcs_temp")
GCS_STAGING = Variable.get("gcs_staging")
DATAFLOW_PROJECT = Variable.get("dataflow_project")
DATAFLOW_REGION = Variable.get("dataflow_region")
DATAFLOW_SERVICE_ACCOUNT = Variable.get("dataflow_service_account")
DATAFLOW_NETWORK = Variable.get("dataflow_network")
DATAFLOW_SUBNETWORK = Variable.get("dataflow_subnetwork")


PROJECT = 'dbce-c360-isl-preprod-d942'
STORAGE_BUCKET = 'dbce-c360-isl-preprod-d942-migration-88fakc29'
DESTINATION_SPANNER_PROJECT = 'dbce-c360-mdm-prod-2a9c'
GCS_SPANNER_INPUT_BUCKET = "gs://dbce-c360-isl-preprod-d942-migration-88fakc29/preprod_data_dump/{}/{}"


default_args = {
    "retries": 0,
    'dataflow_default_options': {
        'project': DATAFLOW_PROJECT,
        'tempLocation': GCS_TMP,
        'stagingLocation': GCS_STAGING,
        'numWorkers': 10,
        'maxNumWorkers': 200,
        'maxWorkers': 200,
        'subnetwork': DATAFLOW_SUBNETWORK,
        'network': DATAFLOW_NETWORK,
        'serviceAccountEmail': DATAFLOW_SERVICE_ACCOUNT,
    }
}


#create spanner-export.json config file required for template (step 1)
def create_config_file(**kwargs):

    table_name = kwargs['spanner_table']
    segment_id = kwargs['segment_id']
    client = storage.Client(PROJECT)
    bucket = client.get_bucket(STORAGE_BUCKET)
    folder_path = f"preprod_data_dump/{segment_id}/{table_name}"
    blobs = bucket.list_blobs(prefix=folder_path)

    file_list = []
    for blob in blobs:
        file_list.append(blob.name.split('/')[-1])    

    payload = {
        "tables": [
            {
                "name": f"{table_name}",
                "dataFiles": file_list
            }
        ]
    }

    blob = bucket.blob(f'preprod_data_dump/{segment_id}/{table_name}/spanner-export.json')
    blob.upload_from_string(
        data=json.dumps(payload,indent=4),
        content_type='application/json'
        )
    logging.info('files are generated for',table_name)


#delete spanner-export.json config file (step 3)
def delete_config_file(**kwargs):

    table_name = kwargs['spanner_table']
    segment_id = kwargs['segment_id']
    client = storage.Client(PROJECT)
    bucket = client.get_bucket(STORAGE_BUCKET)
    file_path = f"preprod_data_dump/{segment_id}/{table_name}/spanner-export.json"
    blob = bucket.blob(file_path)
    blob.delete()

    logging.info('config files were deleted for',table_name)


def spanner_to_spanner(config,dag_id, job_config):

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

        spanner_config_dataflow_task = PythonOperator(
            task_id=f"config_files_creation_{config['mpn']}_{job_config['spanner_table']}",
            python_callable = create_config_file,
            op_kwargs={
                'segment_id':config['segment_id'],
                'spanner_table':job_config['spanner_table']
            },
            dag=dag
        )

        # use GCS_Avro_to_Cloud_Spanner template to send data from GCS isl-preprod to spanner mdm-prod table (step2)
        avro_to_spanner_dataflow_task = DataflowTemplateOperator(
            task_id=f"table_transfer_{job_config['spanner_table']}_prod_{config['job_id']}",
            job_name='{{task.task_id}}',
            template='gs://dataflow-templates/latest/GCS_Avro_to_Cloud_Spanner',
            location=DATAFLOW_REGION,
            parameters={
                'spannerProjectId': DESTINATION_SPANNER_PROJECT,
                'instanceId': job_config["spanner_instance"],
                'databaseId': job_config["spanner_database"],
                'inputDir': GCS_SPANNER_INPUT_BUCKET.format(config['segment_id'],job_config['spanner_table'])
            },
        )

        delete_spanner_config_dataflow_task = PythonOperator(
            task_id=f"config_files_deletion_{config['mpn']}_{job_config['spanner_table']}",
            python_callable = delete_config_file,
            op_kwargs={
                'segment_id':config['segment_id'],
                'spanner_table':job_config['spanner_table']
            },
            dag=dag
        )

        end_process = DummyOperator(task_id="end_task")

        start_process >> spanner_config_dataflow_task >> avro_to_spanner_dataflow_task >> delete_spanner_config_dataflow_task >> end_process

        return dag


# configs to be passed each pipeline
job_configs = [
    {"spanner_instance": "cdp-traits", "spanner_database": "cdp-traits", "spanner_table": "dependent_traits"},
    {"spanner_instance": "cdp-traits", "spanner_database": "cdp-traits", "spanner_table": "traits"},
    {"spanner_instance": "cdp-staging-traces", "spanner_database": "cdp-staging-traces","spanner_table": "staging_traces"},
    {"spanner_instance": "cdp-consents", "spanner_database": "cdp-consents", "spanner_table": "consents"},
    {"spanner_instance": "cdp-profiles", "spanner_database": "cdp-profiles","spanner_table": "profile_events"},
    {"spanner_instance": "cdp-profiles", "spanner_database": "cdp-profiles","spanner_table": "profile_event_properties"},
    {"spanner_instance": "cdp-profiles", "spanner_database": "cdp-profiles","spanner_table": "profile_traits"},
    {"spanner_instance": "cdp-profiles", "spanner_database": "cdp-profiles","spanner_table": "profile_dependent_traits"},
    {"spanner_instance": "cdp-ids", "spanner_database": "cdp-ids","spanner_table": "traces"},
    {"spanner_instance": "cdp-ids", "spanner_database": "cdp-ids","spanner_table": "profiles"},
    {"spanner_instance": "cdp-ids", "spanner_database": "cdp-ids","spanner_table": "id_graphs"},
    {"spanner_instance": "cdp-ids", "spanner_database": "cdp-ids","spanner_table": "id_aliases"},
    {"spanner_instance": "cdp-consents", "spanner_database": "cdp-consents", "spanner_table": "profile_consents"},
    {"spanner_instance": "cdp-profiles", "spanner_database": "cdp-profiles","spanner_table": "profile_aliases"},
]

# configs to be passed each pipeline
mpns = [
    {"job_id": "usa-dental-care", "mpn": "401", "segment_id": "uHF4gx1rXRnHxhpPdJW11A"}, # USA Dental Care
    {"job_id": "jpn-whisper","mpn":"16","segment_id":"0OIBkBp0SO"}, # JPN Whisper
    {"job_id": "esp-beautycode","mpn":"543","segment_id":"hxw1T763uXwhjiVxaXxqnf"}, # ESP Beautycodey
    {"job_id": "che-growing-families", "mpn": "295", "segment_id": "1gzw4X2IzF"}, # CHE Growing Families
    {"job_id": "deu-growing-families", "mpn": "293", "segment_id": "OlbQQtlUbN"}, # DEU Growing Families
    {"job_id": "arb-ghh", "mpn": "363", "segment_id": "MCcgOexQ41"},#ARB growing families everyday me arabia
    {"job_id": "irl-ghh", "mpn": "299", "segment_id": "0DP6JncYre"},  # irl growing families
    {"job_id": "usa-old-spice", "mpn": "178", "segment_id": "sQt2a1ePtvWZrHPTXtYQzZ"},  # usa old spice
    {"job_id": "gbr-ghh", "mpn": "288", "segment_id": "dg8HRLgIrf"},  # gbr growing families
    {"job_id": "ita-ghh", "mpn": "289", "segment_id": "SZWWmdBG8R"},  # ita growing families
    {"job_id": "gbr-oral-b", "mpn": "417", "segment_id": "tE6LZHuBT5"},  # gbr-oral-b
    {"job_id": "deu-oral-b", "mpn": "416", "segment_id": "drN6c1VhJF1TFvPBtNzVm2"},  # deu-oral-b
    {"job_id": "esp-oral-b", "mpn": "412", "segment_id": "BETb2gb5ZF"},  # esp-oral-b
    {"job_id": "nld-oral-b", "mpn": "414", "segment_id": "7omkTccaSsierHHJcfGvA3"},  # nld-oral-b
    {"job_id": "bel-oral-b", "mpn": "524", "segment_id": "bCas6yfwkKMKejhgvA7LU1"},  # bel-oral-b
    {"job_id": "pol-oral-b", "mpn": "411", "segment_id": "hjX61jf8c79XnMLynpdWJH"},  # pol-oral-b
    {"job_id": "swe-oral-b", "mpn": "410", "segment_id": "zW8JEjG1YQkMm9s4JXLQh"},  # swe-oral-b
    {"job_id": "fin-oral-b", "mpn": "517", "segment_id": "vpxp4GjvQGhrvdrqdyim12"},  # fin-oral-b
    {"job_id": "vnm-corporate", "mpn": "39", "segment_id": "d7gSfRhJmVGT8HuqXpHZUu"},#vnm corporate
    {"job_id": "ita-oral-b", "mpn": "413", "segment_id": "uSThcoSGKH6ZyFLik3p821"}, # ita-oral-b
    {"job_id": "dnk-oral-b", "mpn": "516", "segment_id": "6HCs1yXTni2mx2jTY2ePqw"}, # dnk-oral-b
    {"job_id": "usa-olay", "mpn": "127", "segment_id": "5a7Z7BrRdaJqjtjBvYknyU"},#usa-olay
    {"job_id": "usa-olay", "mpn": "127", "segment_id": "5a7Z7BrRdaJqjtjBvYknyU"},  # usa-olay
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

# build a dag for each config
for config in mpns:
    for job_config in job_configs:
        dag_id = f"table_transfer_{config['job_id']}_{job_config['spanner_table']}_prod"
        globals()[dag_id] = spanner_to_spanner(config,dag_id, job_config)