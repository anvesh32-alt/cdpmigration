from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime


default_args = {
    "retries": 0
}


SOURCE_BUCKET = 'dbce-c360-isl-preprod-d942-pii-datalake-segment-7cb2hqjg'
DESTINATION_BUCKET = 'dbce-c360-isl-preprod-d942-migration-88fakc29'


def gcs_to_gcs(config,dag_id):

    SOURCE_OBJECT_PATH = f'segment-logs/{config["segment_id"]}/'
    DESTINATION_OBJECT_PATH_TODO = f'migration_data/output-data/{config["segment_id"]}/todo/'

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        max_active_runs=1,
        start_date=datetime(2022, 8, 3),
        catchup=False,
        schedule_interval=None)
    with dag:

        start_process = DummyOperator(task_id="start_process")

        move_files = GCSToGCSOperator(
            task_id=f"move_files_for_{config['mpn']}",
            source_bucket=SOURCE_BUCKET,
            source_object=SOURCE_OBJECT_PATH + "*",
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PATH_TODO,
            move_object=True,
        )

        end_process = DummyOperator(task_id="end_task")

        start_process >> move_files >> end_process
    
    return dag

# configs to be passed each pipeline

# build a dag for each mpn
mpns=[
    {"job_id": "usa-dental-care", "mpn": "401", "segment_id": "uHF4gx1rXRnHxhpPdJW11A"}, # USA Dental Care
    {"job_id": "jpn-whisper","mpn":"16","segment_id":"0OIBkBp0SO"}, # JPN Whisper
    {"job_id": "esp-beautycode","mpn":"543","segment_id":"hxw1T763uXwhjiVxaXxqnf"}, # ESP Beautycode
    {"job_id": "che-growing-families", "mpn": "295", "segment_id": "1gzw4X2IzF"}, # CHE Growing Families
    {"job_id": "phl-corporate", "mpn": "24", "segment_id": "jzhs6CF79E"}, # PHL Corporate
    {"job_id": "phl-pampers", "mpn": "44", "segment_id": "mQH2ngAyAU"}, # PHL Pampers
    {"job_id": "deu-growing-families", "mpn": "293", "segment_id": "OlbQQtlUbN"}, # DEU Growing Families
    {"job_id": "arb-ghh", "mpn": "363", "segment_id": "MCcgOexQ41"},#ARB growing families everyday me arabia
    {"job_id": "irl-ghh", "mpn": "299", "segment_id": "0DP6JncYre"},#irl growing families
    {"job_id": "usa-old-spice", "mpn": "178", "segment_id": "sQt2a1ePtvWZrHPTXtYQzZ"},#usa old spice
    {"job_id": "gbr-ghh", "mpn": "288", "segment_id": "dg8HRLgIrf"},#gbr growing families
    {"job_id": "ita-ghh", "mpn": "289", "segment_id": "SZWWmdBG8R"},#ita rowing families
    {"job_id": "gbr-oral-b", "mpn": "417", "segment_id": "tE6LZHuBT5"},  # gbr-oral-b
    {"job_id": "deu-oral-b", "mpn": "416", "segment_id": "drN6c1VhJF1TFvPBtNzVm2"},  # deu-oral-b
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
    {"job_id": "tw-living-artist", "mpn": "59", "segment_id": "cutgFGK7j7UM5GcBKYNzXQ"},  # tw-living-artist
    {"job_id": "fra-braun", "mpn": "507", "segment_id": "ugQ5f9kznxji7SmxgYpMD"},  # fra-braun
    {"job_id": "fra-jolly", "mpn": "472", "segment_id": "pqXNv21wnoquueVd4qBYEC"},  # fra-jolly
    {"job_id": "esp-braun", "mpn": "518", "segment_id": "kZKGX53dCZG1eJipdLVZB7"},  # esp-braun
    {"job_id": "esp-gillette", "mpn": "308", "segment_id": "4rXsMdrgZ8G1NZFmfMg5j7"},  # esp-gillette
    {"job_id": "ita-gillette", "mpn": "353", "segment_id": "5g67Lywe9qYHtc4i4tYwwb"},  # ita-gillette
    {"job_id": "nld-gillette", "mpn": "424", "segment_id": "p1tozEgYtweTyHYNG4sc84"},  # nld-gillette
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

for config in mpns:
    dag_id = f"move_files_segment_to_migration_bucket_for_{config['job_id']}"
    globals()[dag_id] = gcs_to_gcs(config,dag_id)