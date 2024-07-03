import airflow
from airflow import models
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
import logging
from datetime import datetime

DAGS_FOLDER = Variable.get("dags_folder")
GCS_TMP = Variable.get("gcs_temp")
GCS_STAGING = Variable.get("gcs_staging")
DATAFLOW_PROJECT = Variable.get("dataflow_project")
DATAFLOW_REGION = Variable.get("dataflow_region")
DATAFLOW_SERVICE_ACCOUNT = Variable.get("dataflow_service_account")
DATAFLOW_NETWORK = Variable.get("dataflow_network")
DATAFLOW_SUBNETWORK = Variable.get("dataflow_subnetwork")

current_date = datetime.now().strftime("%d-%m-%Y")

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0,
}


with DAG(
        dag_id="transfer_to_spanner",
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        start_date=datetime(2020, 1, 1),
        schedule_interval= None,
) as dag:
    job_configs = [
        {"job_id": "staging_traces_table", "table": "staging_traces", "destinationTable": "staging_traces",
         "instanceId": "cdp-staging-traces", "databaseId": "cdp-staging-traces"},
        {"job_id": "id_graphs_table", "table": "id_graphs", "destinationTable": "id_graphs",
         "instanceId": "cdp-ids", "databaseId": "cdp-ids"},
        {"job_id": "traces_table", "table": "traces", "destinationTable": "traces",
         "instanceId": "cdp-ids", "databaseId": "cdp-ids"},
        {"job_id": "profile_consents_table", "table": "profile_consents", "destinationTable": "profile_consents",
         "instanceId": "cdp-consents", "databaseId": "cdp-consents"},
        {"job_id": "profiles_table", "table": "profiles", "destinationTable": "profiles",
         "instanceId": "cdp-ids", "databaseId": "cdp-ids"},
        {"job_id": "profile_traits_table", "table": "profile_traits", "destinationTable": "profile_traits",
         "instanceId": "cdp-profiles", "databaseId": "cdp-profiles"},
        {"job_id": "profile_events_table", "table": "profile_events", "destinationTable": "profile_events",
         "instanceId": "cdp-profiles", "databaseId": "cdp-profiles"},
        {"job_id": "profile_dependent_traits_table", "table": "profile_dependent_traits",
         "destinationTable": "profile_dependent_traits",
         "instanceId": "cdp-profiles", "databaseId": "cdp-profiles"},
        {"job_id": "dependent_traits_table", "table": "dependent_traits", "destinationTable": "dependent_traits",
         "instanceId": "cdp-traits", "databaseId": "cdp-traits"},
    ]
    start_process = DummyOperator(
        task_id="start_process",
        dag=dag,
    )
    launch_jobs = []
    for job_config in job_configs:
        launch_pipeline = BeamRunJavaPipelineOperator(
            runner=BeamRunnerType.DataflowRunner,
            task_id=f"spanner_to_spanner_{job_config['job_id']}",
            jar="gs://us-east4-isl-composer-2-7238166d-bucket/dags/spanner-read-write-bundled-0.0.1.jar",
            pipeline_options={
                'instanceId': job_config["instanceId"],
                'databaseId': job_config["databaseId"],
                'table': job_config["table"],
                "destinationTable": job_config["destinationTable"],
                'project': DATAFLOW_PROJECT,
                'gcpTempLocation': GCS_TMP,
                'MPN': "115",
                'workerMachineType': 'n1-highmem-2',
                'stagingLocation': GCS_STAGING,
                'numWorkers': 10,
                'maxNumWorkers': 32,
                'subnetwork': DATAFLOW_SUBNETWORK,
                'network': DATAFLOW_NETWORK,
                'serviceAccount': DATAFLOW_SERVICE_ACCOUNT
            },
            job_class="dataflow.spanner.SpannerReadWrite",
            dataflow_config={
                "location": "us-east4",
            },
            dag=dag,
        )
        launch_jobs.append(launch_pipeline)

    end_process = DummyOperator(
        task_id="end_process",
        dag=dag,
    )

    start_process>>launch_jobs>>end_process