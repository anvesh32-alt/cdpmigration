##########
# Note 
##########
# Please set the below arguments before running the job as needed
# Line 27 - MPN
# Line 75 - 78 - Destination table configurations
# If the load fails due to DEADLINE Exceeded error while processing large volume markets set the argument 'workerMachineType' to 'c2d-standard-4'.

import airflow
from airflow import models
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreateJavaJobOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow.providers.apache.beam.operators.beam import BeamRunJavaPipelineOperator
import logging
from datetime import datetime
from google.cloud import spanner
from airflow.operators.python_operator import PythonOperator

GCS_TMP = Variable.get("gcs_temp")
GCS_STAGING = Variable.get("gcs_staging")
DATAFLOW_PROJECT = Variable.get("dataflow_project")
DATAFLOW_SERVICE_ACCOUNT = Variable.get("dataflow_service_account")
DATAFLOW_NETWORK = Variable.get("dataflow_network")
DATAFLOW_SUBNETWORK = Variable.get("dataflow_subnetwork")
MPN = 430

current_date = datetime.now().strftime("%d-%m-%Y")

default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 0,
}

with models.DAG(
    "spanner-multi-read-write-profileeventproperties",
    default_args=default_args,
    max_active_runs=1,
    start_date=datetime(2022, 8, 3),
    catchup=False,
    schedule_interval=None,
    tags=['Java Pipeline - Spanner to Spanner multi instance']
) as dag_native_python:
    
    start_process = DummyOperator(
        task_id="startprocess",
    )

    launch_java_pipeline =BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="transfertospanner",
        jar="gs://us-east4-isl-composer-2-7238166d-bucket/dags/spanner-multi-read-write-bundled-0.0.1.jar",
        pipeline_options={
            'project': DATAFLOW_PROJECT,
            'gcpTempLocation': GCS_TMP,
            'stagingLocation': GCS_STAGING,
            'numWorkers': 10,
            'maxNumWorkers': 300,
            'workerMachineType': 'n1-highmem-2',
            #'workerMachineType':'c2d-standard-4',
            'subnetwork': DATAFLOW_SUBNETWORK,
            'network': DATAFLOW_NETWORK,
            'serviceAccount': DATAFLOW_SERVICE_ACCOUNT,
            # main table configurations  
            'mainTableProjectId' : "dbce-c360-mdm-pre-prod-4bfd",
            'mainTableInstanceId' : "cdp-profiles",
            'mainDatabaseId' : "cdp-profiles",
            'mainTableName' : "profile_event_properties",
            # joining table configurations
            'joiningTableProjectId' : "dbce-c360-mdm-prod-2a9c",
            'joiningInstanceId' : "cdp-profiles",
            'joiningDatabaseId' : "cdp-profiles",
            'joiningTableName' : "profile_events",
            # destination table configurations
            'destinationProjectId' : "dbce-c360-mdm-prod-2a9c",
            'destinationInstanceId' : "cdp-profiles",
            'destinationDatabaseId' : "cdp-profiles",
            'destinationTable' : "profile_event_properties",
            # indexes
            "profileEventPropertiesTableIndexName":"trace_id",
            "profileEventTableIndexName":"trace_id",
            'MPN' : MPN

        },
        job_class="dataflow.spanner.SpannerMultipleReadWriteProfileEventProperties",
        dataflow_config={
            "location": "us-east4",
            "job_name" : f"profileeventpropertiesmigration_{MPN}"            
        }

    )

start_process >> launch_java_pipeline