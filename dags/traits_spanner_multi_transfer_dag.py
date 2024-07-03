##########
# Note 
##########
# Please set the below arguments before running the job as needed
# Line 28 - MPN
# Line 77 - 80 - Destination table configurations
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
from airflow.operators.dummy_operator import DummyOperator

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
    "spanner-multi-read-write-traits",
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

    launch_java_pipeline = BeamRunJavaPipelineOperator(
        runner=BeamRunnerType.DataflowRunner,
        task_id="transfertospanner",
        jar="gs://us-east4-isl-composer-2-7238166d-bucket/dags/spanner-multi-read-write-bundled-0.0.1.jar",
        
        pipeline_options={
            'project': DATAFLOW_PROJECT,
            'gcpTempLocation': GCS_TMP,
            'stagingLocation': GCS_STAGING,
            'numWorkers': 10,
            'workerMachineType': 'n1-highmem-2',
            'maxNumWorkers': 32,
            #'workerMachineType':'c2d-standard-4',
            'subnetwork': DATAFLOW_SUBNETWORK,
            'network': DATAFLOW_NETWORK,
            'serviceAccount': DATAFLOW_SERVICE_ACCOUNT,
            # main table configurations
            'mainTableProjectId' : "dbce-c360-mdm-pre-prod-4bfd",
            'mainTableInstanceId' : "cdp-traits",
            'mainDatabaseId' : "cdp-traits",
            'mainTableName' : "traits",
            # joining table configurations
            'joiningTableProjectId' : "dbce-c360-mdm-prod-2a9c",
            'joiningInstanceId' : "cdp-traits",
            'joiningDatabaseId' : "cdp-traits",
            'joiningTableName' : "traits",
            # destination table configurations
            'destinationProjectId' : "dbce-c360-mdm-prod-2a9c",
            'destinationInstanceId' : "cdp-traits",
            'destinationDatabaseId' : "cdp-traits",
            'destinationTable' : "traits",
            # indexes
            'traitsTableIndexName' : "marketing_program_number",
            'MPN' : MPN

        },
        job_class="dataflow.spanner.SpannerMultipleReadWriteTraits",
        dataflow_config={
            "location": "us-east4",
            "job_name" : f"traitsmigration_{MPN}"
        }

    )

def update_trait_value_execute_sql():
    sql_statement="""UPDATE traits SET trait_value =null WHERE trait_value ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-traits")
    database = instance.database("cdp-traits")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)        
    print("trait_value update completed")

def update_trait_value_data_type_execute_sql():
    sql_statement="""UPDATE traits SET trait_value_data_type =null WHERE trait_value_data_type ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-traits")
    database = instance.database("cdp-traits")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)          
    print("trait_value_data_type update completed")

def update_trait_properties_execute_sql():
    sql_statement="""UPDATE traits SET trait_properties =null WHERE TO_JSON_STRING(trait_properties)="{}" and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-traits")
    database = instance.database("cdp-traits")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)          
    print("trait_properties update completed")

def update_trait_consumer_last_updated_date_execute_sql():
    sql_statement="""UPDATE traits SET trait_consumer_last_updated_date =null WHERE trait_consumer_last_updated_date ='1900-01-01T02:19:32Z' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-traits")
    database = instance.database("cdp-traits")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)         
    print("trait_consumer_last_updated_date update completed")

update_trait_value_data = PythonOperator(
        task_id="starttraitvalueupdate",
        python_callable= update_trait_value_execute_sql
    )
update_trait_value_data_type_data = PythonOperator(
        task_id="starttraitvaluedatatypeupdate",
        python_callable= update_trait_value_data_type_execute_sql
    )
update_trait_properties_data = PythonOperator(
        task_id="starttraitpropertiesupdate",
        python_callable= update_trait_properties_execute_sql
    )
update_trait_consumer_last_updated_date_data = PythonOperator(
        task_id="starttraitconsumerlastupdateddateupdate",
        python_callable= update_trait_consumer_last_updated_date_execute_sql
    )

start_process >> launch_java_pipeline >> [update_trait_value_data,update_trait_value_data_type_data,update_trait_properties_data,update_trait_consumer_last_updated_date_data]