##########
# Note 
##########
# Please set the below arguments before running the job as needed
# Line 28 - MPN
# Line 78 - 81 - Destination table configurations
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
    "spanner-multi-read-write-consents",
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
            'maxNumWorkers': 32,
            'workerMachineType': 'n1-highmem-2',
            #'workerMachineType':'c2d-standard-4',
            'subnetwork': DATAFLOW_SUBNETWORK,
            'network': DATAFLOW_NETWORK,
            'serviceAccount': DATAFLOW_SERVICE_ACCOUNT,
            # main table configurations            
            'mainTableProjectId' : "dbce-c360-mdm-pre-prod-4bfd",
            'mainTableInstanceId' : "cdp-consents",
            'mainDatabaseId' : "cdp-consents",
            'mainTableName' : "consents",
            # joining table configurations
            'joiningTableProjectId' : "dbce-c360-mdm-prod-2a9c",
            'joiningInstanceId' : "cdp-consents",
            'joiningDatabaseId' : "cdp-consents",
            'joiningTableName' : "consents",
            # destination table configurations
            'destinationProjectId' : "dbce-c360-mdm-prod-2a9c",
            'destinationInstanceId' : "cdp-consents",
            'destinationDatabaseId' : "cdp-consents",
            'destinationTable' : "consents",
            # indexes
            'consentsTableIndexName' : "marketing_program_number",
            'MPN' : MPN

        },
        job_class="dataflow.spanner.SpannerMultipleReadWriteConsents",
        dataflow_config={
            "location": "us-east4",
            "job_name" : f"consentsmigration_{MPN}" 
        }

    )
def update_email_execute_sql():
    sql_statement="""UPDATE consents SET email =null WHERE email ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)       
    print("email update completed")

def update_phone_number_execute_sql():
    sql_statement="""UPDATE consents SET phone_number =null WHERE phone_number ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)         
    print("phone_number update completed")

def update_opt_text_execute_sql():
    sql_statement="""UPDATE consents SET opt_text =null WHERE opt_text ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)        
    print("opt_text update completed")

def update_address_line1_execute_sql():
    sql_statement="""UPDATE consents SET address_line1 =null WHERE address_line1 ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)       
    print("address_line1 update completed")

def update_address_line2_execute_sql():
    sql_statement="""UPDATE consents SET address_line2 =null WHERE address_line2 ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)          
    print("address_line2 update completed")

def update_address_line3_execute_sql():
    sql_statement="""UPDATE consents SET address_line3 =null WHERE address_line3 ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)        
    print("address_line3 update completed")

def update_street_name_execute_sql():
    sql_statement="""UPDATE consents SET street_name =null WHERE street_name ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)      
    print("street_name update completed")

def update_apartment_number_execute_sql():
    sql_statement="""UPDATE consents SET apartment_number =null WHERE apartment_number ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)       
    print("apartment_number update completed")

def update_city_name_execute_sql():
    sql_statement="""UPDATE consents SET city_name =null WHERE city_name ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)     
    print("city_name update completed")

def update_territory_name_execute_sql():
    sql_statement="""UPDATE consents SET territory_name =null WHERE territory_name ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)  
    print("territory_name update completed")

def update_po_box_number_execute_sql():
    sql_statement="""UPDATE consents SET po_box_number =null WHERE po_box_number ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)      
    print("po_box_number update completed")

def update_postal_area_code_execute_sql():
    sql_statement="""UPDATE consents SET postal_area_code =null WHERE postal_area_code ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)      
    print("postal_area_code update completed")

def update_po_box_execute_sql():
    sql_statement="""UPDATE consents SET po_box =null WHERE po_box ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)        
    print("po_box update completed")

def update_house_number_execute_sql():
    sql_statement="""UPDATE consents SET house_number =null WHERE house_number ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)    
    print("house_number update completed")

def update_street_complement_execute_sql():
    sql_statement="""UPDATE consents SET street_complement =null WHERE street_complement ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)      
    print("street_complement update completed")

def update_city_complement_execute_sql():
    sql_statement="""UPDATE consents SET city_complement =null WHERE city_complement ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)       
    print("city_complement update completed")

def update_building_execute_sql():
    sql_statement="""UPDATE consents SET building =null WHERE building ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)      
    print("building update completed")    

def update_sub_building_execute_sql():
    sql_statement="""UPDATE consents SET sub_building =null WHERE sub_building ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)       
    print("sub_building update completed") 

def update_street_number_execute_sql():
    sql_statement="""UPDATE consents SET street_number =null WHERE street_number ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)      
    print("street_number update completed") 

def update_opt_reason_execute_sql():
    sql_statement="""UPDATE consents SET opt_reason =null WHERE opt_reason ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types) 
    print("opt_reason update completed") 

def update_unsubscribe_url_execute_sql():
    sql_statement="""UPDATE consents SET unsubscribe_url =null WHERE unsubscribe_url ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)       
    print("unsubscribe_url update completed")

def update_source_system_execute_sql():
    sql_statement="""UPDATE consents SET source_system =null WHERE source_system ='NULL' and marketing_program_number = @param_value"""
    spanner_client = spanner.Client("dbce-c360-mdm-prod-2a9c")
    instance = spanner_client.instance("cdp-consents")
    database = instance.database("cdp-consents")
    params = {"param_value": MPN}
    param_types = {"param_value": spanner.param_types.INT64}  
    database.execute_partitioned_dml(sql_statement,params=params, param_types=param_types)     
    print("source_system update completed")

update_email_data = PythonOperator(
        task_id="startemailupdate",
        python_callable= update_email_execute_sql
    )
update_phone_number_data = PythonOperator(
        task_id="startphonenumberupdate",
        python_callable= update_phone_number_execute_sql
    )
update_opt_text_data = PythonOperator(
        task_id="startopttextupdate",
        python_callable= update_opt_text_execute_sql
    )
update_address_line1_data = PythonOperator(
        task_id="startaddressline1update",
        python_callable= update_address_line1_execute_sql
    )
update_address_line2_data = PythonOperator(
        task_id="startaddressline2update",
        python_callable= update_address_line2_execute_sql
    )
update_address_line3_data = PythonOperator(
        task_id="startaddressline3update",
        python_callable= update_address_line3_execute_sql
    )
update_street_name_data = PythonOperator(
        task_id="startstreetnameupdate",
        python_callable= update_street_name_execute_sql
    )
update_apartment_number_data = PythonOperator(
        task_id="startapartmentnumberupdate",
        python_callable= update_apartment_number_execute_sql
    )
update_city_name_data = PythonOperator(
        task_id="startcitynameupdate",
        python_callable= update_city_name_execute_sql
    )
update_territory_name_data = PythonOperator(
        task_id="startterritorynameupdate",
        python_callable= update_territory_name_execute_sql
    )
update_po_box_number_data = PythonOperator(
        task_id="startpoboxnumberupdate",
        python_callable= update_po_box_number_execute_sql
    )
update_postal_area_code_data = PythonOperator(
        task_id="startpostalareacodeupdate",
        python_callable= update_postal_area_code_execute_sql
    )
update_po_box_data = PythonOperator(
        task_id="startpoboxupdate",
        python_callable= update_po_box_execute_sql
    )
update_house_number_data = PythonOperator(
        task_id="starthousenumberupdate",
        python_callable= update_house_number_execute_sql
    )
update_street_complement_data = PythonOperator(
        task_id="startstreetcomplementupdate",
        python_callable= update_street_complement_execute_sql
    )
update_city_complement_data = PythonOperator(
        task_id="startcitycomplementupdate",
        python_callable= update_city_complement_execute_sql
    )
update_building_data = PythonOperator(
        task_id="startbuildingupdate",
        python_callable= update_building_execute_sql
    )
update_sub_building_data = PythonOperator(
        task_id="startsubbuildingupdate",
        python_callable= update_sub_building_execute_sql
    )
update_street_number_data = PythonOperator(
        task_id="startstreetnumberupdate",
        python_callable= update_street_number_execute_sql
    )
update_opt_reason_data = PythonOperator(
        task_id="startoptreasonupdate",
        python_callable= update_opt_reason_execute_sql
    )
update_unsubscribe_url_data = PythonOperator(
        task_id="startunsubscribeurlupdate",
        python_callable= update_unsubscribe_url_execute_sql
    )
update_source_system_data = PythonOperator(
        task_id="startsourcesystemupdate",
        python_callable= update_source_system_execute_sql
    )

start_process >> launch_java_pipeline >> [update_email_data ,update_phone_number_data ,update_opt_text_data ,update_address_line1_data ,update_address_line2_data ,update_address_line3_data ,update_street_name_data ,update_apartment_number_data ,update_city_name_data ,update_territory_name_data ,update_po_box_number_data ,update_postal_area_code_data ,update_po_box_data ,update_house_number_data ,update_street_complement_data ,update_city_complement_data ,update_building_data ,update_sub_building_data ,update_street_number_data ,update_opt_reason_data ,update_unsubscribe_url_data ,update_source_system_data]