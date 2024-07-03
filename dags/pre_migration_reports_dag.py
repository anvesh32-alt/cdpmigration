from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
import pendulum
from utils.preMigrationReports import traits_validation,traits_validation_count,events_property_validation,events_property_validation_count,\
                                        events_validation,mpn_validation,source_validation,opts_validation,ct_ecosystem_trait_validaition


default_args = {
    "owner": "CDP2.0 Migration",
    "retries": 2
}


def create_dynamic_dag(dag_id, job_config):

    logging.info(f"Starting the DAG with config: {job_config}")

    dag = models.DAG(
        dag_id = dag_id,
        default_args = default_args,
        start_date = pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup = False,
        schedule_interval = "@once",
        tags=['Pre Migration Reports'])
    
    with dag:

        start_process = DummyOperator(task_id="start_process")

        traits_validation_task = PythonOperator(
            task_id = f"traits_validation_{job_config['market']}",
            python_callable = traits_validation,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        traits_validation_count_task = PythonOperator(
            task_id = f"traits_validation_count_{job_config['market']}",
            python_callable = traits_validation_count,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        events_property_validation_task = PythonOperator(
            task_id = f"events_property_validation_{job_config['market']}",
            python_callable = events_property_validation,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        events_property_validation_count_task = PythonOperator(
            task_id = f"events_property_validation_count_{job_config['market']}",
            python_callable = events_property_validation_count,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        events_validation_task = PythonOperator(
            task_id = f"events_validation_{job_config['market']}",
            python_callable = events_validation,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        mpn_validation_task = PythonOperator(
            task_id = f"mpn_validation_{job_config['market']}",
            python_callable = mpn_validation,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        source_validation_task = PythonOperator(
            task_id = f"source_validation_{job_config['market']}",
            python_callable = source_validation,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        opts_validation_task = PythonOperator(
            task_id = f"opts_validation_{job_config['market']}",
            python_callable = opts_validation,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )
        ct_ecosystem_trait_validaition_task = PythonOperator(
            task_id = f"ct_ecosystem_trait_validaition_{job_config['market']}",
            python_callable = ct_ecosystem_trait_validaition,
            op_kwargs = {"region" : job_config["region"],"market" : job_config["market"],"mpn" : job_config["mpn"]},
            dag = dag
        )

        end_process = DummyOperator(task_id="end_task")

        start_process >> traits_validation_task >> traits_validation_count_task >> end_process
        start_process >> events_property_validation_task >> events_property_validation_count_task >> end_process
        start_process >> events_validation_task >> end_process
        start_process >> mpn_validation_task >> end_process
        start_process >> source_validation_task >> end_process
        start_process >> opts_validation_task >> end_process
        start_process >> ct_ecosystem_trait_validaition_task >> end_process
        
    return dag


job_configs = [
    {"mpn" : "524", "region" : "dbce-c360-segglblws-prod-b902", "market" : "personas_bel_oral_b"}
]

for job_config in job_configs:
    dag_id = f"pre_migration_reports_dag_for_{job_config['mpn']}"
    globals()[dag_id] = create_dynamic_dag(dag_id,
                                           job_config,
                                           )
