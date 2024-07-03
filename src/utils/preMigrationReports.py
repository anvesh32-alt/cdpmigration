from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging
from helpers import constants,preMigrationReportQueries
import logging


def traits_validation(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    base_trait_reports_query = preMigrationReportQueries.trait_reports_query.format(region,\
                                market,region,market,region,region,region,region,region,region,region,market,region)
    logging.info("Traits Reports Query :",base_trait_reports_query)

    location = {'dbce-c360-segamerws-prod-35ac': 'us','dbce-c360-segglblws-prod-b902': 'us', 'dbce-c360-segamaws-prod-9669': 'us-east4'}
    try:
        if region in location:
            job_config = bigquery.QueryJobConfig()
            table_ref = client.dataset("DataTeam", project = region).table("traits" + mpn)
            job_config.destination = table_ref
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            query_job = client.query(
                base_trait_reports_query,
                location = location[region],
                job_config=job_config).result()
            _ = list(query_job)

            destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/traits_report/traits_{mpn}_result.csv"
            dataset_ref = client.dataset("DataTeam", project=region)
            table_ref = dataset_ref.table(f"traits{mpn}")
            _ = client.extract_table(
                table_ref,
                destination_uri,
                location = location[region]
                )
            _.result()
        
        client.delete_table(f"{region}.DataTeam.traits{mpn}", not_found_ok = True)
    except Exception as err:
        logging.info("Error Occured while running Trait Reports :",err)


def traits_validation_count(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    traits_reports_df = pd.read_csv(f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/traits_report/traits_{mpn}_result.csv")
    traits_reports_df['overall_count'] = 0
    traits_reports_df['non_blank_counts'] = 0
    traits_reports_df['latest_identifies'] = 'N/A'
    traits_reports_df['action'] = ''
    final_results = pd.DataFrame()

    for values in traits_reports_df.index:
        bq_trait_column = traits_reports_df["BQ_trait_column"][values]
        table_schema = traits_reports_df["table_schema"][values]
        marketing_program_number = traits_reports_df["marketing_program_number"][values]      
        base_traits_counts_query = preMigrationReportQueries.traits_counts_query.format(bq_trait_column,region,table_schema,bq_trait_column,\
                                    bq_trait_column,marketing_program_number,bq_trait_column,region,table_schema,marketing_program_number)
        try:
            traits_results = client.query(base_traits_counts_query).to_dataframe()
            traits_reports_df['overall_count'][values] = int(traits_results['non_blank_counts'][1])
            traits_reports_df['non_blank_counts'][values] = traits_results['non_blank_counts'][0]
            traits_reports_df['latest_identifies'][values] = traits_results['latest_identifies'][0]
            
        except Exception as err:
            logging.exception("Error occured while running for traits report:", err)

    final_results = traits_reports_df[traits_reports_df['overall_count'] > 0]
    dataset_name = "cdp_migration"
    table_name = f"traits_reports_for_{mpn}"
    destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/{mpn}/{table_name}.csv"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig( write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    _ = client.load_table_from_dataframe(final_results, table_ref,job_config=job_config).result()

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    dataset_ref = bigquery.DatasetReference(constants.PROJECT, dataset_name)
    table_ref = dataset_ref.table(table_name)
    _ = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-east4",
    ).result()


def events_property_validation(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        client_storage = storage.Client(constants.PROJECT)
        bucket = client_storage.get_bucket(constants.MIGRATION_BUCKET)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    try:
        def validateTrait(tname, main_table, dataset_id, project_id):

            base_sql_val_main = preMigrationReportQueries.sql_val_main.format(dataset_id,tname,main_table,region,market,tname,region,region,tname)
            return base_sql_val_main

        location = {'dbce-c360-segamerws-prod-35ac': 'us','dbce-c360-segglblws-prod-b902': 'us', 'dbce-c360-segamaws-prod-9669': 'us-east4'}
        if region in location:
            base_table_schema_query = preMigrationReportQueries.table_schema_query.format(region,location[region],market)

        events_reports_df = client.query(base_table_schema_query).to_dataframe()
        final_results = pd.DataFrame()
        count = 1
        for values in events_reports_df.iterrows():
            project_id = values[1]['project_id']
            dataset_id = values[1]['dataset']
            table_name = values[1]['table_name']
            count = count + 1
            base_events_list = preMigrationReportQueries.events_list
            text_main = base_events_list.format(project_id=project_id, dataset=dataset_id)
            events_results = client.query(text_main).to_dataframe()

            for values_main in events_results.iterrows():
                table_name = values_main[1]['event']
                main_table = values_main[1]['event_text']
                sql_event = validateTrait(
                    table_name, main_table, dataset_id, project_id)
                
                text_main = sql_event.format(dataset=dataset_id, project_id=project_id, table_name=table_name)
                client = bigquery.Client(constants.PROJECT)
                df_main = client.query(text_main).to_dataframe()
                final_results = final_results.append(df_main, ignore_index=True)
                final_results = final_results.drop_duplicates()
                final_results = final_results.reset_index(drop=True)

        bucket.blob(f"migration_reports/traits_report/events_{mpn}_result.csv").upload_from_string(final_results.to_csv(index=False), 'text/csv')
    
    except Exception as err:
        logging.exception("Error occured while running for events property report:", err)


def events_property_validation_count(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    final_results = pd.DataFrame()
    try:
        traits_reports_df = pd.read_csv(f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/traits_report/events_{mpn}_result.csv")
        traits_reports_df['count'] = 0
        traits_reports_df['latest_event'] = ''
        traits_reports_df['action'] = ''

        for values in traits_reports_df.index:
            base_events_counts_query = preMigrationReportQueries.event_property_counts_query.format(traits_reports_df["BQ_events_column"][values],region,traits_reports_df["dataset"][values],traits_reports_df["tablename"][values])
            query_job = client.query(base_events_counts_query).to_dataframe()
            traits_reports_df['count'][values] = int(query_job['count'][0])
            traits_reports_df['latest_event'][values] = query_job['latest_event'][0]
            
        final_results = traits_reports_df[traits_reports_df['count'] > 0]
        final_results = final_results.drop_duplicates(subset=['tablename','eventName','BQ_events_column']) 
        final_results = final_results.reset_index(drop=True)

    except Exception as err:
        logging.exception("Error occured while running for events report:", err)

    dataset_name = "cdp_migration"
    table_name = f"events_property_report_for_{mpn}"
    destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/{mpn}/{table_name}.csv"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    _ = client.load_table_from_dataframe(final_results, table_ref,job_config=job_config).result()

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    dataset_ref = bigquery.DatasetReference(constants.PROJECT, dataset_name)
    table_ref = dataset_ref.table(table_name)
    _ = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-east4",
    ).result()


def events_validation(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    final_results = pd.DataFrame()
    try:
        base_events_reports_query = preMigrationReportQueries.events_reports_query.format(market,region,market,region,region)

        logging.info("Events Reports Query : ",base_events_reports_query)
        df_query = client.query(base_events_reports_query).to_dataframe()
        df_query['action']=''
        df_query = df_query.drop_duplicates() 
        final_results = df_query.reset_index(drop=True)
        
    except Exception as err:
        logging.exception("Error occured while running for events report :", err)
    
    dataset_name = "cdp_migration"
    table_name = f"events_reports_for_{mpn}"
    destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/{mpn}/{table_name}.csv"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    _ = client.load_table_from_dataframe(final_results, table_ref,job_config=job_config).result()

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    dataset_ref = bigquery.DatasetReference(constants.PROJECT, dataset_name)
    table_ref = dataset_ref.table(table_name)
    _ = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-east4",
    ).result()


def mpn_validation(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    final_results = pd.DataFrame()
    try:
        base_invalid_mpn_reports_query = preMigrationReportQueries.invalid_mpn_reports_query.format(region,market,region,market,region)
        logging.info("MPN Validations Reports Query :",base_invalid_mpn_reports_query)
        mpn_validation_results = client.query(base_invalid_mpn_reports_query).to_dataframe()
        mpn_validation_results['action']=''
        mpn_validation_results = mpn_validation_results.drop_duplicates() 
        final_results = mpn_validation_results.reset_index(drop=True)

    except Exception as err:
        logging.exception("Error occured while running for mpn validation report:", err)

    dataset_name = "cdp_migration"
    table_name = f"mpn_validation_report_for_{mpn}"
    destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/{mpn}/{table_name}.csv"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    _ = client.load_table_from_dataframe(final_results, table_ref, job_config=job_config).result()

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    dataset_ref = bigquery.DatasetReference(constants.PROJECT, dataset_name)
    table_ref = dataset_ref.table(table_name)
    _ = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-east4",
    ).result()


def source_validation(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    final_results = pd.DataFrame()
    try:
        base_source_validation_reports_query = preMigrationReportQueries.source_validation_reports_query.format(region,market,region,region,region)
        logging.info("Sourceids Validations Reports Query :",base_source_validation_reports_query)
        source_validation_results = client.query(base_source_validation_reports_query).to_dataframe()
        source_validation_results['action'] = ''
        source_validation_results = source_validation_results.drop_duplicates() 
        final_results = source_validation_results.reset_index(drop=True)
        
    except Exception as err:
        logging.exception("Error occured while running for source validation report:", err)

    dataset_name = "cdp_migration"
    table_name = f"sourceids_validation_report_for_{mpn}"
    destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/{mpn}/{table_name}.csv"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    _ = client.load_table_from_dataframe(final_results, table_ref,job_config=job_config).result()

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    dataset_ref = bigquery.DatasetReference(constants.PROJECT, dataset_name)
    table_ref = dataset_ref.table(table_name)
    _ = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-east4",
    ).result()


def opts_validation(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    final_results = pd.DataFrame()
    try:
        base_opts_validation_reports_query = preMigrationReportQueries.opts_validation_reports_query.format(market,region,market,region,mpn)
        logging.info("Opts Reports Query :",base_opts_validation_reports_query)
        opts_validation_results = client.query(base_opts_validation_reports_query).to_dataframe()
        opts_validation_results['action'] = ''
        opts_validation_results = opts_validation_results.drop_duplicates()
        final_results = opts_validation_results.reset_index(drop=True)

    except Exception as err:
        logging.exception("Error occured while running for opts validation report:", err)

    dataset_name = "cdp_migration"
    table_name = f"opts_validation_report_for_{mpn}"
    destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/{mpn}/{table_name}.csv"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    _ = client.load_table_from_dataframe(final_results, table_ref,job_config=job_config).result()

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    dataset_ref = bigquery.DatasetReference(constants.PROJECT, dataset_name)
    table_ref = dataset_ref.table(table_name)
    _ = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-east4",
    ).result()


def ct_ecosystem_trait_validaition(**kwargs):

    region = kwargs['region']
    market = kwargs['market']
    mpn = kwargs['mpn']
    try:
        logging.info(f"Initializing Bigquery Client.")
        client = bigquery.Client(constants.PROJECT)
        logging.info("Initializing with Bigquery and bucket completed Successfully.")

    except Exception as err:
        logging.error(f"Unable to Initialize Bigquery client err: {err}")

    final_results = pd.DataFrame()
    try:
        base_ct_ecosystem_validation_reports_query = preMigrationReportQueries.ct_ecosystem_validation_reports_query.format(mpn)
        logging.info("CT Ecosystem Reports Query :",base_ct_ecosystem_validation_reports_query)
        ct_ecosystem_validation_results = client.query(base_ct_ecosystem_validation_reports_query).to_dataframe()
        ct_ecosystem_validation_results['action'] = ''
        ct_ecosystem_validation_results = ct_ecosystem_validation_results.drop_duplicates()
        final_results = ct_ecosystem_validation_results.reset_index(drop=True)

    except Exception as err:
        logging.exception("Error occured while running for ecosystem validation report:", err)

    dataset_name = "cdp_migration"
    table_name = f"ct_ecosystem_trait_validaition_report_for_{mpn}"
    destination_uri = f"gs://dbce-c360-isl-preprod-d942-migration-88fakc29/migration_reports/{mpn}/{table_name}.csv"
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig(write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE)
    _ = client.load_table_from_dataframe(final_results, table_ref,job_config=job_config).result()

    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.CSV
    dataset_ref = bigquery.DatasetReference(constants.PROJECT, dataset_name)
    table_ref = dataset_ref.table(table_name)
    _ = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="us-east4",
    ).result()
