import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.experimental.spannerio import WriteToSpanner

from dofns.updateRecordsInPreprodSpanner import (UpdateTransactionsForPgidOrTraceid, CreateUpdateAndDeleteMutations,
                                                 CreateMutationsForProfileConsents)
from helpers import constants
from helpers.bigqueryDatasetsConfig import pg_id_update_table_config


def run(mpn, pipeline_name, pipeline_args=None):
    """
    Main Entry point of the Pg ID/Trace ID update Dataflow pipeline
    pg_id_update : Updates the tables with new Pg ID in spanner pre-prod tables
    trace_id_update: For each trace_id append -preprod and update the spanner pre-prod spanner tables.
    profile_consents_update: Updates the opt_date and opt_status in profile consents
    """

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    logging.info(f"Starting pipeline with Options, mpn: {mpn}, pipeline_name: {pipeline_name}")

    # get the table name to read either for pg_id/trace_id update.
    table_name = pg_id_update_table_config[mpn][pipeline_name]
    format_pipeline_name = ''.join(x.capitalize() or '_' for x in pipeline_name.split('_'))
    trace_id_update_check = False

    # query to read pre_prod_pg_id with new_pg_id from production for lookup1 and lookup2
    query = f"""SELECT DISTINCT pre_prod_pg_id, new_pg_id FROM 
    `{constants.PROJECT}.{constants.PG_ID_UPDATE_DATASET}.{table_name[0]}` WHERE new_pg_id IS NOT NULL
    UNION ALL 
    SELECT DISTINCT pre_prod_pg_id, new_pg_id FROM 
    `{constants.PROJECT}.{constants.PG_ID_UPDATE_DATASET}.{table_name[1]}` WHERE new_pg_id IS NOT NULL
    """

    # query to update trace_id in spanner tables
    if pipeline_name == 'trace_id_update':
        query = f"SELECT * FROM `{constants.PROJECT}.{constants.PG_ID_UPDATE_DATASET}.{table_name}`"
        trace_id_update_check = True

    # query to update opt_date and opt_status in profile consents
    elif pipeline_name == 'profile_consents_update':
        query = f"SELECT * FROM `{constants.PROJECT}.{constants.PG_ID_UPDATE_DATASET}.{table_name}`"

    logging.info(f"Reading from Bigquery, query: {query}")
    with beam.Pipeline(options=pipeline_options) as pipeline:
        get_data_from_bq = pipeline | f"ReadDataFrom{table_name}Table" >> beam.io.ReadFromBigQuery(
                                                                                           query=query,
                                                                                           use_standard_sql=True,
                                                                                           project=constants.PROJECT,
                                                                                           gcs_location=constants.
                                                                                           TEMP_LOCATION
        )

        if not pipeline_name == 'profile_consents_update':
            batch_input_bq_data = get_data_from_bq | "BatchElements" >> beam.BatchElements(min_batch_size=100,
                                                                                           max_batch_size=1000)

            # update pg_id for id_graphs
            id_graphs = batch_input_bq_data | f"{format_pipeline_name}ForIdGraphs" >> beam.ParDo(
                UpdateTransactionsForPgidOrTraceid(instance_id='cdp-ids', database_id='cdp-ids', table_name='id_graphs',
                                                   trace_id_update=trace_id_update_check))

            # update pg_id for profile_traits
            profile_traits = batch_input_bq_data | f"{format_pipeline_name}ForProfileTraits" >> beam.ParDo(
                UpdateTransactionsForPgidOrTraceid(instance_id='cdp-profiles', database_id='cdp-profiles',
                                                   table_name='profile_traits', trace_id_update=trace_id_update_check))

            # update pg_id for profile_dependent_traits
            profile_dependent_traits = batch_input_bq_data | f"{format_pipeline_name}ForProfileDependentTraits" >> beam.ParDo(
                UpdateTransactionsForPgidOrTraceid(instance_id='cdp-profiles', database_id='cdp-profiles',
                                                   table_name='profile_dependent_traits',
                                                   trace_id_update=trace_id_update_check))

            # update pg_id for profile_events
            if pipeline_name == 'pg_id_update':
                profile_events = batch_input_bq_data | "PgIdUpdateForProfileEvents" >> beam.ParDo(
                    UpdateTransactionsForPgidOrTraceid(instance_id='cdp-profiles', database_id='cdp-profiles',
                                                       table_name='profile_events',
                                                       trace_id_update=trace_id_update_check))

            elif pipeline_name == 'trace_id_update':
                delete_mutation_from_profile_events, insert_mutation_from_profile_events = \
                    batch_input_bq_data | "ReadDataFromProfileEvents" >> beam.ParDo(
                        CreateUpdateAndDeleteMutations(instance_id='cdp-profiles', database_id='cdp-profiles',
                                                       table_name='profile_events', trace_id_update=True)).with_outputs(
                                                                                                 'delete_mutation',
                                                                                                 'insert_mutation')
                _ = delete_mutation_from_profile_events | "DeleteDataFromProfileEvents" >> WriteToSpanner(
                    project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-profiles',
                    database_id='cdp-profiles', max_number_rows=1000, max_batch_size_bytes=100000000)
                _ = insert_mutation_from_profile_events | "UpdateTraceIdsInProfileEvents" >> WriteToSpanner(
                    project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-profiles',
                    database_id='cdp-profiles', max_number_rows=1000, max_batch_size_bytes=100000000)

            # update pg_id with delete/insert for profile_aliases
            delete_mutation_from_profile_aliases, insert_mutation_from_profile_aliases = \
                batch_input_bq_data | "ReadDataFromProfileAliases" >> beam.ParDo(
                    CreateUpdateAndDeleteMutations(instance_id='cdp-profiles', database_id='cdp-profiles',
                                                   table_name='profile_aliases', trace_id_update=trace_id_update_check)
                ).with_outputs('delete_mutation', 'insert_mutation')

            _ = insert_mutation_from_profile_aliases | f"{format_pipeline_name}ForProfileAliases" >> WriteToSpanner(
                project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-profiles', database_id='cdp-profiles',
                max_number_rows=1000, max_batch_size_bytes=100000000)

            _ = delete_mutation_from_profile_aliases | "DeleteDataFromProfileAliases" >> WriteToSpanner(
                project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-profiles', database_id='cdp-profiles',
                max_number_rows=1000, max_batch_size_bytes=100000000)

            # update pg_id with delete/insert for id_aliases
            delete_mutation_from_id_aliases, insert_mutation_from_id_aliases = \
                batch_input_bq_data | "ReadDataFromIDAliases," >> beam.ParDo(
                    CreateUpdateAndDeleteMutations(instance_id='cdp-ids', database_id='cdp-ids',
                                                   table_name='id_aliases', trace_id_update=trace_id_update_check)
                ).with_outputs('delete_mutation', 'insert_mutation')

            _ = insert_mutation_from_id_aliases | f"{format_pipeline_name}ForIDAliases" >> WriteToSpanner(
                project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-ids', database_id='cdp-ids',
                max_number_rows=1000, max_batch_size_bytes=100000000)

            _ = delete_mutation_from_id_aliases | "DeleteDataFromIDAliases" >> WriteToSpanner(
                project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-ids', database_id='cdp-ids',
                max_number_rows=1000, max_batch_size_bytes=100000000)

            # update pg_id with delete/insert for profiles
            if pipeline_name == 'pg_id_update':

                delete_mutation_from_profiles, insert_mutation_from_profiles = \
                    batch_input_bq_data | "ReadDataFromProfiles," >> beam.ParDo(
                        CreateUpdateAndDeleteMutations(instance_id='cdp-ids', database_id='cdp-ids',
                                                       table_name='profiles', trace_id_update=trace_id_update_check)
                    ).with_outputs('delete_mutation', 'insert_mutation')
                _ = insert_mutation_from_profiles | "UpdatePgIdForProfiles" >> WriteToSpanner(
                    project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-ids', database_id='cdp-ids',
                    max_number_rows=1000, max_batch_size_bytes=100000000)
                _ = delete_mutation_from_profiles | "DeleteDataFromProfiles" >> WriteToSpanner(
                    project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-ids', database_id='cdp-ids',
                    max_number_rows=1000, max_batch_size_bytes=100000000)

            elif pipeline_name == 'trace_id_update':
                profiles = batch_input_bq_data | "UpdateTraceIdForProfiles" >> beam.ParDo(
                    UpdateTransactionsForPgidOrTraceid(instance_id='cdp-ids', database_id='cdp-ids',
                                                       table_name='profiles', trace_id_update=True))

        # update opt_date and opt_status for profile_consents
        elif pipeline_name == 'profile_consents_update':
            mutation_records = get_data_from_bq | 'CreateMutationGroupsForProfileConsents' >> \
                               beam.ParDo(CreateMutationsForProfileConsents(destination_table='profile_consents'))

            mutation_records | "UpdateOptDateAndOptStatusInProfileConsents" >> WriteToSpanner(
                project_id=constants.MDM_PRE_PROD_PROJECT_ID, instance_id='cdp-consents', database_id='cdp-consents',
                max_number_rows=1000, max_batch_size_bytes=100000000)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--mpn",
                        dest="mpn",
                        required=True,
                        help="Mpn(Marketing program number) for which pipeline should run."
                        )
    parser.add_argument("--pipeline_name",
                        dest="pipeline_name",
                        required=True,
                        help="Pipline name you want the pipeline to updates the tables either pg_id_update or "
                             "trace_id_update"
                        )
    known_args, pipeline_args = parser.parse_known_args()
    run(known_args.mpn,
        known_args.pipeline_name,
        pipeline_args)
