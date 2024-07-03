import logging
import apache_beam as beam
from google.cloud import spanner
from google.rpc.code_pb2 import OK
from apache_beam.io.gcp.experimental import spannerio

from helpers.constants import MDM_PRE_PROD_PROJECT_ID


class UpdateTransactionsForPgidOrTraceid(beam.DoFn):

    def __init__(self, instance_id, database_id, table_name, trace_id_update=None):
        self.project_id = MDM_PRE_PROD_PROJECT_ID
        self.instance_id = instance_id
        self.database_id = database_id
        self.table_name = table_name
        self.trace_id_update = trace_id_update

    def setup(self):
        try:
            logging.info(f"Initializing Client with Spanner with details, project id: {self.project_id},"
                         f"instance id: {self.instance_id}, database id : {self.database_id}, "
                         f"table name: {self.table_name}")
            self.spanner_client = spanner.Client(self.project_id)
            self.instance = self.spanner_client.instance(self.instance_id)
            self.database = self.instance.database(self.database_id)
            logging.info(f"Connection Initialized Successfully (UpdateTransactionsForPgidOrTraceid)"
                         f"for table: {self.table_name}")

        except Exception as err:
            logging.exception(f"Unable to establish connection for table: {self.table_name}, error: {err}")

    def process(self, element, *args, **kwargs):

        try:
            update_statements = []
            for record in element:
                if self.trace_id_update:
                    update_statement = (f"UPDATE {self.table_name} SET trace_id = '{record['trace_id']}-pp' "
                                        f"WHERE trace_id = '{record['trace_id']}'")
                else:
                    update_statement = (f"UPDATE {self.table_name} SET pg_id = '{record['new_pg_id']}' "
                                        f"WHERE pg_id = '{record['pre_prod_pg_id']}'")
                update_statements.append(update_statement)

            def update_records_in_pre_prod_spanner(transaction):
                status, row_cts = transaction.batch_update(update_statements)
                if status.code != OK:
                    # Do handling here.
                    # Note: the exception will still be raised when
                    # `commit` is called by `run_in_transaction`.
                    return update_statements
                logging.info("Executed {} SQL statements using Batch DML.".format(len(row_cts)))
            self.database.run_in_transaction(update_records_in_pre_prod_spanner)

        except Exception as err:
            logging.exception(f"Unable to execute the Update statement for table: {self.table_name}, err: {err}")


class CreateUpdateAndDeleteMutations(beam.DoFn):

    def __init__(self, instance_id, database_id, table_name, trace_id_update=None):
        self.project_id = MDM_PRE_PROD_PROJECT_ID
        self.instance_id = instance_id
        self.database_id = database_id
        self.table_name = table_name
        self.trace_id_update = trace_id_update

    def setup(self):
        try:
            logging.info(f"Initializing Client with Spanner with details, project id: {self.project_id},"
                         f"instance id: {self.instance_id}, database id : {self.database_id}, "
                         f"table name: {self.table_name}")
            self.spanner_client = spanner.Client(self.project_id)
            self.instance = self.spanner_client.instance(self.instance_id)
            self.database = self.instance.database(self.database_id)
            self.column_names = self.get_columns_names()
            logging.info(f"Connection Initialized Successfully (CreateUpdateAndDeleteMutations), table: {self.table_name}, "
                         f"column names: {self.column_names}")

        except Exception as err:
            logging.exception(f"Unable to establish connection for table: {self.table_name}, error: {err}")

    def get_columns_names(self):
        # Extract the column names

        table = self.database.table(self.table_name)
        column_names = [column.name for column in table.schema]
        if self.table_name == 'profiles':
            self_generated_columns = ['email_sha256_hash', 'email_sha1_hash', 'normalized_email_sha256_hash',
                                      'normalized_email_sha1_hash', 'phone_number_sha256_hash',
                                      'phone_number_sha1_hash', 'normalized_phone_number_sha256_hash',
                                      'normalized_phone_number_sha1_hash', 'phone_number2_sha256_hash',
                                      'phone_number2_sha1_hash', 'normalized_phone_number2_sha256_hash',
                                      'normalized_phone_number2_sha1_hash']

            column_names = [column.name for column in table.schema if column.name not in self_generated_columns]
        elif self.table_name == 'profile_events' and self.trace_id_update:
            self_generated_columns = ['trace_id_pg_id']
            column_names = [column.name for column in table.schema if column.name not in self_generated_columns]
        return column_names

    def process(self, element, *args, **kwargs):

        sql_query = self.form_sql_query_for_bulk_read(element)

        try:
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(sql_query)

                for row in results:
                    result = dict(zip(self.column_names, row))
                    delete_mutation = self.create_delete_mutation(result)
                    if self.trace_id_update:
                        updated_pg_id_data = self.append_preprod_to_trace_ids(result)
                    else:
                        updated_pg_id_data = self.update_new_pg_id(element, result)
                    insert_mutation = self.create_insert_mutation(updated_pg_id_data, self.column_names)
                    yield beam.pvalue.TaggedOutput('delete_mutation', delete_mutation)
                    yield beam.pvalue.TaggedOutput('insert_mutation', insert_mutation)
        except Exception as err:
            logging.exception(f"Unable to create Delete/Insert Mutation for table: {self.table_name}, err: {err}")

    def form_sql_query_for_bulk_read(self, data):

        if self.trace_id_update:
            list_of_trace_ids = [trace_id['trace_id'] for trace_id in data]
            sql_query = f'SELECT * FROM {self.table_name} WHERE trace_id in {tuple(list_of_trace_ids)}'
        else:
            list_of_pg_id = [pg_id['pre_prod_pg_id'] for pg_id in data]
            sql_query = f'SELECT * FROM {self.table_name} WHERE pg_id in {tuple(list_of_pg_id)}'

        return sql_query

    def create_delete_mutation(self, data):

        if self.table_name == 'profiles':
            spanner_delete_mutations = spannerio.WriteMutation.delete(table=self.table_name,
                                                                      keyset=spannerio.KeySet(
                                                                          keys=[[data['pg_id'],
                                                                                 data['marketing_program_number'],
                                                                                 data['source_id']]]))
        elif self.table_name == 'profile_events' and self.trace_id_update:
            spanner_delete_mutations = spannerio.WriteMutation.delete(table=self.table_name,
                                                                      keyset=spannerio.KeySet(
                                                                          keys=[[data['trace_id']]]))

        else:
            spanner_delete_mutations = spannerio.WriteMutation.delete(table=self.table_name,
                                                                      keyset=spannerio.KeySet(
                                                                          keys=[[data['canonical_pg_id'], data['pg_id'],
                                                                                 data['trace_id']]]))

        return spanner_delete_mutations

    def create_insert_mutation(self, data, column_names):
        spanner_insert_mutations = spannerio.WriteMutation.insert_or_update(table=self.table_name,
                                                                            columns=tuple(column_names),
                                                                            values=[tuple(data.values())])

        return spanner_insert_mutations

    def update_new_pg_id(self, pg_id_mapping_data, data):

        for pg_id_mapping in pg_id_mapping_data:
            if data['pg_id'] == pg_id_mapping['pre_prod_pg_id']:
                data['pg_id'] = pg_id_mapping['new_pg_id']
                if self.table_name in ['id_aliases', 'profile_aliases']:
                    data['canonical_pg_id'] = pg_id_mapping['new_pg_id']
                    return data
        return data

    def append_preprod_to_trace_ids(self, data):
        data['trace_id'] = f"{data['trace_id']}-pp"
        return data


class CreateMutationsForProfileConsents(beam.DoFn):
    """
    Extension of beam DoFn to create mutation group for spanner table
    """

    def __init__(self, destination_table):
        self.destination_table = destination_table

    def process(self, element):
        yield spannerio.WriteMutation.update(table=self.destination_table,
                                             columns=('id_value', 'service_name', 'marketing_program_number',
                                                      'contact_point_type_name', 'opt_date', 'opt_status'),
                                             values=[(tuple(element.values()))])
