import logging

import apache_beam as beam
from google.cloud import spanner
from google.cloud.spanner_v1.database import BatchSnapshot


class CreateSpannerReadPartitions(beam.DoFn):
    """
    Extension of beam DoFn to create read partition to read data from spanner in batches.
    """

    def __init__(self, project_id, instance_id, database_id):
        """
        Constructs all the necessary attributes for the CreateSpannerReadPartitions object.
        :param project_id: str - name of the project to connect
        :param instance_id: str - name of the spanner instance to connect
        :param database_id: str - name of the spanner database to connect
        """

        self.project = project_id
        self.instance = instance_id
        self.database = database_id
        self.snapshot = None
        self.snapshot_dict = None

    def setup(self):
        """ Creates a connection with the spanner instance """

        logging.info("Initializing Spanner Client in CreateSpannerReadPartitions class")

        spanner_client = spanner.Client(self.project)
        spanner_instance = spanner_client.instance(self.instance)
        spanner_database = spanner_instance.database(self.database)
        self.snapshot = spanner_database.batch_snapshot()
        self.snapshot_dict = self.snapshot.to_dict()

    def process(self, element):
        """
        Creates a partition to read data from spanner
        :param element: list of sql queries
        :return : partitions of spanner for the next step in the pipeline
        """

        partitions = self.snapshot.generate_query_batches

        for partition in partitions(element):
            yield {"partitions": partition, "transaction_info": self.snapshot_dict}


class ReadSpannerPartitions(beam.DoFn):
    """
    Extension of beam DoFn to read data from spanner.
    """

    def __init__(self, project_id, instance_id, database_id):
        self.project = project_id
        self.instance = instance_id
        self.database = database_id
        self.snapshot = None
        self.snapshot_dict = None

    def setup(self):

        logging.info("Initializing Spanner Client in ReadSpannerPartitions class")

        spanner_client = spanner.Client(self.project)
        spanner_instance = spanner_client.instance(self.instance)
        self.spanner_database = spanner_instance.database(self.database)
        self.snapshot = self.spanner_database.batch_snapshot()
        self.snapshot_dict = self.snapshot.to_dict()

    def process(self, element):
        """
        Read data from the spanner tables
        :param element: spanner partitions to read data from
        :return: pcollection of database values for next step in the pipeline
        """

        self.snapshot = BatchSnapshot.from_dict(self.spanner_database, element['transaction_info'])

        read_batches = self.snapshot.process_query_batch
        for row in read_batches(element['partitions'], timeout=300000):  # timeout of 5 minutes
            yield row

    def teardown(self):
        self.snapshot.close()


class GenerateSqlQueryBatches:
    """
    Runs a sql query and returns list of sql queries to be used to read data in batches based on timestamp, mpn
    and source id.
    """

    def __init__(self, project_id, instance_id, database_id):
        self.project = project_id
        self.instance = instance_id
        self.database = database_id
        self.db_conn = self.db_connection()

    def db_connection(self):
        spanner_client = spanner.Client(self.project)
        instance = spanner_client.instance(self.instance)
        database = instance.database(self.database)

        return database

    def get_batches_from_spanner(self, sql_query, base_sql_query):
        """
        :param sql_query: str
            eg : SELECT marketing_program_number, TIMESTAMP_TRUNC(loaded_date, HOUR, "UTC") AS utc,  source_id
                 FROM  staging_traces GROUP BY  1,2,3
        :param base_sql_query: str
            eg : SELECT  * FROM staging_traces WHERE marketing_program_number IN ({})
                 AND TIMESTAMP_TRUNC(loaded_date, HOUR, "UTC") IN ("{}") AND source_id={}
        :return: list of queries to be used to read data from spanner
        """

        with self.db_conn.snapshot() as snapshot:
            base_sql_for_batches = []
            try:
                results = snapshot.execute_sql(sql_query)
                for rows in results:
                    _ = base_sql_query.format(rows[0], (rows[1].strftime('%Y-%m-%dT%H:%M:%S') + 'Z'), rows[2])

                    base_sql_for_batches.append(_)

            except Exception as err:
                logging.exception(f"Error while generating SQL query to read in batches err: {err}")

        return base_sql_for_batches


