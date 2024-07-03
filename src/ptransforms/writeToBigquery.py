import datetime

import apache_beam as beam
from helpers import constants
from dofns.extractFieldsToWriteToBigquery import ExtractFieldsToWriteToBigquery
from dofns import extractId


class WriteToBigquery(beam.PTransform):
    """
    Ptransform to write data to bigquery
    """

    def __init__(self, dataset_name, table_name, pipeline_name):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.pipeline_name = pipeline_name
        self.table_schema = 'raw_payload:STRING,event_type:STRING,marketing_program_number:STRING,' \
                            'source_id:STRING,message_id:STRING,payload_timestamp:STRING,loaded_date:TIMESTAMP'

    def expand(self, p_coll):
        (
                p_coll | f"{self.pipeline_name}ExtractFieldsForBQLoad" >> beam.ParDo(ExtractFieldsToWriteToBigquery())
                | f"Map{self.pipeline_name}Data" >> beam.Map(lambda x: x)
                | f"Write{self.pipeline_name}DataToBigquery" >> beam.io.WriteToBigQuery(
                                                            table=self.table_name,
                                                            dataset=self.dataset_name,
                                                            project=constants.PROJECT,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                            schema=self.table_schema)
        )


class WriteLatestEventToBigquery(beam.PTransform):
    """ PTransform to Write latest Events per each to Bigquery """

    def __init__(self, dataset_name, table_name, pipeline_name):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.pipeline_name = pipeline_name
        self.table_schema = 'raw_payload:STRING,loaded_date:TIMESTAMP'

    def expand(self, p_coll):
        (
            p_coll | f"MapUserIdAndEventTypeFor{self.pipeline_name}" >> beam.ParDo(extractId.ExtractIdForGroupByOnEvents())
                   | f"GroupRecordsOnUserAndEventTypeFor{self.pipeline_name}" >> beam.GroupByKey()
                   | f"FilterLatestRecordsFor{self.pipeline_name}" >> beam.ParDo(extractId.GetLatestRecordsForEachUser())
                   | f"MapLatestEventForBigquery{self.pipeline_name}" >> \
                                        beam.Map(lambda x: {'raw_payload': x, 'loaded_date': datetime.datetime.utcnow()})
                   | f"Write{self.pipeline_name}DataToBigquery" >> beam.io.WriteToBigQuery(
                                                            table=self.table_name,
                                                            dataset=self.dataset_name,
                                                            project=constants.PROJECT,
                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                                            schema=self.table_schema)
        )
