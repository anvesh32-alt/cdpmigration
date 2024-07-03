import apache_beam as beam
from helpers import constants


class ReadValuesForExternalIds(beam.PTransform):
    """
    Ptransform to read data for External id fields
    """

    def __init__(self, email_query, source_id_query, mpn_query):
        self.email_query = email_query
        self.source_id_query = source_id_query
        self.mpn_query = mpn_query

    def expand(self, p_coll):

        get_email_from_personas = (p_coll | f"ReadEmailFromPersonasTable" >> beam.io.ReadFromBigQuery(
                                                                                            query=self.email_query,
                                                                                            use_standard_sql=True,
                                                                                            project=constants.PROJECT,
                                                                                            gcs_location=constants.
                                                                                            TEMP_LOCATION)
                                   )

        get_source_id_from_personas = (p_coll | f"ReadSourceIdFromPersonasTable" >> beam.io.ReadFromBigQuery(
                                                                                            query=self.source_id_query,
                                                                                            use_standard_sql=True,
                                                                                            project=constants.PROJECT,
                                                                                            gcs_location=constants.
                                                                                            TEMP_LOCATION)
                                       )

        get_mpn_from_personas = (p_coll | f"ReadMpnFromPersonasTable" >> beam.io.ReadFromBigQuery(
                                                                                            query=self.mpn_query,
                                                                                            use_standard_sql=True,
                                                                                            project=constants.PROJECT,
                                                                                            gcs_location=constants.
                                                                                            TEMP_LOCATION)
                                 )

        p_coll_bigquery_data = ((get_email_from_personas, get_source_id_from_personas, get_mpn_from_personas)
                                | "MergeBigQueryData" >> beam.Flatten()
                                | "MapBQData" >> beam.Map(lambda x: (x['user_id'], x))
                                )

        return p_coll_bigquery_data
