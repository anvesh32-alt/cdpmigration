import apache_beam as beam
from dofns import extractId, addFieldToExternalIds, addConsumerIdToExternalIds

from utils.getValueFromTraitsOrProperties import flatten_records


class ValidateAndUpdateExternalIds(beam.PTransform):

    def __init__(self, country_code, pipeline_name):
        self.country_code = country_code
        self.pipeline_name = pipeline_name

    def expand(self, p_coll):
        """
        Ptransform to update external id fields
        """

        p_coll_gcs_data, p_coll_bigquery_data = p_coll

        map_user_id_p_coll_gcs_data = p_coll_gcs_data | f'ExtractUserIdFrom{self.pipeline_name}Data' >> \
                                                        beam.ParDo(extractId.ExtractId())
        updated_payload = (
                ({'map_gcs_data': map_user_id_p_coll_gcs_data, 'map_bq_data': p_coll_bigquery_data})
                | f'Group{self.pipeline_name}RecordsOnUserId' >> beam.CoGroupByKey()
                | f'Flatten{self.pipeline_name}Records' >> beam.FlatMap(flatten_records)
                | f"AddFieldsTo{self.pipeline_name}" >> beam.ParDo(addFieldToExternalIds.AddFieldToExternalIds(
                                                                                                    self.country_code))
                | f"AddConsumerIdTo{self.pipeline_name}" >> beam.ParDo(
                                                                addConsumerIdToExternalIds.AddConsumerIdInExternalIds())
        )

        return updated_payload
