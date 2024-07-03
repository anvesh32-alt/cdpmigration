import logging
from utils import readConfig


class BaseQueries:
    """
    Base Query to be executed to find the retention out users, invalid mpn
    """

    _base_retention_out_query = """
    SELECT distinct user_id FROM (select i.user_id ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)
    [OFFSET (0)] as email ,ARRAY_AGG(i.phone_number IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] 
    as phone_number ,ARRAY_AGG(i.address_line1 IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] as 
    address_line1 ,ARRAY_AGG(i.address_line2 IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] 
    as address_line2 ,ARRAY_AGG(i.address_line3 IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] 
    as address_line3 FROM `{}.{}.{}` i 
    where context_personas_computation_class is null group by  1) 
    where    email=' '    AND phone_number=' ' and address_line1=' ' AND cast(address_line2 as string)=' ' and address_line3=' '
    """

    _base_invalid_mpn_query = """
    SELECT DISTINCT marketing_program_number FROM `{}.{}.{}` WHERE trim(marketing_program_number) NOT IN (SELECT
    DISTINCT cast(identifies.marketing_program_number as string) as mpn FROM
    `{}.{}.{}` AS identifies LEFT JOIN `{}.CDS.mpn` AS crs ON 
    CAST(crs.marketingProgramNumber AS string) =CAST(identifies.marketing_program_number AS string) WHERE
    trim(CAST(marketing_program_number AS string)) <> '' AND CAST(marketing_program_number AS string) IS NOT NULL )
    and trim(CAST(marketing_program_number AS string)) <> ''
    """

    _base_get_email_from_identifies_table = """
        select user_id, email, timestamp from `{}.{}.{}` where email is not null 
        and email <> "" and email <> " " QUALIFY ROW_NUMBER() OVER 
        (PARTITION BY user_id ORDER BY timestamp DESC) = 1  
    """

    _base_get_source_id = """
     SELECT  user_id, source_id as sourceId FROM `{}.{}.{}`
     WHERE source_id IS NOT NULL AND source_id <> " " QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) = 1
     """

    _base_get_mpn_query = """
    SELECT user_id, marketing_program_number AS marketingProgramNumber FROM
    `{}.{}.{}` WHERE marketing_program_number IS NOT NULL AND marketing_program_number <> ""
    AND marketing_program_number <> " " AND marketing_program_number NOT IN 
    (SELECT DISTINCT marketing_program_number 
    FROM `{}.{}.{}` WHERE trim(marketing_program_number) NOT IN 
    (SELECT DISTINCT cast(identifies.marketing_program_number as string) as mpn FROM
    `{}.{}.{}` AS identifies LEFT JOIN `{}.CDS.mpn` AS crs ON 
    CAST(crs.marketingProgramNumber AS string) =CAST(identifies.marketing_program_number AS string) 
    WHERE CAST(marketing_program_number AS string) IS NOT NULL) and 
    trim(CAST(marketing_program_number AS string)) <> '')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) = 1
    """

    _base_partial_retention_out_query = """
        SELECT
        distinct user_id FROM `{}.{}.{}`
        where (email like (' ') or phone_number like (' ')) 
        and (address_line1 is null or address_line1 not like (' '))
        """


class ReformatBaseQueries(BaseQueries):

    def __init__(self, mpn):
        """
        Constructs all the necessary attributes for the GetRetentionOutUsersQuery object.
        :param mpn : str
            marketing program number
        """

        self.mpn = mpn
        self.get_config = readConfig.ReadConfig(self.mpn).read_config_file()
        self.project = self.get_config['project']
        self.dataset_retention = self.get_config['dataset_retention']
        self.table_identifies = self.get_config['table_identifies']
        self.dataset_persona = self.get_config['dataset_persona']
        self.table_ctas = self.get_config['table_ctas']
        self.country_abbreviation = self.get_config['country']

    def get_retention_out_query(self):
        """
        Returns the formatted query to be used to get all the retention out users
        :return str: Formatted final query with tables values
        """

        base_retention_out_query = BaseQueries._base_retention_out_query

        retention_out_query = base_retention_out_query.format(
            self.project, self.dataset_persona, self.table_identifies
        )
        logging.info(f"Bigquery query to get retention users, query:{retention_out_query}")
        return retention_out_query

    def get_invalid_mpn_query(self):
        """
        Returns the formatted query to be used to get all the invalid mpns
        :return str: Formatted final query with tables values
        """

        base_invalid_mpn_query = BaseQueries._base_invalid_mpn_query

        invalid_mpn_query = base_invalid_mpn_query.format(
            self.project, self.dataset_persona, self.table_identifies,
            self.project, self.dataset_persona, self.table_ctas,
            self.project
            )

        logging.info(f"Bigquery query to get invalid mpns, query:{invalid_mpn_query}")
        return invalid_mpn_query

    def get_email_from_identifies_table(self):
        """
        Returns the formatted query to be used to get all the email id from identifies table
        :return str: Formatted final query with tables values
        """

        base_email_query = BaseQueries._base_get_email_from_identifies_table

        email_query_for_external_id = base_email_query.format(
            self.project, self.dataset_persona, self.table_identifies)

        logging.info(f"Bigquery query to get Email Id's from Identifies tables, query:{email_query_for_external_id}")
        return email_query_for_external_id

    def get_source_id_from_identifies_table(self):
        """
        Returns the formatted query to be used to get all the source id from identifies table
        :return str: Formatted final query with tables values
        """

        base_source_id_query = BaseQueries._base_get_source_id

        source_id_query_for_external_id = base_source_id_query.format(
            self.project, self.dataset_persona, self.table_identifies
        )

        logging.info(f"Bigquery query to get Source Id's from Identifies tables, " f"query:{source_id_query_for_external_id}")

        return source_id_query_for_external_id

    def get_mpn_from_identifies_table(self):
        """
        Returns the formatted query to be used to get all the mpn from identifies table
        :return str: Formatted final query with tables values
        """

        base_mpn_id_query = BaseQueries._base_get_mpn_query

        mpn_query_for_external_id = base_mpn_id_query.format(
            self.project, self.dataset_persona, self.table_identifies,
            self.project, self.dataset_persona, self.table_identifies,
            self.project, self.dataset_persona, self.table_ctas, 
            self.project
        )

        logging.info(f"Bigquery query to get Source Id's from Identifies tables, query:{mpn_query_for_external_id}")
        return mpn_query_for_external_id

    def get_country_code(self):

        return self.country_abbreviation

    def get_partial_retention_query(self):

        base_partial_retention_out_query = BaseQueries._base_partial_retention_out_query

        partial_retention_out_query = base_partial_retention_out_query.format(
            self.project, self.dataset_retention, self.table_identifies
        )
        logging.info(f"Bigquery query to get partial retention users, query:{partial_retention_out_query}")
        return partial_retention_out_query
