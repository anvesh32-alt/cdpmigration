import logging
from utils import readConfig





class GetProfileUsersQuery():

    def __init__(self, mpn):
        """
        Constructs all the necessary attributes for the profileusers object.
        :param mpn : str
            marketing program number
        """

        self.mpn = mpn
    def union_query(self,project,persona_list):

        #sql_2=f'''( SELECT user_id FROM `{project}.{persona}` UNION ALL )'''
        query_list=[]
        for persona in persona_list:
            sql=f''' SELECT user_id,previous_id FROM `{project}.{persona}.aliases` UNION ALL'''
            query_list.append(sql)
        sql_2=' '.join(query_list)
        sql_2= sql_2[0:-9]
        # print(sql_2)
        return sql_2

    def main_query(self,project,dataset,country_abbreviation):
        sql_1=f'''
            SELECT
            *
            FROM (
            SELECT
                DISTINCT(user_id) as userId,
            marketing_program_number as marketingProgramNumber,
            consumer_id as consumerId,
            source_id as sourceId,
            TO_HEX(SHA256(LOWER(email))) AS emailsha256hash,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',original_timestamp) AS original_timestamp
            FROM
                `{project}.{dataset}.identifies`
            WHERE
                marketing_program_number NOT IN (
                SELECT
                DISTINCT a.marketing_program_number
                FROM
                `{project}.{dataset}.canonical_user_ctas` AS a
                LEFT JOIN
                `{project}.CDS.mpn` AS b
                ON
                CAST(b.marketingProgramNumber AS string) =CAST(a.marketing_program_number AS string)
                WHERE
                marketing_program_number IS NOT NULL
                AND TRIM(marketing_program_number) NOT LIKE ""
                AND marketingProgramName NOT LIKE ('%{country_abbreviation}%'))
                UNION ALL

                select DISTINCT(user_id) as userId,
                marketing_program_number as marketingProgramNumber,
                consumer_id as consumerId,
                source_id as sourceId,
                TO_HEX(SHA256(LOWER(email))) AS emailsha256hash,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',original_timestamp) AS original_timestamp
                from `{project}.{dataset}.canonical_user_ctas` 
            UNION ALL
            SELECT
                DISTINCT a.previous_id,
                marketing_program_number,
                consumer_id,
                source_id,
                TO_HEX(SHA256(LOWER(email))) AS email_sha256_hashh,
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',c.original_timestamp) AS original_timestamp
            FROM ('''
        

        sql_2=f''') a JOIN
            `{project}.{dataset}.canonical_user_ctas` c
            ON
            c.user_id=a.user_id ) where original_timestamp is not null
            ORDER BY
            original_timestamp asc'''
        
        return sql_1,sql_2
        
    def get_query(self):
        """
        Calls the utility to read the config file and returns the formatted query to be used
        to get all the pprofile users
        :return str: Formatted final query with tables values
        """
        get_config = readConfig.ReadConfig(self.mpn).read_config_file()
        region=get_config['region']
        get_pconfig = readConfig.ReadConfig(self.mpn).read_persona_config_file(region)

        project = get_config['project']
        dataset_persona = get_config['dataset_persona']
        country_abbreviation = get_config['country']
        #print(project,dataset_persona)

        sql_main1,sql_main3 = self.main_query(project,dataset_persona,country_abbreviation)
        sql_main2=self.union_query(project,get_pconfig['personas'])

        users_query=sql_main1+sql_main2+sql_main3
        logging.info(f"Bigquery query to get profile users, query:{users_query}")
        #print(users_query)
        return users_query
