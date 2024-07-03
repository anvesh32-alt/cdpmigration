from google.cloud import bigquery

client = bigquery.Client()


class SplitterQueries():
    def __init__(self):
        pass

    def queries_list(self):
        click_stream_query = """
        SELECT lower(eventName) as eventName
            FROM ( SELECT eventName,eventCategory,eventNameAlias,gacaEventName FROM
            `dbce-c360-segamerws-prod-35ac.CDS.event`
            UNION ALL
            SELECT value, eventCategory,eventNameAlias,gacaEventName FROM
            `dbce-c360-segamerws-prod-35ac.CDS.event`,
            UNNEST(SPLIT(eventNameAlias,'|')) AS value
            WHERE LENGTH(eventNameAlias)>1 AND eventNameAlias !='NULL')
        WHERE gacaEventName!= 'NULL' and gacaEventName !=''
            """      

        clt_query = """
                SELECT lower(eventName) as eventName
                FROM (SELECT eventName,eventCategory  FROM
                `dbce-c360-segamerws-prod-35ac.CDS.event`
                UNION ALL
                SELECT value,eventCategory FROM
                `dbce-c360-segamerws-prod-35ac.CDS.event`,
                UNNEST(SPLIT(eventNameAlias,'|')) AS value
                WHERE LENGTH(eventNameAlias)>1 AND eventNameAlias !='NULL')
                WHERE eventCategory ='Email CLT'
            """

        other_query = """SELECT lower(eventName) as eventName
                FROM ( SELECT eventName,eventCategory,eventNameAlias,gacaEventName FROM
                `dbce-c360-segamerws-prod-35ac.CDS.event`
                UNION ALL
                SELECT value, eventCategory,eventNameAlias,gacaEventName FROM
                `dbce-c360-segamerws-prod-35ac.CDS.event`,
                UNNEST(SPLIT(eventNameAlias,'|')) AS value
                WHERE LENGTH(eventNameAlias)>1 AND eventNameAlias !='NULL')
                WHERE eventName <> 'Changed Opt Status' AND
                eventCategory <>'Email CLT' and (gacaEventName = '' or gacaEventName ='NULL')
            """
        click_stream_query_job = client.query(click_stream_query).to_dataframe()
        clt_query_job = client.query(clt_query).to_dataframe()
        other_query_job = client.query(other_query).to_dataframe()

        registration_list = ['changed opt status', 'identify']
        clickstream_list = list(click_stream_query_job["eventName"])
        clickstream_list.append("screen")
        clickstream_list.append("page")
        clt_list = list(clt_query_job["eventName"])
        other_list = list(other_query_job["eventName"])
        return registration_list, clickstream_list, clt_list, other_list
