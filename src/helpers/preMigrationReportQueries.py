trait_reports_query = """
with traits as ( 
SELECT cast (marketing_program_number as string) as marketing_program_number, table_schema, 
report_date, validation_result, trait_match AS Relevant_trait_match, BQ_trait_column, 
ecoTraitName,marketingProgramNumber,traitName, traitId,
    CASE WHEN trait_match IS FALSE 
    THEN STRING_AGG(DISTINCT suggested_traitName,' | ')
    ELSE NULL
    END AS suggested_traitName,
    CASE WHEN trait_match IS FALSE
    THEN STRING_AGG(DISTINCT CAST(suggested_traitId AS STRING),' | ')
    ELSE NULL
    END AS suggested_traitId,
    CASE WHEN trait_match IS FALSE
    THEN STRING_AGG(DISTINCT suggested_ecoTraitName,' | ')
    ELSE NULL
    END AS suggested_ecoTraitName
    FROM (
    SELECT cast (marketing_program_number as string) as marketing_program_number,
        table_schema, TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) report_date,
        CASE WHEN CDDA_Traits.traitId IS NOT NULL THEN TRUE ELSE FALSE END validation_result,
        CASE WHEN Eco_Traits.traitId IS NOT NULL THEN TRUE ELSE FALSE END trait_match,
        BQlist.column_name AS BQ_trait_column,
        COALESCE(CDDA_Traits.ecoTraitName,
        Eco_Traits.ecoTraitName) AS ecoTraitName,
        COALESCE(CDDA_Traits.marketingProgramNumber,
        Eco_Traits.marketingProgramNumber) AS marketingProgramNumber,
        COALESCE(CDDA_Traits.traitName,
        Eco_Traits.traitName) AS traitName,
        COALESCE(CDDA_Traits.regex,
        Eco_Traits.regex) AS regex,
        COALESCE(CDDA_Traits.traitId,
        Eco_Traits.traitId) AS traitId,
        COALESCE(CDDA_Traits.dataType,
        Eco_Traits.dataType) AS dataType,
        Eco_Traits_Like.traitName AS suggested_traitName,
        Eco_Traits_Like.traitId AS suggested_traitId,
        Eco_Traits_Like.dataType AS suggested_dataType,
        Eco_Traits_Like.ecoTraitName AS suggested_ecoTraitName,
        CONCAT('%', REPLACE(LOWER(SUBSTR(REPLACE(BQlist.column_name, 'trait_', ''), 0, 8)), '_', ''), '%')  as partial_value  
    FROM ( 
        SELECT table_name, column_name, table_schema
        FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'identifies' AND is_system_defined ='NO'
        AND column_name NOT LIKE 'context_%' AND column_name NOT LIKE 'score_%' AND column_name NOT LIKE '%_trait_number'
        AND column_name NOT LIKE '%_last_update%' AND column_name NOT LIKE '%hypercare%'
        AND column_name NOT IN ( 'id',  'anonymous_id', 'id', 'loaded_at', 'migration_flag', 'original_timestamp',
            'received_at', 'sent_at', 'timestamp', 'user_id', 'migration_file_type', 'migration_source_id',
            'janrain_clients', 'user_id', 'transmitter_source_id', 'uuid_ts' ) ) AS BQlist,
        ( SELECT distinct marketing_program_number FROM `{}.{}.identifies` ) AS MP
    LEFT JOIN (
        SELECT mp_tr.marketingProgramNumber, mp_tr.traitName, mp_tr.regex, tr.traitId, tr.dataType, ecoTraitName
        FROM `{}.CDS.ecosystemtrait` AS tr
        JOIN `{}.CDS.mpntraitmap` AS mp_tr
        ON tr.traitId = mp_tr.traitId AND ecoSystemId =5 ) AS CDDA_Traits
    ON  
        CAST(CDDA_Traits.marketingProgramNumber AS STRING) = cast( MP.marketing_program_number as string)
        AND LOWER(CDDA_Traits.ecoTraitName) = LOWER(BQlist.column_name)
    LEFT JOIN ( 
        SELECT mp_tr.marketingProgramNumber, mp_tr.traitName, mp_tr.regex, tr.traitId, tr.dataType, ecoTraitName
        FROM `{}.CDS.ecosystemtrait` AS tr
        JOIN `{}.CDS.mpntraitmap` AS mp_tr
        ON tr.traitId = mp_tr.traitId  AND ecoSystemId =5 ) AS Eco_Traits
    ON 
        CAST(Eco_Traits.marketingProgramNumber AS STRING) = cast(MP.marketing_program_number as string)
        AND REPLACE(LOWER(Eco_Traits.ecoTraitName), '_', '') = REPLACE(LOWER(BQlist.column_name), '_', '')
    LEFT JOIN (
        SELECT mp_tr.marketingProgramNumber, mp_tr.traitName, mp_tr.regex, tr.traitId, tr.dataType, ecoTraitName
        FROM `{}.CDS.ecosystemtrait` AS tr
        JOIN `{}.CDS.mpntraitmap` AS mp_tr
        ON tr.traitId = mp_tr.traitId AND ecoSystemId =5 ) AS Eco_Traits_Like
    ON
        CAST(Eco_Traits_Like.marketingProgramNumber AS STRING) = cast(MP.marketing_program_number as string)
        AND REPLACE(LOWER(Eco_Traits_Like.ecoTraitName), '_', '') LIKE CONCAT('%', REPLACE(LOWER(SUBSTR(REPLACE(BQlist.column_name, 'trait_', ''), 0, 8)), '_', ''), '%') )
    where validation_result=false and marketing_program_number IS NOT NULL AND cast(marketing_program_number as string) not LIKE (' ')
GROUP BY marketing_program_number,table_schema,report_date,validation_result,trait_match,BQ_trait_column,ecoTraitName,marketingProgramNumber,traitName,traitId 
) ,
column_list as (
SELECT distinct column_name FROM 
`{}.{}.INFORMATION_SCHEMA.COLUMNS`   
where table_name='identifies' and (column_name like ('%_opt_indicator') or column_name like ('%_opt_choice_date') ) 
and column_name not like ('%subscription%') and column_name not like ('trait%') 
),

trait_alias as 
(select * from traits left join `{}.CDS.trait_aliases` aliases 
on traits.BQ_trait_column = aliases.alias where aliases.alias is null 
)
SELECT 
distinct marketing_program_number,
table_schema,
report_date,
validation_result,
Relevant_trait_match,
BQ_trait_column,
case when BQ_trait_column in ('consumer_id','consumerId','marketing_program_number','marketingProgramNumber','countrycode','countryCode','country_code','source_id',
'sourceId','last_activity_date','lastActivityDate','marketingProgram',"consumerID","primaryAddress.country","lastUpdated","marketingProgramNbr","sourceID",
"traitcountryCodeIdValue","traitsourceIdIdValue","traitSourceIdIdValue","traitSourceId.traitValue","traitconsumerIdIdValue","traitConsumerIdIdValue",
"traitConsumerId.traitValue","traitCountryCodeIdValue","traitlastActivityDateIdValue","traitLastActivityDateIdValue","traitLastActivityDate.traitValue",
"traitmarketingProgramIdValue","marketingProgram","traitMarketingProgramIdValue","traitMarketingProgram.traitValue") then true else false end as CDP_Mandatory_Field,
ecoTraitName,
marketingProgramNumber,
traitName,
traitId,
suggested_traitName,
suggested_traitId,
suggested_ecoTraitName
from trait_alias left join column_list 
on trait_alias.BQ_trait_column = column_list.column_name 
where column_list.column_name is null
"""

traits_counts_query = """
SELECT 'non_blank_counts' as value,
COUNT({}) AS non_blank_counts,
DATE(MAX(original_timestamp)) AS latest_identifies
FROM `{}.{}.identifies`
WHERE CAST({} AS string) IS NOT NULL AND TRIM(CAST({} AS string)) <> ""
AND CAST (marketing_program_number AS string) = "{}"
union all
SELECT 'overall_counts' as value, 
COUNT({}) AS non_blank_counts,
DATE(MAX(original_timestamp)) AS latest_identifies 
FROM `{}.{}.identifies`
WHERE CAST (marketing_program_number AS string) = "{}"
order by value asc
"""

sql_val_main = """ 
SELECT * FROM 
( SELECT '{}' as dataset ,'{}' as tablename,'{}' as eventName,
TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) report_date,
CASE
WHEN CDDA_events.propertyName IS NOT NULL THEN TRUE
ELSE
FALSE
END
validation_result,
BQlist.column_name AS BQ_events_column,
CDDA_events.*
FROM ( 
SELECT
table_name,
column_name
FROM
`{}`.{}.INFORMATION_SCHEMA.COLUMNS
WHERE
table_name ='{}'
AND is_system_defined ='NO'
AND column_name NOT LIKE 'context_%'
AND column_name NOT LIKE 'score_%'
AND column_name NOT LIKE '%trait_number'
AND column_name NOT LIKE '%last_update%'
AND column_name NOT LIKE '%hypercare_%'
AND column_name NOT IN ( 'id',
'anonymous_id',
'id',
'loaded_at',
'migration_flag',
'original_timestamp',
'received_at',
'sent_at',
'timestamp',
'user_id',
'migration_file_type',
'migration_source_id',
'uuid_ts' ) ) AS BQlist
LEFT JOIN ( 
SELECT
DISTINCT LOWER(REGEXP_REPLACE(propertyName, r"[^a-zA-Z ]","")) as propertyName 
FROM
`{}.CDS.event_property`
UNION ALL
SELECT
DISTINCT LOWER(REGEXP_REPLACE(propertyNameAlias, r"[^a-zA-Z ]","")) as propertyNameAlias
FROM
`{}.CDS.event_property`
WHERE
eventName = '{}' ) AS CDDA_events
ON
LOWER(TRIM(REGEXP_REPLACE(propertyName, r"[^A-Za-z ]","")) )= LOWER(TRIM(REGEXP_REPLACE(BQlist.column_name, r"[^a-zA-Z ]","")))
) where validation_result=FALSE 
"""

table_schema_query = """
SELECT distinct
table_catalog as project_id,
table_schema as dataset,
table_name,
table_schema ||'.'|| table_name as body_template
FROM `{}.region-{}`.INFORMATION_SCHEMA.TABLES
WHERE
table_name='tracks' 
and LOWER(table_schema) ='{}'
"""

events_list = """
select event, event_text, EventCount, EventMin, EventMax from
(SELECT event, event_text, count(1) as EventCount, min(timestamp) as EventMin, max(timestamp) as EventMax
FROM `{project_id}.{dataset}.tracks` 
group by 1,2
order by 1,2) e
order by 1,2,3,4,5 
"""

event_property_counts_query = """
SELECT COUNT({}) AS count,MAX(received_at) AS latest_event
FROM 
`{}.{}.{}`
"""

events_reports_query = """
SELECT * FROM ( SELECT
'{}' AS dataset,
TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) report_date,
CASE WHEN CDDA_events.eventName IS NOT NULL THEN TRUE ELSE FALSE END validation_result,
BQlist.*,
CDDA_events.*
FROM ( 
SELECT LOWER(REGEXP_REPLACE(event, r"[^a-zA-Z ]"," ")) as BQ_event_column, event, event_text, EventCount, EventMax
FROM (
SELECT
    event, event_text, COUNT(1) AS EventCount, MAX(timestamp) AS EventMax
FROM
    `{}.{}.tracks`
GROUP BY  1, 2
ORDER BY  1, 2) ) AS BQlist
LEFT JOIN ( 
SELECT eventName FROM `{}.CDS.event`
UNION ALL SELECT value FROM `{}.CDS.event`,
UNNEST(SPLIT(eventNameAlias,'|')) AS value
WHERE LENGTH(value)>1 AND value !="NULL") AS CDDA_events
ON LOWER(TRIM(REGEXP_REPLACE(eventName, r"[^A-Za-z ]"," ")) )= LOWER(TRIM(REGEXP_REPLACE(BQlist.event, r"[^a-zA-Z ]"," "))))
WHERE validation_result = false
"""

invalid_mpn_reports_query = """
select * from ( SELECT
DISTINCT cast(marketing_program_number as string) AS MPN,'false' as validation_result,
TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) report_date
FROM `{}.{}.identifies`
WHERE trim(marketing_program_number) NOT IN (
SELECT DISTINCT cast(identifies.marketing_program_number as string) as mpn
FROM `{}.{}.canonical_user_ctas` AS identifies
LEFT JOIN `{}.CDS.mpn` AS crs
ON CAST(crs.marketingProgramNumber AS string) =CAST(identifies.marketing_program_number AS string)
WHERE CAST(marketing_program_number AS string) IS NOT NULL )
) where trim(CAST(MPN AS string)) <> ''
"""

source_validation_reports_query = """
SELECT * FROM (
SELECT DISTINCT marketing_program_number,source_id,
CASE WHEN ( cast( marketing_program_number as string) =CAST(pg_mp.marketing_program_id AS string) AND 
cast (source_id as string )=CAST(pg_mp.pg_data_source_id AS string) )THEN TRUE
ELSE FALSE END AS validation_result,TIMESTAMP(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) report_date,counts,
registration_date,latest_event
FROM (  SELECT DISTINCT marketing_program_number, source_id, MAX(registration_date) as registration_date,
max(received_at) as latest_event, COUNT(*) as counts
FROM `{}.{}.identifies`
GROUP BY marketing_program_number, source_id  ) AS ctas
JOIN `{}.CDS.mpn` AS mpn
ON CAST(mpn.marketingProgramNumber AS string) = cast( ctas.marketing_program_number as string)
LEFT JOIN `{}.CDS.pgdatasourcempn` AS pg_mp
ON CAST (pg_mp.marketing_program_id AS string) = cast (ctas.marketing_program_number as string)
AND CAST(pg_mp.pg_data_source_id AS string) = cast (ctas.source_id as string)
JOIN `{}.CDS.pgdatasource` AS pg
ON CAST(pg.sourceId AS string) = cast (ctas.source_id as string )
) WHERE validation_result=FALSE
"""

opts_validation_reports_query = """
WITH opts as (select distinct "{}" as persona_name,opt_id,
COALESCE(cast(email_subscription_opt_number as string),
cast(phone_subscription_opt_number as string),
cast(postal_subscription_opt_number as string),
cast(social_subscription_opt_number as string)) as subscription_opt_number,
ifnull(
COALESCE(email_subscription_service_name,phone_subscription_service_name,postal_subscription_service_name,social_subscription_service_name),"") as service_name
,CASE
WHEN (email_subscription_opt_number is not null or email_subscription_service_name is not null) THEN "Email"
WHEN (phone_subscription_opt_number is not null or phone_subscription_service_name is not null) THEN "Phone"
WHEN (postal_subscription_opt_number is not null or postal_subscription_service_name is not null) THEN "Postal"
WHEN (social_subscription_opt_number is not null or social_subscription_service_name is not null) THEN "Social"
ELSE "KEY NOT FOUND"
END AS  opt_type
,CASE
WHEN (email_subscription_opt_number is not null or email_subscription_service_name is not null) THEN "E"
WHEN (phone_subscription_opt_number is not null or phone_subscription_service_name is not null) THEN "P"
WHEN (postal_subscription_opt_number is not null or postal_subscription_service_name is not null) THEN "A"
WHEN (social_subscription_opt_number is not null or social_subscription_service_name is not null) THEN "S"
ELSE "KEY NOT FOUND"
END AS contact_point_type_code,min(timestamp) as min_timestamp,max(timestamp) as max_timestamp,count(1) as counts
from  `{}.{}.changed_opt_status`
group by 1,2,3,4,5,6
)
,valids as (SELECT distinct ciam_opt_id,cdp_marketing_program_number,subscription_opt_number,service_name,contact_point_type_code FROM
`{}.CDS.marketing_program_opt_map` where cdp_marketing_program_number in ({}))

select opts.*,CASE WHEN v.cdp_marketing_program_number is null THEN "false" ELSE "true" END AS is_valid
from opts
left join valids v on (
CAST(v.subscription_opt_number as string) = coalesce(CAST(opts.subscription_opt_number as string),CAST(v.subscription_opt_number as string))
and v.service_name = coalesce(opts.service_name,v.service_name)
and v.contact_point_type_code = coalesce(opts.contact_point_type_code, v.contact_point_type_code)
and v.ciam_opt_id = coalesce(opts.opt_id,v.ciam_opt_id)
)
where opts.subscription_opt_number<>"9999" and opts.service_name<>"Universal"
order by counts desc
"""

ct_ecosystem_validation_reports_query = """
WITH
traits AS (
SELECT
DISTINCT mpest.marketingProgramId,
mpn.description AS marketingProgramName,
mpn.businessOrgUnit,
mpest.traitId,
est.ecoSystemTraitType,
est.traitName,
CASE
WHEN es.ecoSystemName = "CDP2.0" THEN "CDP2"
ELSE es.ecoSystemName
END AS ecoSystemName,
est.ecoTraitName
FROM `dbce-c360-isl-preprod-d942.CDS.marketing_program_eco_system_trait_map` mpest
LEFT JOIN `dbce-c360-isl-preprod-d942.CDS.mpn` mpn
ON mpn.marketingProgramNumber=mpest.marketingProgramId
LEFT JOIN `dbce-c360-isl-preprod-d942.CDS.ecosystem` es
ON es.ecoSystemId=mpest.ecoSystemId
LEFT JOIN `dbce-c360-isl-preprod-d942.CDS.eco_system_trait` est
ON (est.traitId=mpest.traitId AND est.ecoSystemId=mpest.ecoSystemId)
WHERE 1=1  AND mpest.marketingProgramId IN ({}) 
)

SELECT distinct * FROM traits
PIVOT(MAX(ecoTraitName) FOR ecoSystemName IN ("janrain","okta","segment", "bigQuery","CDP2", "CPC","lytics","braze", "mapp","retentionScience", "dataLake","OmniChat", "AM", "BDU"))
order by marketingProgramName,traitName
"""
