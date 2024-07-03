intermediate_traits = """
CREATE OR REPLACE TABLE
  `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` AS
WITH
  make_table AS (
  SELECT
    JSON_EXTRACT_SCALAR(raw_payload,"$.userId") AS userId,
    JSON_EXTRACT_SCALAR(raw_payload,"$.originalTimestamp") AS lastActivityDate,
    JSON_EXTRACT_SCALAR(raw_payload,"$.messageId") AS traceId,
    JSON_EXTRACT(raw_payload,"$.traits") AS traits_array,
    JSON_EXTRACT_SCALAR(raw_payload,"$.traits.marketingProgramNumber") AS marketingProgramNumber,
    JSON_EXTRACT_SCALAR(raw_payload,"$.traits.consumerId") AS consumerId,
    JSON_EXTRACT_SCALAR(raw_payload,"$.traits.sourceId") AS sourceId,
    JSON_EXTRACT_SCALAR(raw_payload,"$.traits.offlineConsumerId") AS offlineConsumerId,
    JSON_EXTRACT_SCALAR(raw_payload,"$.traits.email") AS email,
    JSON_EXTRACT_SCALAR(raw_payload,"$.traits.phoneNumber") AS phoneNumber,
    JSON_EXTRACT_SCALAR(raw_payload,"$.traits.registrationDate") AS registrationDate,
    MAX(CASE
        WHEN JSON_EXTRACT_SCALAR(e,"$.type")='marketingProgramNumber' THEN JSON_EXTRACT_SCALAR(e,"$.id")
    END
      ) AS e_marketingProgramNumber,
    MAX(CASE
        WHEN JSON_EXTRACT_SCALAR(e,"$.type")='consumerId' THEN JSON_EXTRACT_SCALAR(e,"$.id")
    END
      ) AS e_consumerId,
    MAX(CASE
        WHEN JSON_EXTRACT_SCALAR(e,"$.type")='sourceId' THEN JSON_EXTRACT_SCALAR(e,"$.id")
    END
      ) AS e_sourceId,
    MAX(CASE
        WHEN JSON_EXTRACT_SCALAR(e,"$.type")='offlineConsumerId' THEN JSON_EXTRACT_SCALAR(e,"$.id")
    END
      ) AS e_offlineConsumerId,
    MAX(CASE
        WHEN JSON_EXTRACT_SCALAR(e,"$.type")='email' THEN JSON_EXTRACT_SCALAR(e,"$.id")
    END
      ) AS e_email,
    MAX(CASE
        WHEN JSON_EXTRACT_SCALAR(e,"$.type")='phoneNumber' THEN JSON_EXTRACT_SCALAR(e,"$.id")
    END
      ) AS e_phoneNumber
  FROM
    `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
  LEFT JOIN
    UNNEST(JSON_EXTRACT_ARRAY(raw_payload,"$.context.externalIds")) e
  WHERE
    JSON_EXTRACT_SCALAR(raw_payload,"$.type")='identify'
  GROUP BY
    ALL )
SELECT
  DISTINCT userId,
  traceId,
  traits_array,
  lastActivityDate,
  COALESCE(marketingProgramNumber,e_marketingProgramNumber) AS marketingProgramNumber,
  COALESCE(consumerId,e_consumerId) AS consumerId,
  COALESCE(sourceId,e_sourceId) AS sourceId,
  COALESCE(offlineConsumerId,e_offlineConsumerId) AS offlineConsumerId,
  COALESCE(email,e_email) AS email,
  COALESCE(phoneNumber,e_phoneNumber) AS phoneNumber,
  registrationDate,
  CONCAT(CAST( COALESCE(NULLIF(TRIM(COALESCE(consumerId,e_consumerId)),''),NULLIF(TRIM(COALESCE(offlineConsumerId,e_offlineConsumerId)),''),TO_HEX(SHA256(COALESCE(coalesce(email,
                e_email),COALESCE(phoneNumber,e_phoneNumber)))) ) AS STRING),'_',CAST(COALESCE(sourceId,e_sourceId) AS STRING),'_',CAST(COALESCE(marketingProgramNumber,e_marketingProgramNumber,'{}') AS STRING) ) consumerid_sourceid_mpn
FROM
  make_table
WHERE
  COALESCE(marketingProgramNumber,e_marketingProgramNumber) IN ({},NULL) ;
"""

intermediate_1_traits = """
create or replace table `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` as 
select userId, traceId,lastActivityDate, CAST(marketingProgramNumber AS int64) AS marketingProgramNumber, 
consumerId, CAST(sourceId AS int64) AS sourceId, offlineConsumerId, email, phoneNumber, registrationDate, 
consumerid_sourceid_mpn, key, val
from `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
    ,unnest([struct(`dbce-c360-isl-preprod-d942.cdp2_registration_data.get_leaves`(traits_array) as leaves)])
    ,unnest(`dbce-c360-isl-preprod-d942.cdp2_registration_data.get_keys`(leaves)) key with offset
join unnest(`dbce-c360-isl-preprod-d942.cdp2_registration_data.get_values`(leaves)) val with offset using(offset),
unnest([struct(split(key, '.') as keys)]);
"""

ids_query = """
create or replace table `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` as 
select distinct userId,CAST(marketingProgramNumber AS int64) AS marketingProgramNumber,consumerId,
CAST(sourceId AS int64) AS sourceId,offlineConsumerId,email,phoneNumber,registrationDate,
concat(coalesce(consumerId,offlineConsumerId,to_hex(sha256(concat(coalesce(email,phoneNumber),
registrationDate)))),'_',sourceId,'_',coalesce(marketingProgramNumber,'{}')) as consumerid_sourceid_mpn
from `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
where
    ( (marketingProgramNumber is not null and coalesce(consumerId,offlineConsumerId) is not null and sourceId is not null) OR
        (marketingProgramNumber is not null and coalesce(email,phoneNumber) is not null and 
        registrationDate is not null and sourceId is not null)
    );
"""

traits_query = """
create or replace table `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` as 
    select 
    coalesce(a.consumerid_sourceid_mpn,b.consumerid_sourceid_mpn) consumerid_sourceid_mpn, 
    coalesce(c.traitId,ta.trait_id) traitId,
    a.val trait_value,
    a.lastActivityDate trait_consumer_last_updated_date,
    a.traceId trace_id
    from `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` a 
    left join `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` b
    on a.userId=b.userId
    left join `dbce-c360-segamaws-prod-9669.CDS.eco_system_trait` c
    on a.key=c.ecoTraitName
    and c.ecoSystemId in (1,4,10)
    left join `dbce-c360-segamaws-prod-9669.CDS.trait_aliases` ta
    on a.key=ta.alias
    join `dbce-c360-segamaws-prod-9669.CDS.trait` t
    on coalesce(c.traitId,ta.trait_id)=t.traitId
    join `dbce-c360-segamaws-prod-9669.CDS.pg_data_source_marketing_program` pdsmp 
    on coalesce(a.marketingProgramNumber,b.marketingProgramNumber,{})=pdsmp.marketing_program_id
    and coalesce(a.sourceId,b.sourceId)=pdsmp.pg_data_source_id
    where (a.consumerid_sourceid_mpn is not null OR b.consumerid_sourceid_mpn is not null)
    and trim(coalesce(val,""))<>""
    group by ALL
 qualify rank() over (partition by consumerid_sourceid_mpn,traitId,trait_value order by trait_consumer_last_updated_date desc)=1;
"""

consents_query = """
create or replace table `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` as
select distinct userId,safe_cast(lastActivityDate as timestamp) created_date,traceId trace_id,
anonymousId,
coalesce(a.countryCode,'{}') country_code,
safe_cast(coalesce(emailConsumerChoiceDate,phoneConsumerChoiceDate,postalConsumerChoiceDate,regulatoryConsumerChoiceDate,socialConsumerChoiceDate,lastActivityDate) as timestamp) opt_date,
coalesce(emailOptStatusReason,phoneOptStatusReason,postalOptStatusReason,socialOptStatusReason) OptStatusReason,
cast(coalesce(emailSubscriptionOptIndicator,phoneSubscriptionOptIndicator,postalSubscriptionOptIndicator,regulatorySubscriptionOptIndicator,socialSubscriptionOptIndicator) as string) opt_status,
safe_cast(coalesce(emailSubscriptionOptNumber,phoneSubscriptionOptNumber,postalSubscriptionOptNumber,regulatorySubscriptionOptNumber,socialSubscriptionOptNumber) as int64) opt_number,
coalesce(emailSubscriptionServiceName,phoneSubscriptionServiceName,postalSubscriptionServiceName,regulatorySubscriptionServiceName,socialSubscriptionServiceName) service_name,
coalesce(cast(initialOptStatus as string),'false') initial_opt_status,
languageCode,
coalesce(safe_cast(legalEntityNumber as int64),{}) legal_entity_id,
nationalId,
optId opt_id,
optReason,
optText opt_text,
optVersion,
coalesce(regulatoryContactCategory) ContactCategory,
coalesce(regulatoryContactType,cpt.contact_point_type_code) ContactType,
uuid,
safe_cast(coalesce(a.marketingProgramNumber,marketingProgramNumber1) as int64) marketing_program_number,
coalesce(consumerId,consumerId1) consumer_id,
coalesce(safe_cast(coalesce(sourceId,sourceId1,'0') as int64),0) source_id,
coalesce(offlineConsumerId,offlineConsumerId1) offlineConsumerId,
coalesce(email,email1) email,
coalesce(phoneNumber,phoneNumber1) phone_number,
case when emailSubscriptionServiceName is not null then 'E'
when phoneSubscriptionServiceName is not null then 'P'
when postalSubscriptionServiceName is not null then 'A'
when regulatorySubscriptionServiceName is not null then 'R'
when socialSubscriptionServiceName is not null then 'S' end as channel,
contact_point_type_name
from (
SELECT
max(json_extract_scalar(raw_payload,"$.messageId")) as traceId,
json_extract_scalar(raw_payload,"$.userId") as userId,
json_extract_scalar(raw_payload,"$.originalTimestamp") as lastActivityDate,
json_extract_scalar(raw_payload,"$.anonymousId") as anonymousId,
json_extract_scalar(raw_payload,"$.properties.consumerId") as 				 consumerId,
json_extract_scalar(raw_payload,"$.properties.offlineConsumerId") as 				 offlineConsumerId,
json_extract_scalar(raw_payload,"$.properties.countryCode") as 				 countryCode,
json_extract_scalar(raw_payload,"$.properties.email") as 				 email,
json_extract_scalar(raw_payload,"$.properties.emailConsumerChoiceDate") as 		 emailConsumerChoiceDate,
json_extract_scalar(raw_payload,"$.properties.emailOptStatusReason") as 		 emailOptStatusReason,
CASE WHEN length(JSON_EXTRACT_SCALAR(raw_payload,"$.properties.emailSubscriptionOptIndicator"))>5 THEN NULL ELSE JSON_EXTRACT_SCALAR(raw_payload,"$.properties.emailSubscriptionOptIndicator")
END as emailSubscriptionOptIndicator,
json_extract_scalar(raw_payload,"$.properties.emailSubscriptionOptNumber") as 		 emailSubscriptionOptNumber,
json_extract_scalar(raw_payload,"$.properties.emailSubscriptionServiceName") as 	 emailSubscriptionServiceName,
json_extract_scalar(raw_payload,"$.properties.initialOptStatus") as 			 initialOptStatus,
json_extract_scalar(raw_payload,"$.properties.languageCode") as 			 languageCode,
json_extract_scalar(raw_payload,"$.properties.legalEntityNumber") as 			 legalEntityNumber,
json_extract_scalar(raw_payload,"$.properties.marketingProgramNumber") as 		 marketingProgramNumber,
json_extract_scalar(raw_payload,"$.properties.nationalId") as 				 nationalId,
json_extract_scalar(raw_payload,"$.properties.optId") as 				 optId,
json_extract_scalar(raw_payload,"$.properties.optReason") as 				 optReason,
json_extract_scalar(raw_payload,"$.properties.optText") as 				 optText,
json_extract_scalar(raw_payload,"$.properties.optVersion") as 				 optVersion,
json_extract_scalar(raw_payload,"$.properties.phoneConsumerChoiceDate") as 		 phoneConsumerChoiceDate,
json_extract_scalar(raw_payload,"$.properties.phoneNumber") as 				 phoneNumber,
json_extract_scalar(raw_payload,"$.properties.phoneOptStatusReason") as 		 phoneOptStatusReason,
json_extract_scalar(raw_payload,"$.properties.phoneSubscriptionOptIndicator") as 	 phoneSubscriptionOptIndicator,
json_extract_scalar(raw_payload,"$.properties.phoneSubscriptionOptNumber") as 		 phoneSubscriptionOptNumber,
json_extract_scalar(raw_payload,"$.properties.phoneSubscriptionServiceName") as 	 phoneSubscriptionServiceName,
json_extract_scalar(raw_payload,"$.properties.postalConsumerChoiceDate") as 		 postalConsumerChoiceDate,
json_extract_scalar(raw_payload,"$.properties.postalOptStatusReason") as 		 postalOptStatusReason,
json_extract_scalar(raw_payload,"$.properties.postalSubscriptionOptIndicator") as 	 postalSubscriptionOptIndicator,
json_extract_scalar(raw_payload,"$.properties.postalSubscriptionOptNumber") as 		 postalSubscriptionOptNumber,
json_extract_scalar(raw_payload,"$.properties.postalSubscriptionServiceName") as 	 postalSubscriptionServiceName,
json_extract_scalar(raw_payload,"$.properties.regulatoryConsumerChoiceDate") as 	 regulatoryConsumerChoiceDate,
json_extract_scalar(raw_payload,"$.properties.regulatoryContactCategory") as 		 regulatoryContactCategory,
json_extract_scalar(raw_payload,"$.properties.regulatoryContactType") as 		 regulatoryContactType,
json_extract_scalar(raw_payload,"$.properties.regulatorySubscriptionOptIndicator") as 	 regulatorySubscriptionOptIndicator,
json_extract_scalar(raw_payload,"$.properties.regulatorySubscriptionOptNumber") as 	 regulatorySubscriptionOptNumber,
json_extract_scalar(raw_payload,"$.properties.regulatorySubscriptionServiceName") as 	 regulatorySubscriptionServiceName,
json_extract_scalar(raw_payload,"$.properties.socialConsumerChoiceDate") as 		 socialConsumerChoiceDate,
json_extract_scalar(raw_payload,"$.properties.socialOptStatusReason") as 		 socialOptStatusReason,
json_extract_scalar(raw_payload,"$.properties.socialSubscriptionOptIndicator") as 	 socialSubscriptionOptIndicator,
json_extract_scalar(raw_payload,"$.properties.socialSubscriptionOptNumber") as 		 socialSubscriptionOptNumber,
json_extract_scalar(raw_payload,"$.properties.socialSubscriptionServiceName") as 	 socialSubscriptionServiceName,
json_extract_scalar(raw_payload,"$.properties.sourceId") as 				 sourceId,
json_extract_scalar(raw_payload,"$.properties.uuid") as 				 uuid,
max(case when json_extract_scalar(b,"$.type")='marketingProgramNumber' then json_extract_scalar(b,"$.id") end) as marketingProgramNumber1,
max(case when json_extract_scalar(b,"$.type")='consumerId' then json_extract_scalar(b,"$.id") end) as consumerId1,
max(case when json_extract_scalar(b,"$.type")='sourceId' then json_extract_scalar(b,"$.id") end) as sourceId1,
max(case when json_extract_scalar(b,"$.type")='offlineConsumerId' then json_extract_scalar(b,"$.id") end) as offlineConsumerId1,
max(case when json_extract_scalar(b,"$.type")='email' then json_extract_scalar(b,"$.id") end) as email1,
max(case when json_extract_scalar(b,"$.type")='phoneNumber' then json_extract_scalar(b,"$.id") end) as phoneNumber1,
 FROM `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
 left join unnest(json_extract_array(raw_payload,"$.context.externalIds")) b
 where json_extract_scalar(raw_payload,"$.type")='track'
 group by 2,3,4,5,6,7,8,9,10,
 11,12,13,14,15,16,17,18,19,20,
 21,22,23,24,25,26,27,28,29,30,
 31,32,33,34,35,36,37,38,39,40,
 41,42,43,44,45,46) a 
 join dbce-c360-segamaws-prod-9669.CDS.contact_point_type cpt
 on case when emailSubscriptionServiceName is not null then 'E'
when phoneSubscriptionServiceName is not null then 'P'
when postalSubscriptionServiceName is not null then 'A'
when regulatorySubscriptionServiceName is not null then 'R'
when socialSubscriptionServiceName is not null then 'S' end=cpt.contact_point_type_code;
"""

final_table_query = """
create or replace table `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` as (
SELECT distinct a.*,json_extract_scalar(a.raw_payload,"$.messageId") messageId FROM
`dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` a
left join (select trace_id from
`dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` group by 1) b
on json_extract_scalar(a.raw_payload,"$.messageId")=b.trace_id
left join (select trace_id from (select
trace_id
from
`dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
qualify rank() over (partition by userId,opt_date,opt_status,service_name,contact_point_type_name order by 
created_date desc,trace_id desc)=1) group by 1) c
on json_extract_scalar(a.raw_payload,"$.messageId")=c.trace_id
where b.trace_id is not null or c.trace_id is not null) ;
"""


