create_registration_data_table_query='''CREATE OR REPLACE TABLE
  `dbce-c360-isl-preprod-d942.cdp2_registration_data.registration_data_final_v2_{}` ( message_id STRING,
    raw_payload STRING )
CLUSTER BY
  message_id'''
vld2_query='''INSERT INTO
  `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
WITH
  unnested_data AS(
  SELECT
    *
  FROM (
    SELECT
      messageid,
      raw_payload,
      JSON_EXTRACT_SCALAR(arr, "$['type']") type,
      JSON_EXTRACT_SCALAR(arr, "$['id']") id,
    FROM (
      SELECT
        messageid,
        raw_payload,
        JSON_EXTRACT(dt,"$['externalIds']") ext
      FROM (
        SELECT
          JSON_VALUE(raw_payload,'$.messageId') messageid,
          raw_payload,
          JSON_EXTRACT(raw_payload,"$['context']") dt,
        FROM
          `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`)),
      UNNEST(JSON_EXTRACT_ARRAY(ext)) arr)
  WHERE
    type IN ('marketingProgramNumber') ),
  unnested_data2 AS(
  SELECT
    *
  FROM (
    SELECT
      messageid,
      raw_payload,
      JSON_EXTRACT_SCALAR(arr, "$['type']") type,
      JSON_EXTRACT_SCALAR(arr, "$['id']") id,
    FROM (
      SELECT
        messageid,
        raw_payload,
        JSON_EXTRACT(dt,"$['externalIds']") ext
      FROM (
        SELECT
          JSON_VALUE(raw_payload,'$.messageId') messageid,
          raw_payload,
          JSON_EXTRACT(raw_payload,"$['context']") dt,
        FROM
          `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` )),
      UNNEST(JSON_EXTRACT_ARRAY(ext)) arr)
  WHERE
    type IN ('countryCode') )
SELECT
  messageid,
  raw_payload,
  '{}',
  'VLD2',
  'Validate if mpn and country combination exists in CRS',
  CONCAT('marketingProgramNumber','-',mpn, ' And ','countryCode','-',CCode),
  CURRENT_TIMESTAMP()
FROM (
  SELECT
    unnested_data.messageid,
    unnested_data.raw_payload,
    unnested_data.id mpn,
    unnested_data2.id CCode,
    CASE
      WHEN (unnested_data.id = CAST(marketingProgramNumber AS string) AND unnested_data2.id = CAST(countryCode AS string)) THEN 1
    ELSE
    0
  END
    AS PASS_FAIL_FLAG
  FROM
    unnested_data
  INNER JOIN
    unnested_data2
  ON
    unnested_data.messageid =unnested_data2.messageid
  INNER JOIN
    `dbce-c360-segamaws-prod-9669.CDS.marketing_program` AS b
  ON
    unnested_data.id = CAST(marketingProgramNumber AS string)
    AND unnested_data2.id = CAST(countryCode AS string) )
WHERE
  PASS_FAIL_FLAG=0'''

vld3_query='''INSERT INTO `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
 with unnested_data AS(
SELECT
  *
FROM (
  SELECT
    messageid,raw_payload,
    JSON_EXTRACT_SCALAR(arr, "$['type']") type,
    JSON_EXTRACT_SCALAR(arr, "$['id']") id,
  FROM (
    SELECT
      messageid,raw_payload,
      JSON_EXTRACT(dt,"$['externalIds']") ext
    FROM (
      SELECT
        json_value(raw_payload,'$.messageId') messageid,
        raw_payload,
        JSON_EXTRACT(raw_payload,"$['context']") dt,
      FROM
       `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
        )),
    UNNEST(JSON_EXTRACT_ARRAY(ext)) arr)
WHERE
  type IN ('marketingProgramNumber')
 )
 
SELECT 
messageid,raw_payload,'{}','VLD3','Validate if mpn is Active from CRS (active means available)',
CONCAT('marketingProgramNumber','-',id) ,CURRENT_TIMESTAMP()  
FROM (
 SELECT messageid,raw_payload,unnested_data.id,
 CASE WHEN unnested_data.id = cast(marketingProgramNumber as string) THEN 1 ELSE 0 END AS PASS_FAIL_FLAG
  from 
  unnested_data
  INNER join
  `dbce-c360-segamaws-prod-9669.CDS.marketing_program` as  b
  on unnested_data.id = cast(marketingProgramNumber as string))
where PASS_FAIL_FLAG=0'''

vld4_query ='''INSERT INTO `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
SELECT 
messageid,cast (raw_payload as string),'{}','VLD4','Loyalty account number can be alpha numeric and opt Ip address is optional field',
CONCAT('loyaltyAccountNumber',' ', id ) , CURRENT_TIMESTAMP() 
FROM 
(
SELECT messageid,raw_payload,id,
  CASE  
    WHEN REGEXP_CONTAINS(id,'^[A-Za-z0-9-]*$')  
 THEN 1    --'Alphanumeric'
    ELSE 0   END   --'Not Alphanumeric' 
  AS PASS_FAIL_FLAG   
FROM (
  SELECT messageid,raw_payload,
    JSON_EXTRACT_SCALAR(arr, "$['type']") type,
    JSON_EXTRACT_SCALAR(arr, "$['id']") id,
  FROM (
    SELECT  messageid,raw_payload,
      JSON_EXTRACT(dt,"$['externalIds']") ext
    FROM (
      SELECT 
        json_value(raw_payload,'$.messageId') messageid,
        raw_payload,
        JSON_EXTRACT(raw_payload,"$['context']") dt
      FROM
        `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
           )),
    UNNEST(JSON_EXTRACT_ARRAY(ext)) arr)
WHERE
  type IN ('loyaltyAccountNumber') 
 )
 WHERE  PASS_FAIL_FLAG=0'''

vld5_query = '''INSERT INTO `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
 SELECT
  messageid,raw_payload,'{}','VLD5','Either of optId , opt number or opt service name is required field',
'optId/SubscriptionOptNumber/SubscriptionServiceName is either missing /wrong ' ,CURRENT_TIMESTAMP()  
 FROM
 (
 SELECT  
      raw_payload,
      messageid,
      IF ((cond1=1 OR cond2=1 OR cond3=1 OR cond4=1 OR cond5=1 OR cond6=1),1,0) as PASS_FAIL_FLAG 
FROM
 (
SELECT  raw_payload,
JSON_VALUE(raw_payload,'$.messageId') messageid,
IF (json_value(raw_payload,'$.properties.optId') IS NOT NULL,1,0) cond1,

IF ((json_value(raw_payload,'$.properties.contactType')= "E" AND (json_value(raw_payload,'$.properties.contactCategory') ='EP') AND  
          (
          json_value(raw_payload,'$.properties.emailSubscriptionOptNumber') IS NOT NULL 
          OR
          json_value(raw_payload,'$.properties.emailSubscriptionServiceName') IS NOT NULL 
    )),1,0) cond2,

IF ((json_value(raw_payload,'$.properties.contactType')= "P"  AND (json_value(raw_payload,'$.properties.contactCategory') ='PM') AND  
          (
          json_value(raw_payload,'$.properties.phoneSubscriptionOptNumber') IS NOT NULL 
          OR
          json_value(raw_payload,'$.properties.phoneSubscriptionServiceName') IS NOT NULL 
    )),1,0) cond3,

IF ((json_value(raw_payload,'$.properties.contactType')= "A" AND (json_value(raw_payload,'$.properties.contactCategory') ='AR') AND  
          (
          json_value(raw_payload,'$.properties.postalSubscriptionOptNumber') IS NOT NULL 
          OR
          json_value(raw_payload,'$.properties.postalSubscriptionServiceName') IS NOT NULL 
    )),1,0) cond4,

IF ((json_value(raw_payload,'$.properties.contactType')= "S"  AND (json_value(raw_payload,'$.properties.contactCategory') ='OT') AND  
          (
          json_value(raw_payload,'$.properties.socialSubscriptionOptNumber') IS NOT NULL 
          OR
          json_value(raw_payload,'$.properties.socialSubscriptionServiceName') IS NOT NULL 
    )),1,0) cond5,

IF ((json_value(raw_payload,'$.properties.contactType')= "R" AND (json_value(raw_payload,'$.properties.contactCategory') ='PS') AND  
          (
          json_value(raw_payload,'$.properties.regulatorySubscriptionOptNumber') IS NOT NULL 
          OR
          json_value(raw_payload,'$.properties.regulatorySubscriptionServiceName') IS NOT NULL 
    )),1,0) cond6

FROM  
`dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
WHERE 
         JSON_VALUE(raw_payload,'$.event')= "Changed Opt Status"
         AND
         JSON_VALUE(raw_payload,'$.type')="track"

 )) WHERE  PASS_FAIL_FLAG=0'''

vld6_query= '''INSERT INTO `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}`
SELECT
  messageid,raw_payload,'{}','VLD6','The Universal request with mpn 9999 should have legalEntity trait',
'legalEntity trait is missing' ,CURRENT_TIMESTAMP() 
FROM (
  SELECT
    messageid,
    raw_payload,
    legalEntityNumber,
    JSON_EXTRACT_SCALAR(arr, "$['type']") type,
    JSON_EXTRACT_SCALAR(arr, "$['id']") id,
    ServiceName
  FROM (
    SELECT
      messageid,
      raw_payload,
      legalEntityNumber,
      JSON_EXTRACT(dt,"$['externalIds']") ext,
      ServiceName
    FROM (
      SELECT
        JSON_VALUE(raw_payload,'$.messageId') messageid,
        raw_payload,
        JSON_VALUE(raw_payload,'$.properties.legalEntityNumber') legalEntityNumber,
        JSON_EXTRACT(raw_payload,"$['context']") dt,
        CASE 
            WHEN  (json_value (raw_payload,'$.properties.ContactType')= "E")  AND (json_value(raw_payload,'$.properties.ContactCategory') ='EP') THEN json_value(raw_payload,'$.properties.emailSubscriptionServiceName')
            WHEN  (json_value (raw_payload,'$.properties.ContactType')= "P")  AND (json_value(raw_payload,'$.properties.ContactCategory') ='PM') THEN json_value(raw_payload,'$.properties.phoneSubscriptionServiceName')
            WHEN  (json_value (raw_payload,'$.properties.ContactType')= "A") AND (json_value(raw_payload,'$.properties.ContactCategory') ='AR') THEN json_value(raw_payload,'$.properties.postalSubscriptionServiceName')
            WHEN  (json_value (raw_payload,'$.properties.ContactType')= "S") AND (json_value(raw_payload,'$.properties.ContactCategory') ='OT') THEN json_value(raw_payload,'$.properties.socialSubscriptionServiceName')
            WHEN  (json_value (raw_payload,'$.properties.ContactType')= "R") AND (json_value(raw_payload,'$.properties.ContactCategory') ='PS') THEN json_value(raw_payload,'$.properties.regulatorySubscriptionServiceName')
            ELSE ''
        END AS ServiceName
      FROM
       `dbce-c360-isl-preprod-d942.cdp2_registration_data.{}` 
       WHERE 
         JSON_VALUE(raw_payload,'$.event')= "Changed Opt Status"
         AND
         JSON_VALUE(raw_payload,'$.type')="track"
       )),
    UNNEST(JSON_EXTRACT_ARRAY(ext)) arr)
WHERE
  ((type IN ('marketingProgramNumber') and id = "9999") OR( ServiceName= "Universal"))   AND legalEntityNumber IS NULL'''

final_insert='''
insert into `dbce-c360-isl-preprod-d942.cdp2_registration_data.<PERSONA NAME>_registration_data_final_v2`
select message_id,raw_payload from
`dbce-c360-isl-preprod-d942.cdp2_registration_data.<PERSONA NAME>_registration_data_final` a
where message_id not in  ( select  distinct message_id from 
`dbce-c360-isl-preprod-d942.cdp2_registration_data.dq_validation_dlq_errors_{}`
)'''