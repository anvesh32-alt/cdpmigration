from google.cloud import bigquery,storage
import json
import logging
from helpers import constants
import logging


def segment_audience_counts(**kwargs):

    base_mpn = kwargs['mpn']
    env = kwargs['env']
    client = bigquery.Client(constants.ISL_PROD_PROJECT)
    client_storage = storage.Client(constants.ISL_PROD_PROJECT)
    bucket = client_storage.get_bucket(constants.DAG_BUCKET)
    config = json.loads(bucket.get_blob("dags/cdp2.0-migration/src/configurations/audienceconfigfile.json").download_as_string())
    logging.info("Initializing with Bigquery and bucket completed Successfully.")
    
    opt_indicator = config[base_mpn]['opt_indicator']
    mpn = config[base_mpn]['segment_multiple_mpn']
    country_code = config[base_mpn]['country_code']
    ghh_indicator = config[base_mpn]['ghh_indicator']
    corp_indicator = config[base_mpn]['corp_indicator']
    profiling_opt_indicator = config[base_mpn]['profiling_opt_indicator']
    region = config[base_mpn]['region']
    event_date = {"preprod" : f"'{config[base_mpn]['date']}'", "preprodlive" : "current_date", "prod" : f"'{config[base_mpn]['date']}'"}
    
    
    #Retention Query
    retention_query = f'''
    CREATE or REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.{config[base_mpn]['retention_table']}` AS
    SELECT user_id FROM (select i.user_id
    ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] as email
    ,ARRAY_AGG(i.phone_number IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] as phone_number
    ,ARRAY_AGG(i.address_line1 IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] as address_line1
    ,ARRAY_AGG(i.address_line2 IGNORE NULLS ORDER BY i.timestamp desc LIMIT 1)[OFFSET (0)] as address_line2
    FROM `{constants.PROJECT_TABLES[region]}.{config[base_mpn]['dataset']}.identifies` i
    where context_personas_computation_class is null
    and date(_PARTITIONTIME) <= date({event_date[env]})
    group by  1)
    where cast(email as string)=' ' AND cast(phone_number as string)=' ' and cast(address_line1 as string)=' ' and cast(address_line2 as string)=' '
    '''

    print("Retention Query : ",retention_query)
    _ = client.query(retention_query).to_dataframe()

    #Create Segment Audience Counts Dump Query
    create_table = f'''
    CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
    select country,
    user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
    '''

    opt_ind = ', ' +', '.join(opt_indicator) 

    unmerge_data = f'''
    from(
    select IFNULL(country_code,{country_code}) as country
    ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
    when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
    else coalesce(m.canonical_user_id, i.id) end as user_id
    ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
    ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
    ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
    ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
    ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
    ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
    '''

    opt_arr_agg_query=''',ARRAY_AGG(i.{} IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as {}'''
    opt_arr_agg=[opt_arr_agg_query.format(i,i) for i in opt_indicator]
    opt_arr_agg = ' '.join(map(str,opt_arr_agg))

    personas_table = f'''
    FROM `{constants.PROJECT_TABLES[region]}.{config[base_mpn]['dataset']}.users` i
    left join `{constants.PROJECT_TABLES[region]}.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
    left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
    left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
    where date(_PARTITIONTIME) <= date({event_date[env]}) 
     and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "{base_mpn}" ELSE i.marketing_program_number END="{base_mpn}"
    group by  1, 2
    )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.{config[base_mpn]['retention_table']}` )    
    '''

    if base_mpn !='293' and base_mpn!='363' and base_mpn != '119' and base_mpn != '507' and base_mpn != '249' and base_mpn != '414' and base_mpn != '292':
        segment_query_dump = create_table + opt_ind + unmerge_data + opt_arr_agg + personas_table

    elif base_mpn == '292':
        segment_query_dump = f'''
    CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
    select country,
    user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
    , email_greece_corporate_newsletter_opt_indicator, email_greece_growing_families_newsletter_opt_indicator, email_greece_growing_families_receipt_profiling_opt_indicator,
    trait_number_children_id_value,trait_brandof_preference_id_value,trait_power_toothbrush_model_id_value,trait_femcare_brand_used_p3mid_value,trait_own_dishwasher_id_value,
    trait_own_pet_id_value,trait_automatic_dish_washer_detergent_brand_do_you_buy_id_value,trait_hand_dish_washer_brand_do_you_buy_id_value
    from(
    select IFNULL(country_code,'GRC') as country
    ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
    when i.id in (select id_value from `dbce-c360-isl-preprod-d942.CDP_unmerge.unmerge_list_eu_final_pre_prod`) then i.id
    else coalesce(m.canonical_user_id, i.id) end as user_id
    ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
    ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
    ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
    ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
    ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
    ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
    ,ARRAY_AGG(i.email_greece_corporate_newsletter_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_greece_corporate_newsletter_opt_indicator 
    ,ARRAY_AGG(i.email_greece_growing_families_newsletter_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_greece_growing_families_newsletter_opt_indicator 
    ,ARRAY_AGG(i.email_greece_growing_families_receipt_profiling_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_greece_growing_families_receipt_profiling_opt_indicator

    ,ARRAY_AGG(i.trait_number_children_id_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as trait_number_children_id_value 
    ,ARRAY_AGG(i.trait_brandof_preference_id_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as trait_brandof_preference_id_value 
    ,ARRAY_AGG(i.trait_power_toothbrush_model_id_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as trait_power_toothbrush_model_id_value 
    ,ARRAY_AGG(i.trait_femcare_brand_used_p3mid_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as trait_femcare_brand_used_p3mid_value 
    ,ARRAY_AGG(i.trait_own_dishwasher_id_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as trait_own_dishwasher_id_value 
    ,ARRAY_AGG(i.trait_own_pet_id_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as trait_own_pet_id_value 
    ,ARRAY_AGG(i.trait_automatic_dish_washer_detergent_brand_do_you_buy_id_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as trait_automatic_dish_washer_detergent_brand_do_you_buy_id_value 
    ,ARRAY_AGG(i.trait_hand_dish_washer_brand_do_you_buy_id_value IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as 
    trait_hand_dish_washer_brand_do_you_buy_id_value

    FROM `dbce-c360-segglblws-prod-b902.personas_grc_growing_families.users` i
    left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
    left join `dbce-c360-isl-preprod-d942.CDP_unmerge.CDP_merged_list_pre_prod_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ('292'))
    left join `dbce-c360-isl-preprod-d942.CDP_unmerge.CDP_merged_list_pre_prod_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ('292'))
    where date(_PARTITIONTIME) <= date('2024-06-06') 
    -- and i.marketing_program_number in ('292')
    --and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "292" ELSE cast(i.marketing_program_number as string) END="292"
    group by  1, 2
    )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.grc_growing_families_retention_segment` )
    '''
    elif base_mpn == '249':
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        WITH opts AS (
        SELECT
        user_id,
        opt_id,
        email_subscription_service_name AS opt_service_name,
        email_subscription_opt_indicator AS opt_ind,
        email_subscription_opt_number AS opt_number,
        email_consumer_choice_date AS opt_date,
        timestamp,
        loaded_at,email
        FROM `dbce-c360-segglblws-prod-b902.personas_pol_growing_families.changed_opt_status` i
        WHERE 1=1
        AND DATE(timestamp) <= date({event_date[env]})
        AND (email_subscription_service_name in ("Poland Corporate Newsletter","Poland GHH Newsletter", "Poland Receipt Profiling Newsletter","POL GrowingFamilies Profiling Consent") OR LOWER (email_subscription_service_name) = LOWER("Universal"))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id,email_subscription_service_name ORDER BY timestamp DESC,loaded_at DESC) = 1
        )

        select country, user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
        , email_poland_growing_families_newsletter_opt_indicator, email_poland_corporate_newsletter_opt_indicator,email_poland_growing_families_receipt_profiling_opt_indicator,email_poland_growing_families_profiling_opt_indicator
        from(
        select IFNULL(country_code,'POL') as country
        ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
        when i.id in (select id_value from `dbce-c360-isl-preprod-d942.CDP_unmerge.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
        else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
        ,ARRAY_AGG(case when opts.opt_service_name = "Poland GHH Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as  email_poland_growing_families_newsletter_opt_indicator
        ,ARRAY_AGG(case when opts.opt_service_name = "Poland Corporate Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_poland_corporate_newsletter_opt_indicator    

        ,ARRAY_AGG(case when opts.opt_service_name = "Poland Receipt Profiling Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_poland_growing_families_receipt_profiling_opt_indicator
        ,ARRAY_AGG(case when opts.opt_service_name = "POL GrowingFamilies Profiling Consent" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_poland_growing_families_profiling_opt_indicator

        FROM `dbce-c360-segglblws-prod-b902.personas_pol_growing_families.users` i
        left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join opts on opts.email=i.email
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
        where CASE WHEN i.email_valid_contact_point_indicator is not null THEN "249" ELSE i.marketing_program_number END="249"
        group by  1, 2
        )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.pol_growing_families_retention_segment` )    
        '''
    
    elif base_mpn == '507':
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        WITH opts AS (
        SELECT
        user_id,
        opt_id,
        email_subscription_service_name AS opt_service_name,
        email_subscription_opt_indicator AS opt_ind,
        email_subscription_opt_number AS opt_number,
        email_consumer_choice_date AS opt_date,
        timestamp,
        loaded_at
        FROM `dbce-c360-segglblws-prod-b902.personas_fra_braun.changed_opt_status`
        WHERE 1=1
        AND DATE(timestamp) < date('{config[base_mpn]['date']}')
        AND (email_subscription_service_name in ("FRA Braun Full Opt-in","FRA Braun Profiling Consent") OR LOWER (email_subscription_service_name) = LOWER("Universal"))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id,email_subscription_service_name ORDER BY timestamp DESC,loaded_at DESC) = 1
        )
        select country, user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
        , email_france_braun_profiling_opt_indicator, email_france_braun_full_opt_indicator
        from(
        select IFNULL(country_code,'FRA') as country
        ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
        when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
        else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
        ,ARRAY_AGG(case when opts.opt_service_name = "FRA Braun Full Opt-in" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then 'false'
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_france_braun_full_opt_indicator
        ,ARRAY_AGG(case when opts.opt_service_name = "FRA Braun Profiling Consent" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then 'false'
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_france_braun_profiling_opt_indicator	
        FROM `dbce-c360-segglblws-prod-b902.personas_fra_braun.users` i
        left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join opts on opts.user_id=i.id
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
        where date(_PARTITIONTIME) <= date({event_date[env]}) 
        and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "507" ELSE i.marketing_program_number END="507"
        group by  1, 2
        )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.fra_braun_retention_segment` )   
        '''

    elif base_mpn == '414':
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        WITH opts AS (
        SELECT
        user_id,
        opt_id,
        email_subscription_service_name AS opt_service_name,
        email_subscription_opt_indicator AS opt_ind,
        email_subscription_opt_number AS opt_number,
        email_consumer_choice_date AS opt_date,
        email,
        timestamp,
        loaded_at
        FROM `dbce-c360-segglblws-prod-b902.personas_nld_oral_b.changed_opt_status`
        WHERE 1=1
        AND DATE(timestamp) < date('{config[base_mpn]['date']}')
        AND (email_subscription_service_name in ("Netherlands Oral B Newsletter","NLD Oral B Newsletter", "NLD OralB Profiling Consent", "NLD OralB Full Opt-in", "NLD Oralb Newsletter", 'NLD OralB Newsletter') OR LOWER (email_subscription_service_name) = LOWER("Universal"))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id,email_subscription_service_name ORDER BY timestamp DESC,loaded_at DESC) = 1
        )
        select country, user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
        , email_netherlands_oral_b_full_opt_indicator, email_netherlands_oral_b_newsletter_opt_indicator, email_netherlands_oral_b_profiling_opt_indicator 
        from(
        select IFNULL(country_code,'NLD') as country
        ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
        when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
        else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id

        ,ARRAY_AGG(case when opts.opt_service_name = "NLD Oral B Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then "false"
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_netherlands_oral_b_full_opt_indicator
        ,ARRAY_AGG(case when opts.opt_service_name  in ("NLD Oral B Newsletter","Netherlands Oral B Newsletter", "NLD OralB Full Opt-in", "NLD Oralb Newsletter", 'NLD OralB Newsletter') then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then "false"
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_netherlands_oral_b_newsletter_opt_indicator
        ,ARRAY_AGG(case when opts.opt_service_name  in ("NLD OralB Profiling Consent") then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then "false"
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_netherlands_oral_b_profiling_opt_indicator 
        FROM `dbce-c360-segglblws-prod-b902.personas_nld_oral_b.users` i
        left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join opts on opts.user_id=i.id
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
        where date(_PARTITIONTIME) <= date({event_date[env]}) 
        and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "414" ELSE cast(i.marketing_program_number as string) END="414"
        group by  1, 2
        )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.nld_oral_b_retention_segment` )
        '''

    elif base_mpn == '249':
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        WITH opts AS (
        SELECT
        user_id,
        opt_id,
        email_subscription_service_name AS opt_service_name,
        email_subscription_opt_indicator AS opt_ind,
        email_subscription_opt_number AS opt_number,
        email_consumer_choice_date AS opt_date,
        timestamp,
        loaded_at
        FROM `dbce-c360-segglblws-prod-b902.personas_pol_growing_families.changed_opt_status` i
        WHERE 1=1
        AND DATE(timestamp) <= date({event_date[env]})
        AND (email_subscription_service_name in ("Poland Corporate Newsletter","Poland GHH Newsletter", "Poland Receipt Profiling Newsletter","POL GrowingFamilies Profiling Consent") OR LOWER (email_subscription_service_name) = LOWER("Universal"))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id,email_subscription_service_name ORDER BY timestamp DESC,loaded_at DESC) = 1
        )
        select country, user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
        , email_poland_growing_families_newsletter_opt_indicator, email_poland_corporate_newsletter_opt_indicator,email_poland_growing_families_receipt_profiling_opt_indicator
        from(
        select IFNULL(country_code,'POL') as country
        ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
        when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
        else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
        ,ARRAY_AGG(case when opts.opt_service_name = "Poland GHH Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_poland_corporate_newsletter_opt_indicator
        ,ARRAY_AGG(case when opts.opt_service_name = "Poland Corporate Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_poland_growing_families_newsletter_opt_indicator    

        ,ARRAY_AGG(case when opts.opt_service_name = "Poland Receipt Profiling Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_poland_growing_families_receipt_profiling_opt_indicator

        FROM `dbce-c360-segglblws-prod-b902.personas_pol_growing_families.users` i
        left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join opts on opts.user_id=i.id
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
        where date(_PARTITIONTIME) <= date({event_date[env]}) 
        and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "249" ELSE i.marketing_program_number END="249"
        group by  1, 2
        )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.pol_growing_families_retention_segment` )    
        '''
    
    elif base_mpn == '507':
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        WITH opts AS (
        SELECT
        user_id,
        opt_id,
        email_subscription_service_name AS opt_service_name,
        email_subscription_opt_indicator AS opt_ind,
        email_subscription_opt_number AS opt_number,
        email_consumer_choice_date AS opt_date,
        timestamp,
        loaded_at
        FROM `dbce-c360-segglblws-prod-b902.personas_fra_braun.changed_opt_status`
        WHERE 1=1
        AND DATE(timestamp) <= date({event_date[env]})
        AND (email_subscription_service_name in ("FRA Braun Full Opt-in","FRA Braun Profiling Consent") OR LOWER (email_subscription_service_name) = LOWER("Universal"))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id,email_subscription_service_name ORDER BY timestamp DESC,loaded_at DESC) = 1
        )
        select country, user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
        , email_france_braun_profiling_opt_indicator, email_france_braun_full_opt_indicator
        from(
        select IFNULL(country_code,'FRA') as country
        ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
        when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
        else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
        ,ARRAY_AGG(case when opts.opt_service_name = "FRA Braun Full Opt-in" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then 'false'
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_france_braun_full_opt_indicator
        ,ARRAY_AGG(case when opts.opt_service_name = "FRA Braun Profiling Consent" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then 'false'
        END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_france_braun_profiling_opt_indicator	
        --,ARRAY_AGG(i.email_france_braun_profiling_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_france_braun_profiling_opt_indicator ,ARRAY_AGG(i.email_france_braun_full_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_france_braun_full_opt_indicator
        FROM `dbce-c360-segglblws-prod-b902.personas_fra_braun.users` i
        left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join opts on opts.user_id=i.id
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
        where date(_PARTITIONTIME) <= date({event_date[env]}) 
        -- and i.marketing_program_number in ('507','357')
        and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "507" ELSE i.marketing_program_number END="507"
        group by  1, 2
        )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.fra_braun_retention_segment` )   
        '''
        
    elif base_mpn == '363':
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        WITH opts AS (
        SELECT
        user_id,
        opt_id,
        email_subscription_service_name AS opt_service_name,
        email_subscription_opt_indicator AS opt_ind,
        email_subscription_opt_number AS opt_number,
        email_consumer_choice_date AS opt_date,
        timestamp,
        loaded_at
        FROM `dbce-c360-segamaws-prod-9669.personas_arb_growing_families_everyday_me_arabia.changed_opt_status`
        WHERE 1=1
        AND DATE(timestamp) <= date({event_date[env]})
        AND (email_subscription_service_name in ("GrowingFamilies Arabia") OR LOWER (email_subscription_service_name) = LOWER("Universal"))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id,email_subscription_service_name ORDER BY timestamp DESC,loaded_at DESC) = 1
        )

        select country,user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id,email_arabia_growingfamilies_newsletter_opt_indicator
        from(
        select IFNULL(country_code,'ARB') as country
        ,Case
            when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
            when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
            else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
        ,ARRAY_AGG(case when opts.opt_service_name = "GrowingFamilies Arabia" then opts.opt_ind
                        when LOWER(opts.opt_service_name) = "universal" then false
                END IGNORE NULLS ORDER BY opts.timestamp desc LIMIT 1)[OFFSET (0)] as email_arabia_growingfamilies_newsletter_opt_indicator
        FROM `dbce-c360-segamaws-prod-9669.personas_arb_growing_families_everyday_me_arabia.users` i
        left join `dbce-c360-segamaws-prod-9669.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join opts on opts.user_id=i.id
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
        where date(_PARTITIONTIME) <= date({event_date[env]}) 
        and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "363" ELSE cast(i.marketing_program_number as string) END="363"
        group by  1, 2
        )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention_AMA.arb_growing_families_everyday_me_arabia_retention_segment` )    
        '''

    elif base_mpn == '293':
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        WITH 
        opts AS (
        SELECT
        co.user_id,
        CASE WHEN email_subscription_service_name IN (
        "Germany GHH Newsletter",
        "Germany GHH WordOfMouth",
        "Germany Victoria50 Newsletter",
        "Germany Olay Newsletter",
        "Germany OralB Newsletter") 
        THEN "Germany GHH Newsletter"
        ELSE email_subscription_service_name 
        END AS opt_service_name,
        email_subscription_opt_indicator AS opt_ind,
        email_consumer_choice_date AS opt_date,
        timestamp
        FROM
        `dbce-c360-segglblws-prod-b902.personas_deu_growing_families.changed_opt_status` co
        WHERE 1=1
        AND DATE(co.timestamp) <= date({event_date[env]})
        AND
        (
        email_subscription_service_name in (
        "Germany Corporate Newsletter",
        "Germany GHH Newsletter",
        "Germany GHH WordOfMouth",
        "Germany Victoria50 Newsletter",
        "Germany Olay Newsletter",
        "Germany OralB Newsletter")
        OR LOWER(email_subscription_service_name) = LOWER("Universal")
        )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY co.user_id,email_subscription_service_name ORDER BY email_consumer_choice_date DESC) = 1
        )
        SELECT
        CASE WHEN country is null  THEN {country_code}
        ELSE country END as country,
        user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id,ghh_opt,corp_opt from(
        select country_code as country
        ,Case
        when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`)
        then i.id
        else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as    email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
        ,ARRAY_AGG(case when opts.opt_service_name = "Germany Corporate Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.opt_date desc LIMIT 1)[OFFSET (0)] as corp_opt
        ,ARRAY_AGG(case when opts.opt_service_name = "Germany GHH Newsletter" then opts.opt_ind
        when LOWER(opts.opt_service_name) = "universal" then false
        END IGNORE NULLS ORDER BY opts.opt_date desc LIMIT 1)[OFFSET (0)] as ghh_opt
        FROM `dbce-c360-segglblws-prod-b902.personas_deu_growing_families.users` i
        left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join opts on opts.user_id=i.id
        where date(_PARTITIONTIME) <= date({event_date[env]}) and i.marketing_program_number in ('293','268')  
        group by  1, 2
        )where user_id NOT IN (select user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.deu_growing_families_retention_segment`)
        '''

    elif base_mpn == "119":
        segment_query_dump = f'''
        CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.CDP_Retention.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS 
        select country,
        user_id, email_valid_contact_point_indicator,registration_date,lower(email) as email,gender,source_id
        , email_newsletter_opt_indicator, email_on_demand_opt_indicator, email_treo_newsletter_opt_indicator
        from(
        select IFNULL(country_code,'USA') as country
        ,Case when cdp.canonical_pg_id is not null or cdp1.canonical_Pg_id is not null then coalesce(cdp1.canonical_pg_id,cdp.canonical_pg_id)
        when i.id in (select id_value from `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.unmerge_list_{constants.BIGQUERY_REGION_VARIABLE[region]}_final_{constants.UNMERGE_TABLE[env]}`) then i.id
        else coalesce(m.canonical_user_id, i.id) end as user_id
        ,ARRAY_AGG(i.registration_date IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as registration_date
        ,ARRAY_AGG(i.email_valid_contact_point_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_valid_contact_point_indicator
        ,ARRAY_AGG(i.country_code IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as country_code
        ,ARRAY_AGG(i.email IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email
        ,ARRAY_AGG(i.gender IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as gender
        ,ARRAY_AGG(i.source_id IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as source_id
        ,ARRAY_AGG(i.email_newsletter_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_newsletter_opt_indicator ,ARRAY_AGG(i.email_on_demand_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_on_demand_opt_indicator ,ARRAY_AGG(i.email_treo_newsletter_opt_indicator IGNORE NULLS ORDER BY i.loaded_at desc LIMIT 1)[OFFSET (0)] as email_treo_newsletter_opt_indicator
        FROM `dbce-c360-segamerws-prod-35ac.personas_usa_gillette_v_2.users` i
        left join `dbce-c360-segamerws-prod-35ac.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.id
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_email` cdp on (cdp.email=i.email and cast(cdp.marketing_program_number as string) in ({mpn}))
        left join `dbce-c360-isl-preprod-d942.{constants.UNMERGE_DATASET[region]}.CDP_merged_list_{constants.UNMERGE_TABLE[env]}_user_id` cdp1 on (cdp1.user_id=i.id and cast(cdp1.marketing_program_number as string) in ({mpn}))
        where i.source_id not in (
        "1453", "1438", "1443", "11598", "14433", "12535", "1454", "12536", "31337", "1405", "12533", "1439", "12531", "1450", "11323", "1228", "14514", "12534", "12118", "13718", "14513", "12537", "14149", "11622", "12855", "13814", "11621", "11617", "10410", "11480", "1446", "12530", "1242", "11424", "14498", "12266", "11891", "15068", "12264", "14293", "14272", "12265", "10441", "14434", "9993", "11742", "1441", "14614", "1424", "1423", "1492", "1299", "11227", "12282", "12540", "12281", "1419", "11619", "12190", "10677", "1343", "1109", "14334", "12191", "12174", "14685", "1460", "11556", "11050", "12237", "10188", "1552", "12539", "12168", "14418", "14648", "12173", "11453", "13699", "14671", "10702", "1537", "1898", "10251", "14578", "11047", "10191", "12216", "13489", "10260", "7870", "11694", "13454", "13453", "1491", "11640", "1779", "11743", "14756", "12318", "13805", "1230", "14622", "10117", "999999", "11766", "10671", "0", "10193", "14997", "10718", "1688", "10856", "10170", "10538", "7782", "14796", "13731", "9988888", "10542", "10045", "11769", "1749", "Csaba", "7785", "https://patriotsseasonticketssweeps.com/over", "DSWB")
        and CASE WHEN i.email_valid_contact_point_indicator is not null THEN "119" ELSE i.marketing_program_number END="119"
        group by  1, 2
        )where user_id NOT IN (select distinct user_id from `dbce-c360-isl-preprod-d942.CDP_Retention.usa_gillette_retention_segment` )    
        '''

    print("Segment Audience Dumps Query : ",segment_query_dump)
    _ = client.query(segment_query_dump).to_dataframe()

    #Generate Segment Audience Counts Query
    email_contactable = f'''
    select "Email Contactable" AS audience_name, count(distinct u.user_id) as count
    from `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` u
    where 1=1
    and u.country IN ({country_code})
    and (u.email is not null and u.email <> " ")
    and (u.email_valid_contact_point_indicator is null or u.email_valid_contact_point_indicator <> "N")
    and (
    '''

    opt_ind = '''( {} = 'true' )'''
    opt_ind_block=[opt_ind.format(f'lower(cast({i} as string) )') for i in opt_indicator]
    opt_ind_block = ' OR '.join(map(str,opt_ind_block))

    eu_corp_opt_ind = f"(lower(cast({ghh_indicator} as string) ) = 'true' )  or (lower(cast({corp_indicator} as string) ) = 'true' and  {ghh_indicator} is null )"

    email_optins = f'''
    )
    UNION ALL

    select "Email Optins" AS audience_name, count(distinct u.user_id) as count
    from `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` u
    where 1=1
    and u.country IN ({country_code})
    and (u.email is not null and u.email <> " ")
    and (
    '''

    total_registered_consumers = f'''
    )
    UNION ALL
    SELECT "Total Registered Consumers" AS audience_name,count(distinct user_id) as count
    from `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` u
    where 1=1
    and u.country IN ({country_code}) and date(u.registration_date) < date({event_date[env]})

    UNION ALL

    select "Consumers Registered After January 1st 2023" as audience_name,count(distinct user_id) as count
    from `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` u
    where 1=1
    and u.country IN ({country_code})
    and date(u.registration_date) < date({event_date[env]}) and date(u.registration_date) >= date("2023-01-01")
    UNION ALL

    select "Gender Counts F" as audience_name,count(distinct user_id) as count
    from `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` u
    where 1=1
    and u.country IN ({country_code})
    and u.gender = "F" and u.registration_date is not null
    '''

    email_opened = f'''
    UNION ALL
    select "Email Opened in Past 6 Months" as audience_name,count(distinct coalesce(m.canonical_user_id, i.user_id)) as count
    from `{constants.PROJECT_TABLES[region]}.{config[base_mpn]['dataset']}.email_opened` i
    left join `{constants.PROJECT_TABLES[region]}.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.user_id
    where date(_PARTITIONTIME) <= date({event_date[env]})
    and country_code in ({country_code})
    and date(i.original_timestamp) > date_sub({event_date[env]},interval 6 MONTH)    

    UNION ALL
    SELECT "Profiling Email Optins" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` u
    where 1=1
    and u.country IN ({country_code})
    and (u.email is not null and u.email <> " ")
    and
    '''

    profiling_opt_ind = '''( {} = 'true' )''' 
    profiling_opt_ind_block = [profiling_opt_ind.format(f'lower(cast({profiling_opt_indicator} as string) )') ]
    profiling_opt_ind_block = ' OR '.join(map(str, profiling_opt_ind_block))

    receipt_verified = f"""
    union all
    SELECT "Receipt Verified Between 1st Dec and 31st Dec 2023" as audience_name,count(distinct coalesce(m.canonical_user_id, i.user_id)) as count FROM 
    `dbce-c360-segglblws-prod-b902.personas_pol_growing_families.receipt_verified` i
    left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.user_id
    where  original_timestamp between "2023-01-01" and "2023-12-31"

    union all
    SELECT "Registered Consumers for 13718 FB Lead Ads sourceId" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt 
    WHERE source_id in ('13718') and registration_date is not null

    union all
    SELECT "Oral Who Owns Electric Toothbrush Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_oral_who_owns_electric_toothbrush_id_value is not null

    union all
    SELECT "Brush Type Used Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_brush_type_used_id_value is not null

    union all
    SELECT "Number Children Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_number_children_id_value is not null

    union all
    SELECT "Femcare Product Used Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_femcare_product_used_id_value is not null

    union all
    SELECT "Primarily RemoveHair Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_primarily_remove_hair_id_value is not null

    union all
    SELECT "Sustainable Product Features Preference Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_sustainable_product_features_preference_id_value is not null

    union all
    SELECT "How You Wash Dishes Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_how_you_wash_dishes_id_value is not null
    """

    trait_shave_id_legs_value = f"""
    union all
    SELECT "How shave legs Id Value contains Lametta" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_how_shave_legs_id_value) like '%lametta%'

    union all
    SELECT "Trait detergent type id value contains Liquido" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_detergent_type_id_value) like '%liquido%'

    union all
    SELECT "Trait Toothbrush Type Used Id Value contains Elettrico or Electric" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_toothbrush_type_used_id_value) like '%elettrico%' or lower(trait_toothbrush_type_used_id_value) like '%electric%'

    union all
    SELECT "Trait Method Of Shaving Id Value contains Manuale" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_method_of_shaving_id_value) like '%manuale%'

    union all
    SELECT "Brand Purchased Toothpaste P12M contains Zend" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_brand_purchased_toothpaste_p12mid_value) like '%zend%'

    union all
    SELECT "Brand Purchased Shampoo contains Elvive" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_brand_purchased_shampoo_id_value) like '%elvive%'

    union all
    SELECT "Use Conditioner equals Si or si" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_use_conditioner_id_value = 'Si' or trait_use_conditioner_id_value =  'si'

    union all
    SELECT "Brand Purchased Toothpaste P12M contains AZ" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_brand_purchased_toothpaste_p12mid_value) like '%az%'

    union all
    SELECT "Brand Electric Shaver Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_brand_electric_shaver_id_value is not null

    union all
    SELECT "Sleep Aid Brand P12M contains Zzz" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_sleep_aid_brand_p12mid_value) like '%zzz%'
    """

    trait_number_of_children = f"""
    union all
    SELECT "Meta lead ads registration between 1st July 23 to 1st June 24" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt 
    WHERE source_id in ('14514') and registration_date between "2023-07-01" and "2024-06-01"

    union all
    SELECT "Users with receipt verified event as between 1st Sept to 10th Jan" AS audience_name, count(distinct coalesce(m.canonical_user_id, i.user_id)) as count 
    from `dbce-c360-segglblws-prod-b902.personas_grc_growing_families.receipt_verified` i
    left join `dbce-c360-segglblws-prod-b902.DataTeam.canonical_user_id_table` m on m.previous_user_id = i.user_id
    where original_timestamp between "2023-09-01" and "2024-01-10"

    union all
    SELECT "Number of children Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_number_children_id_value is not null

    union all
    SELECT "ShaveCare Brand Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_brandof_preference_id_value is not null

    union all
    SELECT "PowerOralCare Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_power_toothbrush_model_id_value is not null

    union all
    SELECT "All Femcare used line ups Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_femcare_brand_used_p3mid_value is not null

    union all
    SELECT "Dishwasher owners Exists marked as yes" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(trait_own_dishwasher_id_value) like "%yes%"

    union all
    SELECT "Pet owners for Home care Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_own_pet_id_value is not null

    union all
    SELECT "Automatic Dish Washer Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_automatic_dish_washer_detergent_brand_do_you_buy_id_value is not null

    union all
    SELECT "Hand Dish Washer Exists" AS audience_name, count(distinct user_id) as count
    FROM `dbce-c360-isl-preprod-d942.{constants.RETENTION_DATASET[region]}.seg_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where trait_hand_dish_washer_brand_do_you_buy_id_value is not null
    """

    if region == 'US' or region == 'AMA':
        segment_audience_query = email_contactable + opt_ind_block + email_optins + opt_ind_block + total_registered_consumers

    elif region == 'EU':
        segment_audience_query = email_contactable + opt_ind_block + email_optins + opt_ind_block + total_registered_consumers + email_opened + profiling_opt_ind_block 

    elif region == 'EU_corp' and base_mpn != '249' and base_mpn != '289' and base_mpn !='292':
        segment_audience_query = email_contactable + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_registered_consumers + email_opened + profiling_opt_ind_block 

    elif region == 'EU_corp' and base_mpn == "249":
        segment_audience_query = email_contactable + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_registered_consumers + email_opened + profiling_opt_ind_block + receipt_verified

    elif region == 'EU_corp' and base_mpn == "289":
        segment_audience_query = email_contactable + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_registered_consumers + email_opened + profiling_opt_ind_block + trait_shave_id_legs_value

    elif region == 'EU_corp' and base_mpn == "292":
        segment_audience_query = email_contactable + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_registered_consumers + email_opened + profiling_opt_ind_block + trait_number_of_children

    print("Segment Audience Query : ",segment_audience_query)
    segment_audience_counts_df = client.query(segment_audience_query)
    results = segment_audience_counts_df.result()

    segment_audience_counts = {}
    for row in results:
        segment_audience_counts.update({row[0]:row[1]})

    for index in opt_indicator:
        if index == "null":
            segment_audience_counts.update({"Email Optins" : 0})
            segment_audience_counts.update({"Email Contactable" : 0})

    #Insert Segment Counts to Bigquery Table
    segment_counts_insert_query = f'''
    INSERT INTO  `dbce-c360-isl-preprod-d942.cdp_migration.{constants.AUDIENCE_DASHBOARD_TABLE_NAME[env]}` 
    '''

    insert_query = '''SELECT CURRENT_TIMESTAMP() as timestamp,"{}" AS MP_Name,"{}" AS audience_name, {} AS count, "Segment" AS application_name'''
    insert_query_block = [insert_query.format(config[base_mpn]['mp_name'],item,segment_audience_counts[item]) for item in segment_audience_counts]
    insert_query_block = ' UNION ALL '.join(map(str, insert_query_block))

    print("Segment Counts Insert Query : ", segment_counts_insert_query + insert_query_block)
    _ = client.query(segment_counts_insert_query + insert_query_block).to_dataframe()
