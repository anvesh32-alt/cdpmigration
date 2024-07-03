from google.cloud import bigquery,storage
import json
from helpers import constants
import logging


def cdp_audience_counts(**kwargs):
    
    base_mpn = kwargs['mpn']
    env = kwargs['env']
    client = bigquery.Client(constants.ISL_PROD_PROJECT)
    client_storage = storage.Client(constants.ISL_PROD_PROJECT)
    bucket = client_storage.get_bucket(constants.DAG_BUCKET)
    config = json.loads(bucket.get_blob("dags/cdp2.0-migration/src/configurations/audienceconfigfile.json").download_as_string())
    logging.info("Initializing with Bigquery and bucket completed Successfully.")

    mpn = config[base_mpn]['cdp_multiple_mpn']
    region = config[base_mpn]['region']
    cdp_dumps_dataset = constants.RETENTION_DATASET[region]
    retention_table = config[base_mpn]["retention_table"]
    event_date = {"preprod" : f"'{config[base_mpn]['date']}'", "preprodlive" : "current_date", "prod" : f"'{config[base_mpn]['date']}'"}


    #Query to get computed trait id and computed trait name from CRS
    crs_query = f'''
    select distinct ecoTraitName as traitName,tm.traitId as traitId from
    `dbce-c360-isl-preprod-d942.CDS.marketing_program_eco_system_trait_map` as tm
    join `dbce-c360-isl-preprod-d942.CDS.trait` AS tr
    on tm.traitId=tr.traitId
    join `dbce-c360-isl-preprod-d942.CDS.ecosystemtrait` est
    on tm.traitId=est.traitId
    and tm.ecoSystemId=est.ecoSystemId
    where tm.ecoSystemId =10 and tm.marketingProgramId in ({mpn}) and lower(tr.traitName) LIKE "email%opt%ind%"
    '''

    cdp_opt_indicator = []
    corp_indicator = "''"
    ghh_indicator = "''"
    profiling_opt_indicator = "''"

    if 'cdp_trait_id' in config[base_mpn]:
      cdp_opt_indicator = config[base_mpn]['cdp_opt_indicator']

    else:
      traits_list_df = client.query(crs_query).to_dataframe()
      cdp_opt_indicator = list(traits_list_df['traitName']) 

    for index in cdp_opt_indicator:
      if "Corporate" in index:
          corp_indicator = index

      if "GrowingFamiliesNewsletter" in index:
        ghh_indicator = index

      if "ProfilingOptIndicator" in index:
        profiling_opt_indicator = index

    #Create CDP Audience Dump Query
    cdp_dump_query = f"""
    CREATE OR REPLACE TABLE `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` AS
    WITH 
    ret_table AS (select distinct user_id from `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.{retention_table}`),
    id_graph as (select * from `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.id_graphs_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` ),
    profile_aliases as (select * from `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_aliases_{constants.AUDIENCE_DUMPS_TABLE[env]}`),
    profile_traits as (select * from `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_traits_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`),
    non_ret_cdp as (
    select distinct pg_id from (
    select idg.pg_id,idg.id_value as user_id, case when ret.user_id is not null then 1 else 0 end as ret_out
    from id_graph idg
    left join ret_table ret on ret.user_id=idg.id_value
    ) where ret_out=0
    ),
    ret_cdp as (
    select distinct idg.pg_id
    from id_graph idg
    left join non_ret_cdp nret on nret.pg_id=idg.pg_id
    where nret.pg_id is null
    ),
    pt as (
    SELECT distinct marketing_program_number,trait_name,trait_value,pg_id,trait_id FROM (
    SELECT
    marketing_program_number,
    pg_id,
    a.trait_id,
    a.trait_name,
    ( SELECT b.trait_value FROM UNNEST(nested_value) b ORDER BY b.trait_consumer_last_updated_date DESC,b.trait_expiration_date DESC,b.last_updated_date DESC,b.trait_value DESC LIMIT 1)
    trait_value,
    last_updated_date,
    trace_id
    FROM (
    SELECT
    a.marketing_program_number,
    b.canonical_pg_id AS pg_id,
    a.trait_id,
    a.trait_name,

    ARRAY_AGG(STRUCT (a.trait_value AS trait_value,
    a.trait_consumer_last_updated_date,
    a.last_updated_date,
    a.trait_expiration_date)) nested_value,
    MAX(a.last_updated_date) last_updated_date,
    ARRAY_AGG(a.trace_id) trace_id
    FROM
    profile_aliases b
    INNER JOIN
    (SELECT a.pg_id,max(a.last_updated_date) last_updated_date
    from profile_aliases a
    group by 1) b1
    on b.pg_id=b1.pg_id
    and b.last_updated_date=b1.last_updated_date
    INNER JOIN
    (select a.consumerid_sourceid_mpn,a.pg_id,a.trait_id,a.marketing_program_number,a.trait_name,
    max(a.trait_consumer_last_updated_date) trait_consumer_last_updated_date,
    max(a.trait_expiration_date) trait_expiration_date,
    max(a.last_updated_date) last_updated_date, max(a.trace_id) trace_id,string_agg(a.trait_value) trait_value
    from profile_traits a
    group by 1,2,3,4,5) a

    ON
    b.pg_id=a.pg_id

    GROUP BY
    a.marketing_program_number,
    b.canonical_pg_id,
    a.trait_id,
    a.trait_name
    ) a
    )
    where marketing_program_number in ( {mpn} ) 
    )

    SELECT
    pg_id,
    ARRAY_AGG(userId IGNORE NULLS)[OFFSET (0)] as userId,
    ARRAY_AGG(email IGNORE NULLS)[OFFSET (0)] as email,
    ARRAY_AGG(gender IGNORE NULLS)[OFFSET (0)] as gender,
    ARRAY_AGG(sourceId IGNORE NULLS)[OFFSET (0)] as sourceId,
    ARRAY_AGG(registrationDate IGNORE NULLS)[OFFSET (0)] as registrationDate,
    ARRAY_AGG(countryCode IGNORE NULLS)[OFFSET (0)] as countryCode,
    ARRAY_AGG(emailValidContactPointIndicator IGNORE NULLS)[OFFSET (0)] as emailValidContactPointIndicator,
    """

    opt_arr_agg_func='''ARRAY_AGG({} IGNORE NULLS )[OFFSET (0)] as {},'''
    opt_arr_agg=[opt_arr_agg_func.format(i,i) for i in cdp_opt_indicator]
    opt_arr_agg = ' '.join(map(str,opt_arr_agg))

    trait = f"""
    FROM
    (
    select distinct pg_id,marketing_program_number,
    case when trait_name ='userId'  then trait_value end as userId,
    case when trait_name ='email'  then trait_value end as email,
    case when trait_name ='sourceId'  then trait_value end as sourceId,
    case when trait_name ='gender'  then trait_value end as gender,
    case when trait_name ='registrationDate'  then trait_value end as registrationDate,
    case when trait_name ='countryCode'  then trait_value end as countryCode,
    """

    opt_case_statment='''case when trait_name ='{}'  then trait_value end as  {},'''
    opt_case=[opt_case_statment.format(i,i) for i in cdp_opt_indicator]
    opt_case = ' '.join(map(str,opt_case))

    retention_filter = f"""
    case when trait_name ='emailValidContactPointIndicator'  then trait_value end as emailValidContactPointIndicator
    FROM pt
    where pg_id NOT IN (SELECT pg_id FROM ret_cdp)
    )
    group by 1
    """

    cdp_dumps_query = cdp_dump_query + opt_arr_agg + trait + opt_case + retention_filter
    print("CDP Audience Dumps Query : ",cdp_dumps_query)
    _ = client.query(cdp_dumps_query).to_dataframe()

    #Generate CDP Audience Query
    all_tables_sql = f"""
    WITH
    profile_aliases as (select * from `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_aliases_{constants.AUDIENCE_DUMPS_TABLE[env]}`),

    profile_events AS ( select * from `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_events_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` ),

    profile_event_properties AS ( select * from `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.profile_event_properties_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` )
    """

    email_cont = f"""
    SELECT "Email Contactable" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt
    where 1=1
    and (
    """

    opt_ind = '''coalesce({},"default") = 'true' '''
    coalesce_opt_ind=[opt_ind.format(f'{i}') for i in cdp_opt_indicator]
    coalesce_opt_ind = ' OR '.join(map(str,coalesce_opt_ind))
    
    eu_corp_opt_ind = f'(coalesce({ghh_indicator},"default") ="true") or (coalesce({corp_indicator},"default") ="true" and  {ghh_indicator} is null )'

    email_optins = f"""
    )
    and (coalesce(emailValidContactPointIndicator,"default") = "Y" or emailValidContactPointIndicator is null)
    and (email is not null and email <> " ")
    UNION ALL

    SELECT "Email Optins" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt
    where 1=1
    and (
    """

    total_reg = f"""
    )
    and (email is not null and email <> " ")
    UNION ALL

    SELECT "Consumers Registered After January 1st 2023" AS audience_name,count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt
    WHERE registrationDate is not null and registrationDate >= '2023-01-01'
    UNION ALL

    SELECT "Total Registered Consumers" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt 
    WHERE registrationDate is not null

    UNION ALL   
    SELECT "Gender Counts F" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt
    WHERE 1=1
    AND gender = 'F'
    AND registrationDate is not null
    """

    email_opened = f"""
    UNION ALL
    Select "Email Opened in Past 6 Months" AS audience_name, count(distinct c.canonical_pg_id) as count 
    from profile_events a
    join profile_event_properties B on a.trace_id=b.trace_id
    join profile_aliases c on a.pg_id=c.pg_id
    INNER JOIN
    (SELECT pg_id,max(last_updated_date) last_updated_date
    from profile_aliases
    group by 1) b1
    on c.pg_id=b1.pg_id
    and c.last_updated_date=b1.last_updated_date
    and date(event_date) > date_sub({event_date[env]},interval 6 MONTH) and marketing_program_number in ({mpn}) and a.event_name='Email Opened'

    UNION ALL
    SELECT "Profiling Email Optins" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt
    where 1=1
    and (email is not null and email <> " ")
    and
    """

    cdp_profiling_opt_ind = """coalesce({},"default") = 'true' """
    cdp_profiling_opt_ind_block=[cdp_profiling_opt_ind.format(f'{profiling_opt_indicator}')]
    cdp_profiling_opt_ind_block = ' OR '.join(map(str,cdp_profiling_opt_ind_block))
    
    receipt_verified = f"""
    union all
    select "Receipt Verified Between 1st Dec and 31st Dec 2023" AS audience_name, count(distinct c.canonical_pg_id) as count 
    from profile_events a
    join profile_event_properties B on a.trace_id=b.trace_id
    join profile_aliases c on a.pg_id=c.pg_id
    INNER JOIN
    (SELECT pg_id,max(last_updated_date) last_updated_date
    from profile_aliases
    group by 1) b1
    on c.pg_id=b1.pg_id
    and c.last_updated_date=b1.last_updated_date
    and a.event_date between "2023-01-01" and "2023-12-31" and marketing_program_number in ({mpn}) and a.event_name='Receipt Verified'

    union all
    SELECT "Registered Consumers for 13718 FB Lead Ads sourceId" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt 
    WHERE sourceId in ('13718') and registrationDate is not null

    union all
    SELECT "Oral Who Owns Electric Toothbrush Exists" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`  pt 
    where OralWhoOwnsElectricToothbrush is not null

    union all
    SELECT "Brush Type Used Exists" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`  pt 
    where brushTypeUsed is not null

    union all
    SELECT "Number Children Exists" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`  pt 
    where numberChildren is not null

    union all
    SELECT "Femcare Product Used Exists" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`  pt 
    where femcareProductUsed is not null

    union all
    SELECT "Primarily RemoveHair Exists" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`  pt 
    where PrimarilyRemoveHair is not null

    union all
    SELECT "Sustainable Product Features Preference Exists" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`  pt 
    where SustainableProductFeaturesPreference is not null

    union all
    SELECT "How You Wash Dishes Exists" AS audience_name, count(distinct pt.pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}`  pt 
    where howYouWashDishes is not null
    """

    trait_shave_id_legs_value = f"""
    union all
    SELECT "How shave legs Id Value contains Lametta" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(howShaveLegs) like '%lametta%'

    union all
    SELECT "Trait detergent type id value contains Liquido" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(detergent) like '%liquido%'

    union all
    SELECT "Trait Toothbrush Type Used Id Value contains Elettrico or Electric" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(toothbrushTypeUsed) like '%elettrico%' or lower(toothbrushTypeUsed) like '%electric%'

    union all
    SELECT "Trait Method Of Shaving Id Value contains Manuale" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(MethodOfShaving) like '%manuale%'

    union all
    SELECT "Brand Purchased Toothpaste P12M contains Zend" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(BrandPurchasedToothpasteP12M) like '%zend%'

    union all
    SELECT "Brand Purchased Shampoo contains Elvive" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(BrandPurchasedShampoo) like '%elvive%'

    union all
    SELECT "Use Conditioner equals Si or si" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where UseConditioner = 'Si' or UseConditioner =  'si'

    union all
    SELECT "Brand Purchased Toothpaste P12M contains AZ" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(BrandPurchasedToothpasteP12M) like '%az%'

    union all
    SELECT "Brand Electric Shaver Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where BrandElectricShaver is not null

    union all
    SELECT "Sleep Aid Brand P12M contains Zzz" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(SleepAidBrandP12M) like '%zzz%'
    """

    trait_number_of_children = f"""
    union all
    SELECT "Meta lead ads registration between 1st July 23 to 1st June 24" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` pt 
    WHERE sourceId in ('14514') and registrationDate between "2023-07-01" and "2024-06-01"

    union all
    select "Users with receipt verified event as between 1st Sept to 10th Jan" AS audience_name, count(distinct c.canonical_pg_id) as count 
    from profile_events a
    join profile_event_properties B on a.trace_id=b.trace_id
    join profile_aliases c on a.pg_id=c.pg_id
    INNER JOIN
    (SELECT pg_id,max(last_updated_date) last_updated_date
    from profile_aliases
    group by 1) b1
    on c.pg_id=b1.pg_id
    and c.last_updated_date=b1.last_updated_date
    and a.event_date between "2023-09-01" and "2024-01-10" and marketing_program_number in ({mpn}) and a.event_name='Receipt Verified'

    union all
    SELECT "Number of children Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where numberChildren is not null

    union all
    SELECT "ShaveCare Brand Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where BrandofPreference is not null

    union all
    SELECT "PowerOralCare Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where PowerToothbrushModel is not null

    union all
    SELECT "All Femcare used line ups Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where FemcareBrandUsedP3M is not null

    union all
    SELECT "Dishwasher owners Exists marked as yes" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where lower(OwnDishwasher) like "%yes%"

    union all
    SELECT "Pet owners for Home care Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where ownPet is not null

    union all
    SELECT "Automatic Dish Washer Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where AutomaticDishWasherDetergentBrandDoYouBuy is not null

    union all
    SELECT "Hand Dish Washer Exists" AS audience_name, count(distinct pg_id) as count
    FROM `dbce-c360-isl-preprod-d942.{cdp_dumps_dataset}.cdp_dump_{base_mpn}_{constants.AUDIENCE_DUMPS_TABLE[env]}` 
    where HandDishWasherBrandDoYouBuy is not null
    """

    if region == 'US' or region == 'AMA':
      cdp_audience_query = email_cont + coalesce_opt_ind + email_optins + coalesce_opt_ind + total_reg
    
    elif region == 'EU':
      cdp_audience_query = all_tables_sql + email_cont + coalesce_opt_ind + email_optins + coalesce_opt_ind + total_reg + email_opened + cdp_profiling_opt_ind_block

    elif region == 'EU_corp' and base_mpn != '249' and base_mpn != '289' and base_mpn != '292':
      cdp_audience_query = all_tables_sql + email_cont + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_reg + email_opened + cdp_profiling_opt_ind_block

    elif region == 'EU_corp' and base_mpn == "249":
      cdp_audience_query = all_tables_sql + email_cont + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_reg + email_opened + cdp_profiling_opt_ind_block + receipt_verified

    elif region == 'EU_corp' and base_mpn == "289":
      cdp_audience_query = all_tables_sql + email_cont + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_reg + email_opened + cdp_profiling_opt_ind_block + trait_shave_id_legs_value

    elif region == 'EU_corp' and base_mpn == "292":
      cdp_audience_query = all_tables_sql + email_cont + eu_corp_opt_ind + email_optins + eu_corp_opt_ind + total_reg + email_opened + cdp_profiling_opt_ind_block + trait_number_of_children


    print("CDP Audience Counts Query : ",cdp_audience_query)
    cdp_audience_counts_df = client.query(cdp_audience_query)
    results = cdp_audience_counts_df.result()
    cdp_audience_counts = {}
    for row in results:
        cdp_audience_counts.update({row[0]:row[1]})

    #Insert CDP Counts to Bigquery Table
    cdp_counts_insert_query = f'''
    INSERT INTO  `dbce-c360-isl-preprod-d942.cdp_migration.{constants.AUDIENCE_DASHBOARD_TABLE_NAME[env]}` 
    '''

    insert_query = '''SELECT CURRENT_TIMESTAMP() as timestamp,"{}" AS MP_Name,"{}" AS audience_name, {} AS count, "CDP" AS application_name'''
    insert_query_block = [insert_query.format(config[base_mpn]['mp_name'],item,cdp_audience_counts[item]) for item in cdp_audience_counts]
    insert_query_block = ' UNION ALL '.join(map(str, insert_query_block))

    print("CDP Counts Insert Query : ", cdp_counts_insert_query + insert_query_block)
    _ = client.query(cdp_counts_insert_query + insert_query_block).to_dataframe()
