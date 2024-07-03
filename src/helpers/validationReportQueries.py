opt_validation_base_query = """
        WITH opts as (select distinct "{}" as persona_name,
        COALESCE(cast(email_subscription_opt_number as string),
        cast(phone_subscription_opt_number as string),
        cast(postal_subscription_opt_number as string),
        cast(social_subscription_opt_number as string)) as subscription_opt_number,
        ifnull(
        COALESCE(email_subscription_service_name,phone_subscription_service_name,postal_subscription_service_name,
        social_subscription_service_name),"") as service_name
        ,CASE
        WHEN (email_subscription_opt_number is not null or email_subscription_service_name is not null) THEN "Email"
        WHEN (phone_subscription_opt_number is not null or phone_subscription_service_name is not null) THEN "Phone"
        WHEN (postal_subscription_opt_number is not null or postal_subscription_service_name is not null) THEN "Postal"
        WHEN (social_subscription_opt_number is not null or social_subscription_service_name is not null) THEN "Social"
        ELSE "NONE"
        END AS  opt_type
        ,CASE
        WHEN (email_subscription_opt_number is not null or email_subscription_service_name is not null) THEN "E"
        WHEN (phone_subscription_opt_number is not null or phone_subscription_service_name is not null) THEN "P"
        WHEN (postal_subscription_opt_number is not null or postal_subscription_service_name is not null) THEN "A"
        WHEN (social_subscription_opt_number is not null or social_subscription_service_name is not null) THEN "S"
        ELSE "NONE"
        END AS contact_point_type_code,max(timestamp) as latest_date
        from  `{}.{}.changed_opt_status`
        group by 1,2,3,4,5
        )
        ,valids as (SELECT distinct cdp_marketing_program_number,subscription_opt_number,service_name,contact_point_type_code FROM 
        `{}.CDS.marketing_program_opt_map` where cdp_marketing_program_number={})
        ,opts_counts as (
        select persona_name,subscription_opt_number,service_name,count(*) as counts from(
        select "{}" as persona_name,
        COALESCE(cast(email_subscription_opt_number as string),
        cast(phone_subscription_opt_number as string),
        cast(postal_subscription_opt_number as string),
        cast(social_subscription_opt_number as string)) as subscription_opt_number,
        ifnull(
        COALESCE(email_subscription_service_name,phone_subscription_service_name,postal_subscription_service_name,
        social_subscription_service_name),"") as service_name
        from  `{}.{}.changed_opt_status` )
        group by 1,2,3
        )
        select opts.*,oc.counts,CASE WHEN v.cdp_marketing_program_number is null THEN "false" ELSE "true" END AS is_valid
        from opts
        left join valids v on (
        CAST(v.subscription_opt_number as string)=CAST(opts.subscription_opt_number as string)
        and v.service_name=opts.service_name
        and v.contact_point_type_code=opts.contact_point_type_code)
        left join opts_counts oc on (
        CAST(oc.subscription_opt_number as string)=CAST(opts.subscription_opt_number as string)
        and CAST(oc.service_name as string)=CAST(opts.service_name as string)
        )
        where opts.subscription_opt_number<>"9999"
"""
