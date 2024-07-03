crs_sql_query = """
SELECT
  `mpom`.`contact_point_type_code` AS `contact_point_type_code`,
  `mpom`.`contact_point_category_code` AS `contact_point_category_code`,
  `mpom`.`cdp_marketing_program_number` AS `marketing_program_number`,
  `mpom`.`service_name` AS `service_name`,
  `sot`.`trait_id_opt_indicator` AS `trait_id_opt_indicator`,
  `sot`.`trait_id_opt_date` AS `trait_id_opt_date`,
  `cpt`.`contact_point_type_name` AS `contact_point_type_name`
FROM
  `dbce-c360-segamerws-prod-35ac.CDS.marketing_program_opt_map` `mpom`
LEFT JOIN
  `dbce-c360-segamerws-prod-35ac.CDS.marketing_program_service` `mps`
ON
  `mpom`.`cdp_marketing_program_number` = `mps`.`marketing_program_number`
  AND `mpom`.`service_name` = `mps`.`service_name`
LEFT JOIN
  `dbce-c360-segamerws-prod-35ac.CDS.contact_point_category` `cpc`
ON
  `mpom`.`contact_point_category_code` = `cpc`.`contact_point_category_code`
LEFT JOIN
  `dbce-c360-segamerws-prod-35ac.CDS.contact_point_type` `cpt`
ON
  `cpc`.`contact_point_type_code` = `cpt`.`contact_point_type_code`
LEFT JOIN (
  SELECT
    DISTINCT `sot`.`service_name` AS `service_name`,
    SUBSTR(`est1`.`traitName`,1,5) AS `trait_name_sub`,
    `sot`.`default_opt_frequency` AS `default_opt_frequency`,
    `sot`.`service_scope` AS `service_scope`,
    GREATEST(COALESCE(`sot`.`last_update_date`,'1000-01-01'), COALESCE(`est1`.`lastUpdateDate`,'1000-01-01')) AS `last_updated_date`,
    (CASE
        WHEN (`est1`.`traitId` IS NOT NULL) THEN `est1`.`traitName`
    END
      ) AS `trait_id_opt_indicator`,
    (CASE
        WHEN (`est2`.`traitId` IS NOT NULL) THEN `est2`.`traitName`
    END
      ) AS `trait_id_opt_date`
  FROM ((`dbce-c360-segamerws-prod-35ac.CDS.service_opt_traits` `sot`
      LEFT JOIN
        `dbce-c360-segamerws-prod-35ac.CDS.trait` `est1`
      ON
        ((`est1`.`traitId` = `sot`.`trait_id_opt_indicator`)))
    LEFT JOIN
      `dbce-c360-segamerws-prod-35ac.CDS.trait` `est2`
    ON
      ((cast(`est2`.`traitId` as STRING) = `sot`.`trait_id_opt_date`)))) `sot`
ON
  (((`mps`.`service_name` = `sot`.`service_name`)
      AND ((SUBSTR(`cpt`.`contact_point_type_name`,1,5) = `sot`.`trait_name_sub`)
        OR (`sot`.`trait_name_sub` = 'regul'))))
"""