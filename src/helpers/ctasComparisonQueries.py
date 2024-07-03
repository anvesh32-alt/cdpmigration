ctas_traits='''
create table `dbce-c360-isl-preprod-d942.{}.{}` as
SELECT distinct user_id, trait_name, trait_value
FROM
(SELECT distinct user_id,{}
FROM `{}.{}.canonical_user_ctas`
) AS SourceTable
UNPIVOT
(trait_value FOR trait_name IN (
{}
)) AS Alias
'''

segment_traits = '''
create table `dbce-c360-isl-preprod-d942.{}.{}`
as
select * from `{}.cdp_consumer_pii_views.view_segment_traits_pg_id{}`
where marketing_program_number in ({}) ;

create or replace table `dbce-c360-isl-preprod-d942.{}.{}`
as
SELECT distinct pg_id,id_value as user_id FROM EXTERNAL_QUERY("{}.{}.cdp-ids{}_cdp-ids_pii", 
"SELECT * FROM id_graphs where marketing_program_number in ({}) and id_type = 'userId'");

create or replace table `dbce-c360-isl-preprod-d942.{}.{}`
as
select distinct pg_id,max(user_id) as userId
from `dbce-c360-isl-preprod-d942.{}.{}`
group by pg_id
having count (user_id) < 2
'''

ecosystem_trait = '''
create or replace table `dbce-c360-isl-preprod-d942.{}.{}` as
SELECT distinct traitId
,ARRAY_AGG(Segment IGNORE NULLS)[OFFSET (0)] as Segment
,ARRAY_AGG(BQ IGNORE NULLS)[OFFSET (0)] as BQ
,ARRAY_AGG(CDP2 IGNORE NULLS)[OFFSET (0)] as CDP2
FROM (
select distinct est.traitId
,CASE WHEN est.ecoSystemId=1 THEN ecoTraitName END as Segment
,CASE WHEN est.ecoSystemId=5 THEN ecoTraitName END as BQ
,CASE WHEN est.ecoSystemId=10 THEN ecoTraitName END as CDP2
from `{}.CDS.eco_system_trait` est
left join `{}.CDS.marketing_program_eco_system_trait_map` mpest on mpest.traitId=est.traitId
where
est.ecoSystemId IN (1,5,10)
and mpest.marketingProgramId in ({})
)
group by 1
order by 1
'''

seg_vs_cdp_trait = '''
create or replace table `dbce-c360-isl-preprod-d942.{}.{}` as
select distinct seg_traits.user_id, seg_traits.trait_name,seg_traits.trait_value as seg_trait_value,cdp_traits.trait_value as cdp_trait_value,cdp_traits.trait_name as cdp_trait_name,cdp_traits.pg_id,
case when trim(lower(cdp_traits.trait_value)) = trim(lower(seg_traits.trait_value)) then 1
end as matched,
case when trim(lower(cdp_traits.trait_value)) <> trim(lower(seg_traits.trait_value)) then 1
end as un_matched
from
`dbce-c360-isl-preprod-d942.{}.{}` seg_traits
left join
`dbce-c360-isl-preprod-d942.{}.{}` id_graph
on seg_traits.user_id = id_graph.user_id
left join
`dbce-c360-isl-preprod-d942.{}.{}` cdp_traits
on cdp_traits.pg_id = id_graph.pg_id and trim(lower(cdp_traits.trait_name)) = trim(lower(replace(seg_traits.trait_name, '_', '')));

create or replace table `dbce-c360-isl-preprod-d942.{}.{}` as
select distinct trait_name,sum(IFNULL(matched,0)) as match_count, sum(IFNULL(un_matched,0)) as un_match_count, (sum(IFNULL(un_matched,0))/(IFNULL(sum(matched),0.01)+IFNULL(sum(un_matched),0.01)))*100 as un_match_percent
from  `dbce-c360-isl-preprod-d942.{}.{}`
group by trait_name
having un_match_percent > 0
order by un_match_percent desc,match_count desc
'''
