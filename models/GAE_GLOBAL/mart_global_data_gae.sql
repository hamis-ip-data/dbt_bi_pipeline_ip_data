
{{
  config(
    materialized = 'table',
    )
}}

with stg_socialads_consolidation as (
  select
    Date,
    Data_Source_name,
    Data_Source_type,
    Media_type,
    account,
    cast(account_id as string) as account_id,
    cast(adversiter_id as string) as adversiter_id,
    advertiser,
    campaign_name,
    cast(campaign_id as string) as campaign_id,
    ad_group,
    ad_group_id,
    ad_id,
    ad_name,
    impressions,
    clicks,
    cost

from {{ ref('stg_socialads_consolidation') }}
 ) , 

data_from_adform as ( 

  select
    date,
    'Adform' as Data_Source_name , 
    'Adserver' as data_source_type, 
    data_source_type as Media_type, 
    advertiser_name as account ,
    advertiser_name as account_id ,
    advertiser_name as adversiter_id,
    advertiser_name as advertiser, 
    campaign_name,
    cast(campaign_id as string) as campaign_id,
    placement_name as ad_group,
    cast(placement_id as string) as ad_group_id,
    cast(creative_id as string) as ad_id,
    creative_name as ad_name,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(media_cost) as cost 

 from {{ ref('prd_adform_consolidation') }}
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

)

select * from stg_socialads_consolidation
union all 
select * from data_from_adform