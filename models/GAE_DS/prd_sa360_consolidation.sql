{{
  config(
    materialized = 'table'
    )
}}

with data_consolidation as (
select 
  info_key,
  date,
  agency,
  agency_id,
  data_source,
  account_name,
  account_id,
  advertiser_name,
  advertiser_id,
  campaign,
  campaign_id,
  ad_group,
  ad_group_id,
  engine,
  media_type,
  impressions_ds,
  impressions,
  clicks_ds,
  clicks,
  cost_ds,
  cost,
  actions_ds,
  conversions,
  visits,
  transactions
 from {{ ref('stg_sa360_data_consolidation') }}
) , 
data_floodlight as ( 

 select 
   info_key,
  Date,
  Data_Source_type,
  Data_Source_id,
  Data_Source_name,
  Data_Source,
  Account__DoubleClick_Search,
  account_id,
  ad_group,
  ad_group_id,
  advertiser_id,
  advertiser_name,
  agency_id,
  agency,
  campaign_id,
  campaign,
  engine,
  actions_ds,
  floodlight,
  floodlight_id,
  floodlight_tag,
  floodlight_group,
  floodlight_group_id

  from {{ ref('stg_sa360_floodlight_consolidation') }}
)

select 
  data_consolidation.info_key,
  data_consolidation.date,
  data_consolidation.agency,
  data_consolidation.agency_id,
  data_consolidation.data_source,
  data_consolidation.account_name,
  data_consolidation.account_id,
  data_consolidation.advertiser_name,
  data_consolidation.advertiser_id,
  data_consolidation.campaign,
  data_consolidation.campaign_id,
  data_consolidation.ad_group,
  data_consolidation.ad_group_id,
  data_consolidation.engine,
  data_consolidation.media_type,
  data_consolidation.impressions_ds,
  data_consolidation.impressions,
  data_consolidation.clicks_ds,
  data_consolidation.clicks,
  data_consolidation.cost_ds,
  data_consolidation.cost,
  data_consolidation.actions_ds,
  data_consolidation.conversions,
  data_consolidation.visits,
  data_consolidation.transactions , 
  data_floodlight.actions_ds as actions_ds_floodlight,
  data_floodlight.floodlight,
  data_floodlight.floodlight_id,
  data_floodlight.floodlight_tag,
  data_floodlight.floodlight_group,
  data_floodlight.floodlight_group_id

  from data_consolidation
  left join data_floodlight
  on data_consolidation.info_key = data_floodlight.info_key


