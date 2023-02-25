{{
  config(
    materialized = 'table',
    )
}}

with data_floodlight as (
  select
    Date,
    Data_Source_type,
    Data_Source_id,
    Data_Source_name,
    Data_Source,
    Account__DoubleClick_Search,
    Account_ID__DoubleClick_Search as account_id,
    Ad_group__DoubleClick_Search as ad_group,
    Ad_group_ID__DoubleClick_Search as ad_group_id,
    Advertiser_ID__DoubleClick_Search as advertiser_id,
    Advertiser__DoubleClick_Search as advertiser_name,
    Agency_ID__DoubleClick_Search as agency_id,
    Agency__DoubleClick_Search as agency,
    Campaign_ID__DoubleClick_Search as campaign_id,
    Campaign__DoubleClick_Search as campaign,
    Engine__DoubleClick_Search as engine,
    Floodlight_activity__DoubleClick_Search as floodlight,
    Floodlight_activity_ID__DoubleClick_Search as floodlight_id,
    Floodlight_activity_tag__DoubleClick_Search as floodlight_tag,
    Floodlight_group__DoubleClick_Search as floodlight_group,
    Floodlight_group_ID__DoubleClick_Search as floodlight_group_id,
    sum(Actions__DoubleClick_Search) as actions_ds,

  from {{ source('gae_ds_data', 'gae_ds_floodlight_export_*') }}

  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
)

select 
     concat(date,'_', advertiser_id,'_',campaign_id,'_',ad_group_id) as info_key, 
     *
 from data_floodlight

