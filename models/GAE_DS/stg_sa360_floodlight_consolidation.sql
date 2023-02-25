{{
  config(
    materialized = 'table',
    )
}}

 SELECT
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
FROM
    {{ ref('src_sa360_floodlight') }}
order by actions_ds desc