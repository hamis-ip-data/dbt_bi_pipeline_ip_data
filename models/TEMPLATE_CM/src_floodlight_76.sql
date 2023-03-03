
{{
  config(
    materialized = 'table',
    )
}}

select 
  concat(date,'_',advertiser_id,'_',campaign_id,'_',site_id,'_',placement_id,'_',ad_id,'_',creative_id) as info_key,
  *
  from {{ source('template_cm', 'floodlight_76') }}