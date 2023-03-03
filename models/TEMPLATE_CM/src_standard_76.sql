{{
  config(
    materialized = 'table',
    )
}}

SELECT
  concat(date,'_',advertiser_id,'_',campaign_id,'_',site_id,'_',placement_id,'_',ad_id,'_',creative_id) as info_key,
  *
FROM {{ source('template_cm', 'standard_76')}}