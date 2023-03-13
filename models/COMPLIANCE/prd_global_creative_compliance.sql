

{{
  config(
    materialized = 'table',
    )
}}

  select
    campaign_name,
    campaign_id,
    placement_name,
    placement_id,
    placement_type,
    creative_id,
    creative_name,
    creative_asset_type,
    check_creative_asset_type,
    creative_format_name,
    check_creative_format_name,
    creative_format_size,
    check_creative_format_size,
    total_check,
    check_status

  from {{ ref('prd_display_creative_compliance') }}

union all 

  select
    campaign_name,
    campaign_id,
    placement_name,
    placement_id,
    placement_type,
    creative_id,
    creative_name,
    creative_asset_type,
    check_creative_asset_type,
    creative_format_name,
    check_creative_format_name,
    creative_format_size,
    check_creative_format_size,
    total_check,
    check_status

  from   {{ ref('prd_social_creative_compliance') }}