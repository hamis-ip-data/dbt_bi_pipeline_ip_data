

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
    check_placement_type,
    placement_objective,
    check_placement_objective,
    placement_byying_method,
    check_placement_byying_method,
    placement_regie,
    check_placement_regie,
    placement_audience,
    check_placement_audience,
    placement_ciblage,
    check_placement_ciblage,
    placement_format_name,
    check_placement_format_name,
    placement_format_size,
    check_placement_format_size,
    placement_device,
    check_placement_device,
    total_check,
    check_status

  from 
      {{ ref('prd_display_placement_compliance') }}

union all 

  select
    campaign_name,
    campaign_id,
    placement_name,
    placement_id,
    placement_type,
    check_placement_type,
    placement_objective,
    check_placement_objective,
    placement_byying_method,
    check_placement_byying_method,
    placement_regie,
    check_placement_regie,
    placement_audience,
    check_placement_audience,
    placement_ciblage,
    check_placement_ciblage,
    placement_format_name,
    check_placement_format_name,
    placement_format_size,
    check_placement_format_size,
    placement_device,
    check_placement_device,
    total_check,
    check_status

  from 
    {{ ref('prd_social_placement_compliance') }}

