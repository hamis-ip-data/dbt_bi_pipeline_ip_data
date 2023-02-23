{{
  config(
    materialized = 'table',
    )
}}

with stg_adform_standards_metrics as (

  select
    info_key,
    date,
    advertiser_name,
    campaign_name,
    campaign_id,
    campaign_date,
    campaign_objective,
    campaign_type,
    campaign_type_2,
    campaign_type_3,
    campaign_quotation_id,
    site_id,
    site_name,
    placement_id,
    placement_name,
    placement_type,
    placement_type_1,
    placement_method,
    placement_site,
    placement_target,
    placement_format,
    placement_format_2,
    placement_device,
    adform_placement_name9,
    adform_placement_name10,
    creative_id,
    creative_name,
    impressions,
    clicks,
    media_cost,
    rich_media_video_plays,
    rich_media_video_first_quartile_completes,
    rich_media_video_midpoints,
    rich_media_video_third_quartile_completes,
    rich_media_video_completions,
    active_view_viewable_impressions,
    active_view_measurable_impressions,
    active_view_eligible_impressions

  from {{ ref('stg_adform_standards_metrics') }}
) , 

stg_adform_conversions_dict as (

  select
    info_key,
    date,
    advertiser_name,
    campaign_id,
    site_id,
    site_name,
    placement_id,
    placement_name,
    creative_id,
    creative_name,
    activity_name,
    ad_id,
    ad_name,
    pv_arrivee,
    pv_arrivee_no_rep,
    pc_arrivee,
    pc_arrivee_no_rep,
    pv_micro,
    pv_micro_no_rep,
    pc_micro,
    pc_micro_no_rep,
    pv_lead,
    pv_lead_no_rep,
    pc_lead,
    pc_lead_no_rep

from  {{ ref('stg_adform_conversions_dict') }}
)

select 
     distinct 
    stg_adform_standards_metrics.info_key,
    stg_adform_standards_metrics.date,
    stg_adform_standards_metrics.advertiser_name,
    stg_adform_standards_metrics.campaign_name,
    stg_adform_standards_metrics.campaign_id,
    stg_adform_standards_metrics.campaign_date,
    stg_adform_standards_metrics.campaign_objective,
    stg_adform_standards_metrics.campaign_type,
    stg_adform_standards_metrics.campaign_type_2,
    stg_adform_standards_metrics.campaign_type_3,
    stg_adform_standards_metrics.campaign_quotation_id,
    stg_adform_standards_metrics.site_id,
    stg_adform_standards_metrics.site_name,
    stg_adform_standards_metrics.placement_id,
    stg_adform_standards_metrics.placement_name,
    stg_adform_standards_metrics.placement_type,
    stg_adform_standards_metrics.placement_type_1,
    stg_adform_standards_metrics.placement_method,
    stg_adform_standards_metrics.placement_site,
    stg_adform_standards_metrics.placement_target,
    stg_adform_standards_metrics.placement_format,
    stg_adform_standards_metrics.placement_format_2,
    stg_adform_standards_metrics.placement_device,
    stg_adform_standards_metrics.adform_placement_name9,
    stg_adform_standards_metrics.adform_placement_name10,
    stg_adform_standards_metrics.creative_id,
    stg_adform_standards_metrics.creative_name,
    stg_adform_standards_metrics.impressions,
    stg_adform_standards_metrics.clicks,
    stg_adform_standards_metrics.media_cost,
    stg_adform_standards_metrics.rich_media_video_plays,
    stg_adform_standards_metrics.rich_media_video_first_quartile_completes,
    stg_adform_standards_metrics.rich_media_video_midpoints,
    stg_adform_standards_metrics.rich_media_video_third_quartile_completes,
    stg_adform_standards_metrics.rich_media_video_completions,
    stg_adform_standards_metrics.active_view_viewable_impressions,
    stg_adform_standards_metrics.active_view_measurable_impressions,
    stg_adform_standards_metrics.active_view_eligible_impressions, 
    stg_adform_conversions_dict.activity_name,
    stg_adform_conversions_dict.pv_arrivee,
    stg_adform_conversions_dict.pv_arrivee_no_rep,
    stg_adform_conversions_dict.pc_arrivee,
    stg_adform_conversions_dict.pc_arrivee_no_rep,
    stg_adform_conversions_dict.pv_micro,
    stg_adform_conversions_dict.pv_micro_no_rep,
    stg_adform_conversions_dict.pc_micro,
    stg_adform_conversions_dict.pc_micro_no_rep,
    stg_adform_conversions_dict.pv_lead,
    stg_adform_conversions_dict.pv_lead_no_rep,
    stg_adform_conversions_dict.pc_lead,
    stg_adform_conversions_dict.pc_lead_no_rep

  from stg_adform_standards_metrics
    left join stg_adform_conversions_dict
  on stg_adform_standards_metrics.info_key = stg_adform_conversions_dict.info_key  
   order by impressions desc 


   