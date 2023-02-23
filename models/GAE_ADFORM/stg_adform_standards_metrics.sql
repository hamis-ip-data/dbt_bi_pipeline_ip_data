{{
  config(
    materialized = 'table',
    )
}}

select 
    concat(date,'_',campaign_id,'_',site_id,'_',placement_id,'_',creative_id,'_', ad_id) as info_key,
    date, 
    advertiser_name ,
    campaign_name, 
    campaign_id,
    split(substr(campaign_name, 12), '-')[safe_offset(0)] as campaign_date,
    split(substr(campaign_name, 12), '-')[safe_offset(1)] as campaign_objective,
    split(substr(campaign_name, 12), '-')[safe_offset(2)] as campaign_type,
    split(substr(campaign_name, 12), '-')[safe_offset(3)] as campaign_type_2,
    split(substr(campaign_name, 12), '-')[safe_offset(4)] as campaign_type_3,
    split(substr(campaign_name, 12), '-')[safe_offset(5)] as campaign_quotation_id,    
    site_id,
    site_name, 
    placement_id,
    placement_name,  
    split(placement_name, '-')[safe_offset(0)] as placement_type,
    split(placement_name, '-')[safe_offset(1)] as placement_type_1,
    split(placement_name, '-')[safe_offset(2)] as placement_method,
    split(placement_name, '-')[safe_offset(3)] as placement_site,
    split(placement_name, '-')[safe_offset(4)] as placement_target,
    split(placement_name, '-')[safe_offset(5)] as placement_format,
    split(placement_name, '-')[safe_offset(6)] as placement_format_2,
    split(placement_name, '-')[safe_offset(7)] as placement_device,
    split(placement_name, '-')[safe_offset(8)] as adform_placement_name9,
    split(placement_name, '-')[safe_offset(9)] as adform_placement_name10,    
    creative_id,
    creative_name , 
  -- Rules for handling diffusion metrics
  case when impressions is null then impressions_original else impressions end as impressions,
  case when clicks is null then clicks_original else clicks end as clicks,
  case when media_cost is null then media_cost_original else media_cost end as media_cost,
  case when rich_media_video_plays is null then rich_media_video_plays_original else rich_media_video_plays end as rich_media_video_plays,
  case when rich_media_video_first_quartile_completes is null then rich_media_video_first_quartile_completes_original else rich_media_video_first_quartile_completes end as rich_media_video_first_quartile_completes,
  case when rich_media_video_midpoints is null then rich_media_video_midpoints_original else rich_media_video_midpoints end as rich_media_video_midpoints,
  case when rich_media_video_third_quartile_completes is null then rich_media_video_third_quartile_completes_original else rich_media_video_third_quartile_completes end as rich_media_video_third_quartile_completes,
  case when rich_media_video_completions is null then rich_media_video_completions_original else rich_media_video_completions end as rich_media_video_completions,
  case when active_view_viewable_impressions is null then active_view_viewable_impressions_original else active_view_viewable_impressions end as active_view_viewable_impressions,
  case when active_view_measurable_impressions is null then active_view_measurable_impressions_original else active_view_measurable_impressions end as active_view_measurable_impressions,
  case when active_view_eligible_impressions is null then active_view_eligible_impressions_original else active_view_eligible_impressions end as active_view_eligible_impressions
    from 
      {{ ref('src_adform_data_standard') }}
order by impressions desc       
