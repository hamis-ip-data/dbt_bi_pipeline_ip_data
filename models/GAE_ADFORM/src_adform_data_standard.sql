{{
  config(
    materialized = 'table',
    )
}}

select
  date,
  client_id,
  campaign_id,
  campaign_name,
  advertiser_id,
  advertiser_name,
  site_id,
  site_name,
  placement_id,
  placement_name,
  placement_size,
  placement_strategy,
  content_category,
  ad_id,
  ad_name,
  creative_id,
  creative_name,
  creative_type,
  click_through_url,
  zip_code,
  country,
  platform_type,
  package_roadblock_id,
  package_roadblock_name,
  placement_total_booked_units,
  package_roadblock_total_booked_units,
  placement_total_planned_media_cost,
  impressions_original,
  clicks_original,
  media_cost_original,
  rich_media_video_plays_original,
  rich_media_video_first_quartile_completes_original,
  rich_media_video_midpoints_original,
  rich_media_video_third_quartile_completes_original,
  rich_media_video_completions_original,
  active_view_viewable_impressions_original,
  active_view_measurable_impressions_original,
  active_view_eligible_impressions_original,
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
  active_view_eligible_impressions,
  total_conversions,
  activity_view_through_conversions,
  activity_click_through_conversions,
  total_conversions_revenue,
  activity_view_through_revenue,
  activity_click_through_revenue
FROM
    {{ source('data_adform_2023', 'standard_5550') }}

