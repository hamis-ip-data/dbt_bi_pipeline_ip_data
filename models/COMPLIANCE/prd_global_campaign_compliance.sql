

SELECT
  'Display' as data_type,
  campaign_id,
  check_category,
  check_levier,
  check_minister,
  total_check,
  check_status
FROM
  `dash-project-376514.dbt_dev_pipeline.prd_display_campaign_compliance`

union all 

SELECT
  'Search' as data_type,
  campaign_id,
  check_category,
  check_levier,
  check_minister,
  total_check,
  check_status
FROM
  `dash-project-376514.dbt_dev_pipeline.prd_search_campaign_compliance`

union all

SELECT
  'Social' as data_type,
  campaign_id,
  check_category,
  check_levier,
  check_minister,
  total_check,
  check_status
FROM
  `dash-project-376514.dbt_dev_pipeline.prd_social_campaign_compliance`


