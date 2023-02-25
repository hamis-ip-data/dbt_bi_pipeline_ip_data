{{
  config(
    materialized = 'table',
    )
}}


select 
    
    Date,
    Data_Source_id,
    Campaign,
    Data_Source_name,
    Data_Source,
    Account__DoubleClick_Search,
    Account_ID__DoubleClick_Search,
    Ad__DoubleClick_Search,
    Ad_group__DoubleClick_Search,
    Ad_group_ID__DoubleClick_Search,
    Ad_ID__DoubleClick_Search,
    Advertiser_ID__DoubleClick_Search,
    Advertiser__DoubleClick_Search,
    Agency_ID__DoubleClick_Search,
    Agency__DoubleClick_Search,
    Campaign_ID__DoubleClick_Search,
    Campaign__DoubleClick_Search,
    Engine__DoubleClick_Search,
    Actions__DoubleClick_Search,
    Floodlight_activity__DoubleClick_Search,
    Floodlight_activity_ID__DoubleClick_Search,
    Floodlight_activity_tag__DoubleClick_Search,
    Floodlight_group__DoubleClick_Search,
    Floodlight_group_ID__DoubleClick_Search
 from {{ ref('src_sa360_floodlight') }}