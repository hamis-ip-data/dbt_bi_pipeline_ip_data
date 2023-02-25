{{
  config(
    materialized = 'table'
    )
}}

  select
    Date,
    Data_Source_type,
    Data_Source_id,
    Cost,
    Clicks,
    Impressions,
    Media_type,
    Campaign,
    Data_Source_name,
    Data_Source,
    Account__DoubleClick_Search,
    Account_ID__DoubleClick_Search,
    case when Ad__DoubleClick_Search is null then '0' else Ad__DoubleClick_Search end as Ad__DoubleClick_Search,
    Ad_group__DoubleClick_Search,
    Ad_group_ID__DoubleClick_Search,
    case when Ad_ID__DoubleClick_Search is null then '0' else Ad_ID__DoubleClick_Search end as Ad_ID__DoubleClick_Search,
    Advertiser_ID__DoubleClick_Search,
    Advertiser__DoubleClick_Search,
    Agency_ID__DoubleClick_Search,
    Agency__DoubleClick_Search,    
    Campaign_ID__DoubleClick_Search,
    Campaign__DoubleClick_Search,
    Engine__DoubleClick_Search,
    Actions__DoubleClick_Search,
    AdWords_conversions__DoubleClick_Search,
    AdWords_VT_conversions__DoubleClick_Search,
    AdWords_conversion_value__DoubleClick_Search,
    Clicks__DoubleClick_Search,
    Cost__DoubleClick_Search,
    Impressions__DoubleClick_Search,
    Transactions__DoubleClick_Search,
    Visits__DoubleClick_Search
    from {{ source('gae_ds_data', 'gae_ds_export_*') }}
    where lower(Data_Source_name) not like '%floodlight%'