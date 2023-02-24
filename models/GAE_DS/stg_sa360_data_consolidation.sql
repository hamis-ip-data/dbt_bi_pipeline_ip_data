{{
  config(
    materialized = 'table'
    )
}}

select 
     date, 
     Agency__DoubleClick_Search as agency ,
     Agency_ID__DoubleClick_Search as agency_id,
     data_source_name as data_source, 
     account__doubleClick_search as account_name,
     account_id__doubleclick_search as account_id ,
     Advertiser__DoubleClick_Search as advertiser_name, 
     Advertiser_ID__DoubleClick_Search as advertiser_id,      
     Campaign__DoubleClick_Search as campaign ,
     Campaign_ID__DoubleClick_Search as campaign_id ,
     Ad_group__DoubleClick_Search as ad_group ,
     Ad_group_ID__DoubleClick_Search as ad_group_id ,
     Ad__DoubleClick_Search as ad_name , 
     Ad_ID__DoubleClick_Search as ad_id ,
     Engine__DoubleClick_Search as engine, 
     media_type, 
     Impressions__DoubleClick_Search as impressions_ds, 
     impressions, 
     Clicks__DoubleClick_Search as clicks_ds , 
     clicks, 
     Cost__DoubleClick_Search as cost_ds, 
     cost, 
     Actions__DoubleClick_Search as actions_ds , 
     AdWords_VT_conversions__DoubleClick_Search as conversions, 
     Visits__DoubleClick_Search as visits , 
     Transactions__DoubleClick_Search as transactions
 from {{ ref('src_sa360_data') }}

  
     

