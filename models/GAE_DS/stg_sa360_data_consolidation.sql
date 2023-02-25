{{
  config(
    materialized = 'table'
    )
}}

with data_standard as (
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
     Engine__DoubleClick_Search as engine, 
     media_type, 
     sum(Impressions__DoubleClick_Search) as impressions_ds, 
     sum(impressions) as impressions, 
     sum(Clicks__DoubleClick_Search) as clicks_ds , 
     sum(clicks) as clicks,  
     sum(Cost__DoubleClick_Search) as cost_ds, 
     sum(cost) as cost, 
     sum(Actions__DoubleClick_Search) as actions_ds , 
     sum(AdWords_VT_conversions__DoubleClick_Search) as conversions, 
     sum(Visits__DoubleClick_Search) as visits , 
     sum(Transactions__DoubleClick_Search) as transactions
 from {{ ref('src_sa360_data') }}
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select 
     concat(date,'_', advertiser_id,'_',campaign_id,'_',ad_group_id) as info_key, 
     *
from data_standard

     

