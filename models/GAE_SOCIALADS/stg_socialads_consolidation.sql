{{
  config(
    materialized = 'table',
    )
}}

select 
  Date , 
  Data_Source_name , 
  Data_Source_type ,
  Media_type , 
  Account__DoubleClick_Search as account, 
  Account_ID__DoubleClick_Search as account_id , 
  Advertiser_ID__DoubleClick_Search as adversiter_id, 
  Advertiser__DoubleClick_Search as advertiser, 
  Campaign__DoubleClick_Search as campaign_name , 
  Campaign_ID__DoubleClick_Search as campaign_id,
  Ad_group__DoubleClick_Search as ad_group,
  Ad_group_ID__DoubleClick_Search as ad_group_id,
  Ad_ID__DoubleClick_Search as ad_id, 
  Ad__DoubleClick_Search as ad_name,
  sum(impressions) as impressions, 
  sum(Clicks) as clicks , 
  sum(Cost) as cost 
from {{ ref('src_global_socialads_data') }}
where Data_Source_type='doubleclicksearch'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

union all 

select 
  Date , 
  Data_Source_name , 
  Data_Source_type ,
  Media_type , 
  Account__LinkedIn as account, 
  Account_ID__LinkedIn as account_id, 
  Account_ID__LinkedIn as adversiter_id, 
  Account__LinkedIn as advertiser,
  Campaign__LinkedIn as campaign_name , 
  Campaign_ID__LinkedIn as campaign_id,
  Campaign_Group__LinkedIn as ad_group,
  Campaign_Group_ID__LinkedIn as ad_group_id,
  Creative_ID__LinkedIn as ad_id, 
  Creative_Name__LinkedIn as ad_name,
  sum(impressions) as impressions, 
  sum(Clicks) as clicks , 
  sum(Cost) as cost    
from {{ ref('src_global_socialads_data') }}
where Data_Source_type='linkedin_api'  
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

union all 

select 
  Date , 
  Data_Source_name , 
  Data_Source_type ,
  Media_type , 
  Ad_account_name__Snapchat as account, 
  Account_ID__Snapchat as account_id, 
  Account_ID__Snapchat as adversiter_id, 
  Ad_account_name__Snapchat as advertiser,
  Campaign_Name__Snapchat as campaign_name , 
  Campaign_ID__Snapchat as campaign_id,
  'none' as ad_group,
  'none' as ad_group_id,
  Ad_ID__Snapchat as ad_id, 
  Ad_Name__Snapchat as ad_name,
  sum(impressions) as impressions, 
  sum(Clicks) as clicks , 
  sum(Cost) as cost    
from {{ ref('src_global_socialads_data') }}
where Data_Source_type='snapchat'  
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14


union all 

select 
  Date , 
  Data_Source_name , 
  Data_Source_type ,
  Media_type , 
  Data_Source_name as account, 
  Ad_Account_ID__Facebook_Ads as account_id, 
  Ad_Account_ID__Facebook_Ads as adversiter_id, 
  Data_Source_name as advertiser,
  Campaign_Name__Facebook_Ads as campaign_name , 
  Campaign_ID__Facebook_Ads as campaign_id,
  Ad_Set_Name__Facebook_Ads as ad_group,
  Ad_Set_ID__Facebook_Ads as ad_group_id,
  Ad_ID__Facebook_Ads as ad_id, 
  Ad_Name__Facebook_Ads as ad_name,
  sum(impressions) as impressions, 
  sum(Clicks) as clicks , 
  sum(Cost) as cost    
from {{ ref('src_global_socialads_data') }}
where Data_Source_type='facebookads' 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14


union all 

select 
  Date , 
  Data_Source_name , 
  Data_Source_type ,
  Media_type , 
  Data_Source_name as account, 
  Advertiser_ID__TikTok as account_id, 
  Advertiser_ID__TikTok as adversiter_id, 
  Advertiser_name__TikTok as advertiser,
  Campaign_name__TikTok as campaign_name , 
  Campaign_ID__TikTok as campaign_id,
  Adgroup_name__TikTok as ad_group,
  Adgroup_ID__TikTok as ad_group_id,
  Ad_ID__TikTok as ad_id, 
  Ad_name__TikTok as ad_name,
  sum(impressions) as impressions, 
  sum(Clicks) as clicks , 
  sum(Cost) as cost    
from {{ ref('src_global_socialads_data') }}
where Data_Source_type='tiktok'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14


union all 


select 
  Date , 
  Data_Source_name , 
  Data_Source_type ,
  Media_type , 
  Data_Source_name as account, 
  Data_Source_name as account_id, 
  Data_Source_name as adversiter_id, 
  Data_Source_name as advertiser,
  Campaign__Twitter as campaign_name , 
  Campaign_ID__Twitter as campaign_id,
  Ad_Group__Twitter as ad_group,
  Ad_Group_ID__Twitter as ad_group_id,
  Ad_Name__Twitter as ad_id, 
  Ad_Name__Twitter as ad_name,
  sum(impressions) as impressions, 
  sum(Clicks) as clicks , 
  sum(Cost) as cost    
from {{ ref('src_global_socialads_data') }}
where Data_Source_type='twitter'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
