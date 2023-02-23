{{
  config(
    materialized = 'table',
    )
}}

/* Rule to categorize the type of conversions. We will check the naming convention of the conversion tag
and then categorise the conversion. 
If activity name contain pagearrivee or pageacc then arrivee
If activity name contain pagetunnel or other micro conversion then micro
If activity name contain confirmation then lead
*/

select
    concat(date,'_',campaign_id,'_',site_id,'_',placement_id,'_',creative_id,'_', ad_id) as info_key,
    date, 
    advertiser_name , 
    campaign_id,
    site_id,
    site_name, 
    placement_id,
    placement_name,  
    creative_id,
    creative_name, 
    ad_id,
    ad_name,
    activity_name,
    -- Page arrivee , Landing page or page accueil 
    case when regexp_contains(lower(activity_name), 'pagearrivee|pageacc') and activity_view_through_conversions_original is not null
        then activity_view_through_conversions_original else 0 end as pv_arrivee , 
    case when regexp_contains(lower(activity_name), 'pagearrivee|pageacc') and activity_view_through_conversions_no_rep_original is not null
        then activity_view_through_conversions_no_rep_original else 0 end as pv_arrivee_no_rep,
    case when regexp_contains(lower(activity_name), 'pagearrivee|pageacc') and activity_click_through_conversions_original is not null
        then activity_click_through_conversions_original else 0 end as pc_arrivee ,
    case  when regexp_contains(lower(activity_name), 'pagearrivee|pageacc') and activity_click_through_conversions_no_rep_original is not null
        then activity_click_through_conversions_no_rep_original else 0 end as pc_arrivee_no_rep ,

    -- Page tunnel 
    case when regexp_contains(lower(activity_name), 'pagetunnel') and activity_view_through_conversions_original is not null
        then activity_view_through_conversions_original else 0 end as pv_micro ,
    case when regexp_contains(lower(activity_name), 'pagetunnel') and activity_view_through_conversions_no_rep_original is not null
        then activity_view_through_conversions_no_rep_original else 0 end as pv_micro_no_rep,
    case when regexp_contains(lower(activity_name), 'pagetunnel') and activity_click_through_conversions_original is not null
        then activity_click_through_conversions_original else 0 end as pc_micro , 
    case when regexp_contains(lower(activity_name), 'pagetunnel') and activity_click_through_conversions_no_rep_original is not null
        then activity_click_through_conversions_no_rep_original else 0 end as pc_micro_no_rep , 

    -- Page Lead or confirmation
    case when regexp_contains(lower(activity_name), 'pageconfirmation') and activity_view_through_conversions_original is not null
        then activity_view_through_conversions_original else 0 end as pv_lead ,
    case when regexp_contains(lower(activity_name), 'pageconfirmation') and activity_view_through_conversions_no_rep_original is not null
        then activity_view_through_conversions_no_rep_original else 0 end as pv_lead_no_rep,
    case when regexp_contains(lower(activity_name), 'pageconfirmation') and activity_click_through_conversions_original is not null
        then activity_click_through_conversions_original else 0  end as pc_lead , 
    case when regexp_contains(lower(activity_name), 'pageconfirmation') and activity_click_through_conversions_no_rep_original is not null
        then activity_click_through_conversions_no_rep_original else 0 end as pc_lead_no_rep

    -- from datasource 
from {{ ref('src_adform_data_floodlight') }}

order by pc_arrivee desc 

