{{
    config(
        materialized="incremental",
        labels={"type": "datorama_data", "clients": "gae", "category": "analysis"},
    )
}}

select
    *,

    -- Gestion des campagnes display
    substr(campaign_name, 0, 10) as date_campagne,
    substr(campaign_name, 12) as campagne_,
    split(substr(campaign_name, 12), '-')[safe_offset(0)] as adform_campaign_type_1,
    split(substr(campaign_name, 12), '-')[safe_offset(1)] as adform_campaign_type_2,
    split(substr(campaign_name, 12), '-')[safe_offset(2)] as adform_campaign_type_3,
    split(substr(campaign_name, 12), '-')[safe_offset(3)] as adform_campaign_type_4,
    split(substr(campaign_name, 12), '-')[safe_offset(4)] as adform_campaign_type_5,
 
    -- Gestion des campagnes display 
    media_buy_name as prd_media_buy_name,
    split(media_buy_name, '-')[safe_offset(0)] as adform_media_buy_name1,
    split(media_buy_name, '-')[safe_offset(1)] as adform_media_buy_name2,
    split(media_buy_name, '-')[safe_offset(2)] as adform_media_buy_name3,
    split(media_buy_name, '-')[safe_offset(3)] as adform_media_buy_name4,
    split(media_buy_name, '-')[safe_offset(4)] as adform_media_buy_name5,
    split(media_buy_name, '-')[safe_offset(5)] as adform_media_buy_name6,
    split(media_buy_name, '-')[safe_offset(6)] as adform_media_buy_name7,
    split(media_buy_name, '-')[safe_offset(7)] as adform_media_buy_name8,
    split(media_buy_name, '-')[safe_offset(8)] as adform_media_buy_name9,
    split(media_buy_name, '-')[safe_offset(9)] as adform_media_buy_name10,

    case
        when
            regexp_contains(
                lower(site_name), 'bing ads|dart search|google|google adwords'
            )
        then 'Search Ads'
        when
            regexp_contains(
                lower(site_name), 'linkedin|facebook|instagram|tiktok|snapchat'
            )
        then 'Social Ads'
        else 'Display'
    end as data_type,
    case
        when media_cost_rtb > 0 and media_cost = 0
        then media_cost_rtb
        when media_cost_rtb = 0 and media_cost = 0 and media_cost_original > 0
        then media_cost_original
        when media_cost != 0 and media_cost_fld > 0
        then media_cost_fld
        else media_cost
    end as budget_depense
from {{ ref("stg_global_dato") }}
