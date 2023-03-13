{{
    config(
        materialized='table'
    )
}}

with data_checking as ( 
select
    distinct
    campaign_id,
    campaign_category,
    campaign_levier,
    campaign_ministere,
    campaign_name_name,
    campaign_adazzle_number,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'social_campagne') }} as b
                where b.category = a.campaign_category
            )
        then 1
        else 0
    end as check_category,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'social_campagne') }}  as b
                where b.levier = a.campaign_levier
            )
        then 1
        else 0
    end as check_levier,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'social_campagne') }}  as b
                where b.ministere = a.campaign_ministere
            )
        then 1
        else 0
    end as check_minister

from {{ ref("prd_adform_consolidation") }} as a
where data_source_type = 'Social Ads'
)

select 
* , 
check_category + check_levier + check_minister as total_check, 
case when (check_category + check_levier + check_minister < 3) then 'NO' else 'YES' end as check_status 

from data_checking