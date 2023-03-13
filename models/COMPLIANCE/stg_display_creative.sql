{{
    config(
        materialized='table'
    )
}}

with data_checking as ( 
select
    distinct 
    campaign_name, 
    campaign_id, 
    placement_name,
    placement_id,
    placement_type,
    creative_id,
    creative_name,
    creative_asset_type,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_creative') }} as b
                where b.type_asset = a.creative_asset_type
            )
        then 1
        else 0
    end as check_creative_asset_type,
creative_format_name  , 
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_creative') }}  as b
                where b.nomdeformat = a.creative_format_name
            )
        then 1
        else 0
    end as check_creative_format_name,
creative_format_size  , 
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_creative') }}  as b
                where b.tailledeformat = a.creative_format_size
            )
        then 1
        else 0
    end as check_creative_format_size,
/*
creative_message_name  , 
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_creative') }}  as b
                where b.nomdemessage = a.creative_message_name
            )
        then 1
        else 0
    end as check_creative_message_name
*/
from {{ ref("prd_adform_consolidation") }} as a
where data_source_type = 'Display'
)

select 
* , 
check_creative_asset_type + check_creative_format_name + check_creative_format_size  as total_check, 
case when (check_creative_asset_type + check_creative_format_name + check_creative_format_size  < 3) then 'NO' else 'YES' end as check_status 

from data_checking