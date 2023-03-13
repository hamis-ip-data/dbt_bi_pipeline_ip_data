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
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.type_emplacem_t = a.placement_type
            )
        then 1
        else 0
    end as check_placement_type,    
    placement_objective,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.objectif = a.placement_objective
            )
        then 1
        else 0
    end as check_placement_objective,       
    placement_byying_method,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.type_achat = a.placement_byying_method
            )
        then 1
        else 0
    end as check_placement_byying_method,           
    placement_regie,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.regie = a.placement_regie
            )
        then 1
        else 0
    end as check_placement_regie,      
    placement_audience,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.audi_ce = a.placement_audience
            )
        then 1
        else 0
    end as check_placement_audience,       
    placement_ciblage,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.ciblage = a.placement_ciblage
            )
        then 1
        else 0
    end as check_placement_ciblage,      
    placement_format_name,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.nomdeformat = a.placement_format_name
            )
        then 1
        else 0
    end as check_placement_format_name,          
    placement_format_size,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.tailledeformat = a.placement_format_size
            )
        then 1
        else 0
    end as check_placement_format_size,      
    placement_device,
    case
        when
            exists (
                select 1
                from {{ source('compliance', 'adserver_line_item') }} as b
                where b.device = a.placement_device
            )
        then 1
        else 0
    end as check_placement_device,        

from {{ ref("prd_adform_consolidation") }} as a
where data_source_type = 'Display'
)
select 
* , 
check_placement_type + check_placement_objective + check_placement_byying_method + check_placement_regie + check_placement_audience + 
check_placement_ciblage + check_placement_format_name + check_placement_format_size + check_placement_device  as total_check, 
case when (check_placement_type + check_placement_objective + check_placement_byying_method + check_placement_regie + check_placement_audience + 
check_placement_ciblage + check_placement_format_name + check_placement_format_size + check_placement_device < 9) then 'NO' else 'YES' end as check_status 

from data_checking