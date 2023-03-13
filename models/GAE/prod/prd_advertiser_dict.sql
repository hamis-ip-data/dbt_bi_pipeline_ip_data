
{{
  config(
    materialized = 'table'
    )
}}

with data_advertiser as (

  select
    data_source_name,
    data_source_type,
    media_type,
    account,
    account_id,
    adversiter_id,
    advertiser,
    final_advertiser_name

  from  {{ source('compliance', 'data_advertiser') }}
) , 

mapping_mm_label as (

  select
    id_media_manager,
    id_nouveau,
    nom_annonceur,
    ministere_annonceur
  from  {{ source('compliance', 'mapping_mm_label') }}
)

select 
data_advertiser.data_source_name, 
data_advertiser.data_source_type,
data_advertiser.media_type,
data_advertiser.account,
data_advertiser.account_id,
data_advertiser.adversiter_id,
data_advertiser.advertiser,
data_advertiser.final_advertiser_name,
mapping_mm_label.id_media_manager,
mapping_mm_label.id_nouveau,
mapping_mm_label.nom_annonceur,
mapping_mm_label.ministere_annonceur
from data_advertiser
left join mapping_mm_label
on data_advertiser.final_advertiser_name = mapping_mm_label.id_nouveau
