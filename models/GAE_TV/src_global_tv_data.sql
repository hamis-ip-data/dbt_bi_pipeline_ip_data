

{{
  config(
    materialized = 'table',
    )
}}

select 
 * from {{ source('gae_data', 'gae_popcorn_tv') }}