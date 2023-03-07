
{{
  config(
    materialized = 'table',
    )
}}

select 
   * from {{ source('data_adform_2023', 'standard_5550') }}