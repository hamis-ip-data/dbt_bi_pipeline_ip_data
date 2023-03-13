  

  {{
    config(
      materialized = 'table'
      )
  }}

  select 
   * 
   from {{ ref('prd_adform_consolidation') }}