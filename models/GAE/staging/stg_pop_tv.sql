{{
  config(
    materialized = 'table',
    )
}}

-- We need to add all the metrics for this source

select 
      one_dimension, 
      date, 
      data_stream,
      campaign_name,
      site_name, 
      advertiser, 
      media_buy_key, 
      campaign_key, 
      site_key

-- check metrics data       

   from {{ ref('prd_global_data') }}
where data_stream = 'SIG - Popcorn'
