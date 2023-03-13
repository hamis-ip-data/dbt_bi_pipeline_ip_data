 {{
   config(
     materialized = 'table'
     )
 }}

  select
    distinct 
    day,
    campaign_name,
    media_buy_key,
    media_buy_position,
    media_buy_type,
    media_buy_targeting_age,
    horaire_hh_mm,
    emission_avant,
    emission_apres,
    libelle_client,
    site_name,
    creative_name,
    site_category,
    site_network,
    cast (grp as float64) as grp,
    cast(position as float64) as position,
    cast(tarif_brut_pay as float64) as tarif_brut_pay,
    cast(tarif_bt_amp as float64) as tarif_bt_amp,
    cast(tarif_net_co as float64) as tarif_net_co,
    cast(tarif_bt_fact as float64) as tarif_bt_fact,
    cast(tarif_net_fo as float64) as tarif_net_fo,
    cast(tarif_net_fo_hi as float64) as tarif_net_fo_hi,
    cast(tarif_secodip as float64) as tarif_secodip,
    cast(duree_de_spot as int64) as duree_de_spot,
    cast(grp_pc_actifs as float64) as grp_pc_actifs,
    cast(grp_pc_enf4a10 as float64) as grp_pc_enf4a10,
    cast(grp_pc_ens1219 as float64) as grp_pc_ens1219,
    cast(grp_pc_ens8_12 as float64) as grp_pc_ens8_12,
    insert_date   

   from {{ ref('src_global_tv_data') }}
