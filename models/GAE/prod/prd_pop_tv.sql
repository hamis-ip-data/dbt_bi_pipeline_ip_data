
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
    grp,
    position,
    tarif_brut_pay,
    tarif_bt_amp,
    tarif_net_co,
    tarif_bt_fact,
    tarif_net_fo,
    tarif_net_fo_hi,
    tarif_secodip,
    duree_de_spot,
    grp_pc_actifs,
    grp_pc_enf4a10,
    grp_pc_ens1219,
    grp_pc_ens8_12,

   from {{ ref('stg_pop_tv') }} 