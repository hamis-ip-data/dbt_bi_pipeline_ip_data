  

  {{
    config(
      materialized = 'table'
      )
  }}

select
    con.info_key,
    con.date,
    con.advertiser_name,
    con.advertiser_name_bis,
    dict.final_advertiser_name,
    dict.id_media_manager,
    dict.id_nouveau,
    dict.nom_annonceur,
    dict.ministere_annonceur,
    con.campaign_name,
    con.campaign_id,
    con.campaign_date,
    con.campaign_category,
    con.campaign_levier,
    con.campaign_ministere,
    con.campaign_name_name,
    con.campaign_adazzle_number,
    
    /* checking campaign compliance */
    prd_camp_comp.check_category as check_campaign_category,
    prd_camp_comp.check_levier as check_campaign_levier,
    prd_camp_comp.check_minister as check_campaign_minister,
    prd_camp_comp.total_check as check_campaign_total_check,
    prd_camp_comp.check_status as check_campaign_check_status,
    /* checking campaign compliance */

    con.site_id,
    con.site_name,
    con.placement_id,
    con.placement_name,
    con.placement_type,
    con.placement_objective,
    con.placement_byying_method,
    con.placement_regie,
    con.placement_audience,
    con.placement_ciblage,
    con.placement_format_name,
    con.placement_format_size,
    con.placement_device,

    /* check placement compliance */
    prd_plac_comp.check_placement_type,
    prd_plac_comp.check_placement_objective,
    prd_plac_comp.check_placement_byying_method,
    prd_plac_comp.check_placement_regie,
    prd_plac_comp.check_placement_audience,
    prd_plac_comp.check_placement_ciblage,
    prd_plac_comp.check_placement_format_name,
    prd_plac_comp.check_placement_format_size,
    prd_plac_comp.check_placement_device,
    prd_plac_comp.total_check as check_placement_total_check,
    prd_plac_comp.check_status as check_placement_check_status,
    /* check placement compliance */

    con.creative_id,
    con.creative_name,
    con.creative_asset_type,
    con.creative_format_name,
    con.creative_format_size,
    con.creative_message_name,

    /* check creative compliance */
    prd_crea_comp.check_creative_asset_type,
    prd_crea_comp.check_creative_format_name,
    prd_crea_comp.check_creative_format_size,
    prd_crea_comp.total_check as check_creative_total_check,
    prd_crea_comp.check_status as check_creative_check_status, 
    /* check creative compliance */

    con.impressions,
    con.clicks,
    con.media_cost,
    con.rich_media_video_plays,
    con.rich_media_video_first_quartile_completes,
    con.rich_media_video_midpoints,
    con.rich_media_video_third_quartile_completes,
    con.rich_media_video_completions,
    con.active_view_viewable_impressions,
    con.active_view_measurable_impressions,
    con.active_view_eligible_impressions,
    con.data_source_type,
    con.activity_name,
    con.pv_arrivee,
    con.pv_arrivee_no_rep,
    con.pc_arrivee,
    con.pc_arrivee_no_rep,
    con.pv_micro,
    con.pv_micro_no_rep,
    con.pc_micro,
    con.pc_micro_no_rep,
    con.pv_lead,
    con.pv_lead_no_rep,
    con.pc_lead,
    con.pc_lead_no_rep    
    from {{ ref('prd_adform_consolidation') }} con
    left join {{ ref('prd_advertiser_dict') }} dict
    on con.advertiser_name = dict.advertiser
    left join {{ ref('prd_global_campaign_compliance') }} prd_camp_comp
    on con.campaign_id = prd_camp_comp.campaign_id
    left join {{ ref('prd_global_performance_compliance') }} prd_plac_comp
    on con.placement_id = prd_plac_comp.placement_id and con.campaign_id = prd_plac_comp.campaign_id
    left join {{ ref('prd_global_creative_compliance') }} prd_crea_comp
    on con.creative_id = prd_crea_comp.creative_id and con.placement_id = prd_crea_comp.placement_id and con.campaign_id = prd_crea_comp.campaign_id