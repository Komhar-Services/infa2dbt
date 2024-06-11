{{
    config(
        materialized="table",
        post_hook="update tgt set tgt.cml_agmt_id = src.cml_agmt_id, tgt.agmt_anchr_id = src.agmt_anchr_id, tgt.updt_pop_info_id = src.updt_pop_info_id, tgt.pymt_pln_cd = src.pymt_pln_cd, tgt.mstr_pol_nbr = src.mstr_pol_nbr, tgt.tail_expi_dt = src.tail_expi_dt, tgt.parnt_child_contr_typ_cd = src.parnt_child_contr_typ_cd, tgt.ratg_bas_cd = src.ratg_bas_cd, tgt.assm_ptnr_ctry_pol_id = src.assm_ptnr_ctry_pol_id, tgt.assm_us_pol_nbr = src.assm_us_pol_nbr, tgt.cede_ptnr_ctry_anchr_id = src.cede_ptnr_ctry_anchr_id, tgt.drvd_contr_crcy_cd = src.drvd_contr_crcy_cd, tgt.frnt_cd = src.frnt_cd, tgt.non_renl_rsn_cd = src.non_renl_rsn_cd, tgt.zi_pol_sys_contr_id = src.zi_pol_sys_contr_id from cml_agmt tgt inner join {{this}} src on tgt.cml_agmt_id = src.cml_agmt_idAND tgt.agmt_anchr_id = src.agmt_anchr_id",
    )
}}

with
    rtr_ins_upd_update_only as (
        select *
        from {{ ref("m_atomic_cml_agmt_upd_cesar_rtr_ins_upd") }}
        where out_expins_upd_flag = 'U'
    ),
    exp_set_val_cml_agmt_upd as (
        exp_set_val_cml_agmt_upd as (
            select
                cml_agmt_id,
                agmt_anchr_id,
                src_mstr_pol_nbr,
                src_pymt_pln_cd,
                src_ext_rpt_perd_dt,
                out_mstr_pol_cd as mstr_pol_cd,
                out_src_wc_ratg_bas_cd as src_wc_ratg_bas_cd,
                out_ips_contr_id as out_ips_contr_id1,
                out_plc_anchr_id as out_plc_anchr_id1,
                out_ptnr_ctry_pol_id as out_ptnr_ctry_pol_id1,
                out_ptnr_ctry_pol_nbr as out_ptnr_ctry_pol_nbr1,
                out_frntg_cd as out_frntg_cd1,
                out_pol_lvl_crcy_cd as out_pol_lvl_crcy_cd1,
                out_non_renl_rsn_cd as out_non_renl_rsn_cd1,
                {{ var("pop_info_id") }} as out_updt_pop_info_id
            from rtr_ins_upd_update_only
        ),
    ),
    upd_cml_agmt as (
        select
            cml_agmt_id,
            agmt_anchr_id,
            src_mstr_pol_nbr,
            src_pymt_pln_cd,
            src_ext_rpt_perd_dt,
            mstr_pol_cd,
            src_wc_ratg_bas_cd,
            out_ips_contr_id1,
            out_plc_anchr_id1,
            out_ptnr_ctry_pol_id1,
            out_ptnr_ctry_pol_nbr1,
            out_frntg_cd1,
            out_pol_lvl_crcy_cd1,
            out_non_renl_rsn_cd1,
            out_updt_pop_info_id
        from exp_set_val_cml_agmt_upd
    ),
select
    cml_agmt_id as cml_agmt_id,
    agmt_anchr_id as agmt_anchr_id,
    out_updt_pop_info_id as updt_pop_info_id,
    src_pymt_pln_cd as pymt_pln_cd,
    src_mstr_pol_nbr as mstr_pol_nbr,
    src_ext_rpt_perd_dt as tail_expi_dt,
    mstr_pol_cd as parnt_child_contr_typ_cd,
    src_wc_ratg_bas_cd as ratg_bas_cd,
    out_ptnr_ctry_pol_id1 as assm_ptnr_ctry_pol_id,
    out_ptnr_ctry_pol_nbr1 as assm_us_pol_nbr,
    out_plc_anchr_id1 as cede_ptnr_ctry_anchr_id,
    out_pol_lvl_crcy_cd1 as drvd_contr_crcy_cd,
    out_frntg_cd1 as frnt_cd,
    out_non_renl_rsn_cd1 as non_renl_rsn_cd,
    out_ips_contr_id1 as zi_pol_sys_contr_id
from upd_cml_agmt
where update_strategy_flag = 1  -- DD_UPDATE = 1
