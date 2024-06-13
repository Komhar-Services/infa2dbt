{{
	config(
		materialized = "incremental"
	)
}}

with
r_lkp_mntry_acct as (select * from {{ ref("r_lkp_mntry_acct") }}),
stg_sap_dtl as (select * from {{ source("dbaall", "stg_sap_dtl") }}),
pers as (select * from {{ source("dbaall", "pers") }}),
typ as (select * from {{ source("dbaall", "typ") }}),
acct_dervn as (select * from {{ source("dbaall", "acct_dervn") }}),
sq_stg_sap_dtl_n_rec as (
    select
        coalesce(a.ent_usr_id, 'null') as ent_usr_id,
        coalesce(a.ent_usr_nm, 'null') as ent_usr_nm,
        a.gledgr_post_dt as gledgr_post_dt,
        coalesce(a.finc_co_cd, 'null') as finc_co_cd,
        coalesce(a.finc_acct_nbr, 'null') as finc_acct_nbr,
        coalesce(a.new_co_cd, 'null') as new_co_cd,
        a.ent_dt as ent_dt,
        coalesce(a.finc_doc_typ_cd, 'null') as finc_doc_typ_cd,
        coalesce(a.doc_nbr, 'null') as doc_nbr,
        coalesce(a.free_frm_txt_desc, 'null') as free_frm_txt_desc,
        coalesce(a.free_frm_hdr_txt_desc, 'null') as free_frm_hdr_txt_desc,
        a.rvrsl_dt as rvrsl_dt,
        coalesce(a.deb_cr_cd, 'null') as deb_cr_cd,
        coalesce(a.org_id, 'null') as org_id,
        coalesce(a.prft_ctr_id, 'null') as prft_ctr_id,
        coalesce(a.prft_ctr_lvl1_nm, 'null') as prft_ctr_lvl1_nm,
        coalesce(a.prft_ctr_lvl2_nm, 'null') as prft_ctr_lvl2_nm,
        coalesce(a.cst_ctr_id, 'null') as cst_ctr_id,
        coalesce(a.cst_ctr_lvl1_nm, 'null') as cst_ctr_lvl1_nm,
        coalesce(a.cst_ctr_lvl2_nm, 'null') as cst_ctr_lvl2_nm,
        coalesce(a.yy_lob_cd, 'null') as yy_lob_cd,
        coalesce(a.ln_itm_txt_desc, 'null') as ln_itm_txt_desc,
        coalesce(a.st_cd, 'null') as st_cd,
        coalesce(a.acc_yy, 'null') as acc_yy,
        coalesce(a.mvmnt_typ_cd, 'null') as mvmnt_typ_cd,
        coalesce(a.reinsr_id, 'null') as reinsr_id,
        a.reins_agmt as reins_agmt,
        a.sub_agnt as sub_agnt,
        coalesce(a.trd_ptnr_cd, 'null') as trd_ptnr_cd,
        a.amt as amt,
        coalesce(a.home_ofc_lob_cd, 'null') as home_ofc_lob_cd,
        a.prem_postal_regn_anchr_id as prem_postal_regn_anchr_id,
        a.finc_lob_cd as finc_lob_cd,
        a.opert_co_cd as opert_co_cd,
        a.co_rol_plyr_anchr_id as co_rol_plyr_anchr_id,
        a.dac_cd as dac_cd,
        a.acct_ent_knd_cd as acct_ent_knd_cd,
        a.pol_yy as pol_yy,
        a.inv_co_cd_err as inv_co_cd_err,
        a.inv_doc_typ_err as inv_doc_typ_err,
        a.inv_finc_lob_err as inv_finc_lob_err,
        a.sap_co_cd_not_fnd_err as sap_co_cd_not_fnd_err,
        a.inv_acct_err as inv_acct_err,
        a.cmtn_ind as cmtn_ind,
        a.cmtn_agmt_anchr_id as cmtn_agmt_anchr_id,
        a.assm_reins_agmt_anchr_id as assm_reins_agmt_anchr_id,
        a.cede_reins_agmt_anchr_id as cede_reins_agmt_anchr_id,
        a.reins_not_fnd_err as reins_not_fnd_err,
        a.reins_agmt_anchr_id as reins_agmt_anchr_id,
        a.reins_agmt_knd_cd as reins_agmt_knd_cd,
        a.org_id_err as org_id_err,
        a.reins_typ as reins_typ,
        a.za_grp_cd as za_grp_cd,
        a.dac_mismatch_err as dac_mismatch_err,
        a.reins_fein_nbr as reins_fein_nbr,
        a.reins_sub_agnt as reins_sub_agnt,
        a.par_rt as par_rt,
        a.rcar_anchr_id as rcar_anchr_id,
        a.sub_agnt_amt as sub_agnt_amt,
        a.sub_agnt_not_fnd_err as sub_agnt_not_fnd_err,
        a.afsi_err as afsi_err,
        a.trd_ptnr_rol_plyr_anchr_id as trd_ptnr_rol_plyr_anchr_id,
        a.inv_trd_ptnr_err as inv_trd_ptnr_err,
        a.zrptr_sapd_id as zrptr_sapd_id,
        trim(a.src_sys_cd) as src_sys_cd,
        trim(a.sys_prcg_cd) as sys_prcg_cd,
        a.bil_sys_cd,
        a.exc_cd,
        coalesce(a.acct_dervn_finc_acct_nbr, 'null') acct_dervn_finc_acct_nbr,
        a.acct_dervn_knd_cd,
        a.onset_offset_ind,
        a.pl_loc_ori_cd,
        a.retro_ratg_cd,
        a.ntr_of_provsn_cd,
        acct_dervn.drvd_acct_anchr_id as drvd_acct_anchr_id,
        case
            when acct_dervn.drvd_acct_anchr_id is null then 'N' else 'Y'
        end as acct_dervn_src_ind,
        mon_acct_typ.mon_acct_typ_id as mon_acct_typ_id,
        finc_acct_typ.finc_acct_typ_id as finc_acct_typ_id,
        finc_adj_typ.finc_adj_typ_id as finc_adj_typ_id,
        case
            when pers.rol_plyr_anchr_id is null then -442 else pers.rol_plyr_anchr_id
        end as pers_rol_plyr_anchr_id,
        1 as sts_cd,
        0 as bal_amt,
        0 as distb_amt,
        to_date('1900-01-01', 'YYYY-MM-DD') as bal_dt,
        'Y' as actv_rec_ind,
        to_date('2999-12-31', 'YYYY-MM-DD') as eff_to_tistmp,
        'null' as finc_ins_typ_cd,
        reinsr_finc_serv_rol_for_reins_rol_plyr_anchr_id as fsri_rol_plyr_anchr_id  -- Added for Remod
    from
        (

            select
                coalesce(stg.ent_usr_id, 'null') as ent_usr_id,
                coalesce(stg.ent_usr_nm, 'null') as ent_usr_nm,
                stg.gledgr_post_dt as gledgr_post_dt,
                coalesce(stg.finc_co_cd, 'null') as finc_co_cd,
                coalesce(substr(stg.finc_acct_nbr, 5, 6), 'null') as finc_acct_nbr,
                coalesce(stg.new_co_cd, 'null') as new_co_cd,
                stg.ent_dt as ent_dt,
                coalesce(stg.finc_doc_typ_cd, 'null') as finc_doc_typ_cd,
                coalesce(stg.doc_nbr, 'null') as doc_nbr,
                coalesce(stg.free_frm_txt_desc, 'null') as free_frm_txt_desc,
                coalesce(stg.free_frm_hdr_txt_desc, 'null') as free_frm_hdr_txt_desc,
                stg.rvrsl_dt as rvrsl_dt,
                coalesce(stg.deb_cr_cd, 'null') as deb_cr_cd,
                coalesce(stg.org_id, 'null') as org_id,
                coalesce(stg.prft_ctr_id, 'null') as prft_ctr_id,
                coalesce(stg.prft_ctr_lvl1_nm, 'null') as prft_ctr_lvl1_nm,
                coalesce(stg.prft_ctr_lvl2_nm, 'null') as prft_ctr_lvl2_nm,
                coalesce(stg.cst_ctr_id, 'null') as cst_ctr_id,
                coalesce(stg.cst_ctr_lvl1_nm, 'null') as cst_ctr_lvl1_nm,
                coalesce(stg.cst_ctr_lvl2_nm, 'null') as cst_ctr_lvl2_nm,
                coalesce(stg.yy_lob_cd, 'null') as yy_lob_cd,
                coalesce(stg.ln_itm_txt_desc, 'null') as ln_itm_txt_desc,
                coalesce(stg.st_cd, 'null') as st_cd,
                coalesce(stg.acc_yy, 'null') as acc_yy,
                coalesce(stg.mvmnt_typ_cd, 'null') as mvmnt_typ_cd,
                coalesce(stg.reinsr_id, 'null') as reinsr_id,
                stg.reins_agmt as reins_agmt,
                stg.sub_agnt as sub_agnt,
                coalesce(stg.trd_ptnr_cd, 'null') as trd_ptnr_cd,
                stg.amt as amt,
                coalesce(stg.home_ofc_lob_cd, 'null') as home_ofc_lob_cd,
                stg.prem_postal_regn_anchr_id as prem_postal_regn_anchr_id,
                coalesce(stg.finc_lob_cd, 'null') as finc_lob_cd,
                coalesce(stg.opert_co_cd, 'null') as opert_co_cd,
                stg.co_rol_plyr_anchr_id as co_rol_plyr_anchr_id,
                coalesce(stg.dac_cd, 'null') as dac_cd,
                coalesce(stg.acct_ent_knd_cd, 'null') as acct_ent_knd_cd,
                coalesce(stg.pol_yy, 'null') as pol_yy,
                stg.inv_co_cd_err as inv_co_cd_err,
                stg.inv_doc_typ_err as inv_doc_typ_err,
                stg.inv_finc_lob_err as inv_finc_lob_err,
                stg.sap_co_cd_not_fnd_err as sap_co_cd_not_fnd_err,
                stg.inv_acct_err as inv_acct_err,
                stg.cmtn_ind as cmtn_ind,
                stg.cmtn_agmt_anchr_id as cmtn_agmt_anchr_id,
                stg.assm_reins_agmt_anchr_id as assm_reins_agmt_anchr_id,
                stg.cede_reins_agmt_anchr_id as cede_reins_agmt_anchr_id,
                stg.reins_not_fnd_err as reins_not_fnd_err,
                stg.reins_agmt_anchr_id as reins_agmt_anchr_id,
                stg.reins_agmt_knd_cd as reins_agmt_knd_cd,
                stg.org_id_err as org_id_err,
                stg.reins_typ as reins_typ,
                stg.za_grp_cd as za_grp_cd,
                stg.dac_mismatch_err as dac_mismatch_err,
                stg.reins_fein_nbr as reins_fein_nbr,
                stg.reins_sub_agnt as reins_sub_agnt,
                stg.par_rt as par_rt,
                stg.rcar_anchr_id as rcar_anchr_id,
                stg.sub_agnt_amt as sub_agnt_amt,
                stg.sub_agnt_not_fnd_err as sub_agnt_not_fnd_err,
                stg.afsi_err as afsi_err,
                stg.trd_ptnr_rol_plyr_anchr_id as trd_ptnr_rol_plyr_anchr_id,
                stg.inv_trd_ptnr_err as inv_trd_ptnr_err,
                stg.zrptr_sapd_id as zrptr_sapd_id,
                stg.src_sys_cd as src_sys_cd,
                stg.sys_prcg_cd as sys_prcg_cd,
                'null' as bil_sys_cd,
                'null' as exc_cd,
                coalesce(
                    trim(substr(stg.finc_acct_nbr, 5, 6)), 'null'
                ) as acct_dervn_finc_acct_nbr,
                'SAPE' as acct_dervn_knd_cd,
                'null' as onset_offset_ind,
                'null' as pl_loc_ori_cd,
                'null' as retro_ratg_cd,
                'null' as ntr_of_provsn_cd,
                stg.reins_sprd_err as reins_sprd_err,
                stg.etl_proc_ind as etl_proc_ind,
                stg.reinsr_finc_serv_rol_for_reins_rol_plyr_anchr_id  -- Added for REMOD
            from stg_sap_dtl stg
        ) a
    left outer join
        acct_dervn acct_dervn
        on (
            a.assm_reins_agmt_anchr_id = acct_dervn.assm_reins_agmt_anchr_id
            and a.cede_reins_agmt_anchr_id = acct_dervn.cede_reins_agmt_anchr_id
            and a.cmtn_agmt_anchr_id = acct_dervn.cmtn_agmt_anchr_id
            and a.co_rol_plyr_anchr_id = acct_dervn.co_rol_plyr_anchr_id
            and a.prem_postal_regn_anchr_id = acct_dervn.prem_postal_regn_anchr_id
            and a.trd_ptnr_rol_plyr_anchr_id = acct_dervn.trd_ptnr_rol_plyr_anchr_id
            and trim(a.cst_ctr_id) = trim(acct_dervn.cst_ctr_id)
            and trim(a.cst_ctr_lvl1_nm) = trim(acct_dervn.cst_ctr_txt_1)
            and trim(a.cst_ctr_lvl2_nm) = trim(acct_dervn.cst_ctr_txt_2)
            and trim(a.finc_doc_typ_cd) = trim(acct_dervn.doc_typ_cd)
            and trim(a.finc_lob_cd) = trim(acct_dervn.finc_lob_cd)
            and trim(a.org_id) = trim(acct_dervn.low_lvl_org_id)
            and trim(a.pl_loc_ori_cd) = trim(acct_dervn.pl_loc_ori_cd)
            and trim(a.prft_ctr_id) = trim(acct_dervn.prft_ctr_id)
            and trim(a.dac_cd) = trim(acct_dervn.dac_cd)
            and trim(a.prft_ctr_lvl1_nm) = trim(acct_dervn.prft_ctr_txt_1)
            and trim(a.prft_ctr_lvl2_nm) = trim(acct_dervn.prft_ctr_txt_2)
            and trim(a.onset_offset_ind) = trim(acct_dervn.onset_offset_ind)
            and trim(a.cmtn_ind) = trim(acct_dervn.cmtn_ind)
            and trim(a.acc_yy) = trim(acct_dervn.acc_ccyy)
            and a.rcar_anchr_id = acct_dervn.reins_cmtn_agmt_rel_id
            and trim(a.dac_cd) = trim(acct_dervn.dac_cd)
            and trim(a.finc_lob_cd) = trim(acct_dervn.finc_lob_cd)
            and trim(a.pol_yy) = trim(acct_dervn.pol_ccyy)
            and trim(a.retro_ratg_cd) = trim(acct_dervn.retro_ratg_cd)
            and trim(a.bil_sys_cd) = trim(acct_dervn.bil_sys_cd)
            and trim(acct_dervn.knd_cd) = 'SAPE'
            and acct_dervn.actv_rec_ind = 'Y'
            and trim(acct_dervn.finc_acct_nbr) = trim(a.acct_dervn_finc_acct_nbr)
            and acct_dervn.typ_id in (
                select typ_id
                from typ
                where trim(description) = 'Financial Account' and actv_rec_ind = 'Y'
            )
            and acct_dervn.actv_rec_ind = 'Y'
            and acct_dervn.finc_servs_rol_for_reins_reinsr_rol_plyr_anchr_id
            = a.reinsr_finc_serv_rol_for_reins_rol_plyr_anchr_id
        )  -- ReMod Phase 1  
    left outer join
        pers pers
        on (
            trim(pers.extl_refr_cd) = trim(a.ent_usr_id)
            and pers.actv_rec_ind = 'Y'
            and pers.typ_id = 439
        )
    left outer join
        (
            select typ_id as mon_acct_typ_id
            from typ
            where trim(description) = 'Monetary Account' and actv_rec_ind = 'Y'
        ) mon_acct_typ
        on 1 = 1
    left outer join
        (
            select typ_id as finc_acct_typ_id
            from typ
            where trim(description) = 'Financial Account' and actv_rec_ind = 'Y'
        ) finc_acct_typ
        on 1 = 1
    left outer join
        (
            select typ_id as finc_adj_typ_id
            from typ
            where trim(description) = 'Financial Adjustments' and actv_rec_ind = 'Y'
        ) finc_adj_typ
        on 1 = 1
    where
        acct_dervn.drvd_acct_anchr_id is null
        and trim(a.reins_sprd_err) = '000'
        and a.etl_proc_ind = 'N'
        and trim(a.reins_not_fnd_err) = '000'  -- Added for CR 12221 on 11/10/2013
        and trim(a.inv_doc_typ_err) <> '066'  -- Added for CR 17802
        and trim(a.inv_acct_err) <> '065'  -- Added for CR 17802),
r_exp_set_audit_cols as (
    select
        actv_rec_ind as in_dummy,
        -- *INF*: TO_DATE('2999-12-31-00.00.00.000000', 'YYYY-MM-DD HH24:MI:SS NS')
        to_timestamp(
            '2999-12-31-00.00.00.000000', 'YYYY-MM-DD HH24:MI:SS NS'
        ) as out_eff_to_tistmp,
        current_timestamp as out_eff_fm_tistmp,
        {{ var("batch_id") }} as out_batch_id,
        {{ var("pop_info_id") }} as out_pop_info_id
    from sq_stg_sap_dtl_n_rec
),
exp_set_vals as (
    select
        sq_stg_sap_dtl_n_rec.ent_usr_id,
        sq_stg_sap_dtl_n_rec.ent_usr_nm,
        sq_stg_sap_dtl_n_rec.gledgr_post_dt,
        sq_stg_sap_dtl_n_rec.finc_co_cd,
        sq_stg_sap_dtl_n_rec.finc_acct_nbr,
        sq_stg_sap_dtl_n_rec.new_co_cd,
        sq_stg_sap_dtl_n_rec.ent_dt,
        sq_stg_sap_dtl_n_rec.finc_doc_typ_cd,
        sq_stg_sap_dtl_n_rec.doc_nbr,
        sq_stg_sap_dtl_n_rec.free_frm_txt_desc,
        sq_stg_sap_dtl_n_rec.free_frm_hdr_txt_desc,
        sq_stg_sap_dtl_n_rec.rvrsl_dt,
        sq_stg_sap_dtl_n_rec.deb_cr_cd,
        sq_stg_sap_dtl_n_rec.org_id,
        sq_stg_sap_dtl_n_rec.prft_ctr_id,
        sq_stg_sap_dtl_n_rec.prft_ctr_lvl1_nm,
        sq_stg_sap_dtl_n_rec.prft_ctr_lvl2_nm,
        sq_stg_sap_dtl_n_rec.cst_ctr_id,
        sq_stg_sap_dtl_n_rec.cst_ctr_lvl1_nm,
        sq_stg_sap_dtl_n_rec.cst_ctr_lvl2_nm,
        sq_stg_sap_dtl_n_rec.yy_lob_cd,
        sq_stg_sap_dtl_n_rec.ln_itm_txt_desc,
        sq_stg_sap_dtl_n_rec.st_cd,
        sq_stg_sap_dtl_n_rec.acc_yy,
        sq_stg_sap_dtl_n_rec.mvmnt_typ_cd,
        sq_stg_sap_dtl_n_rec.reinsr_id,
        sq_stg_sap_dtl_n_rec.reins_agmt,
        sq_stg_sap_dtl_n_rec.sub_agnt,
        sq_stg_sap_dtl_n_rec.trd_ptnr_cd,
        sq_stg_sap_dtl_n_rec.amt,
        sq_stg_sap_dtl_n_rec.home_ofc_lob_cd,
        sq_stg_sap_dtl_n_rec.prem_postal_regn_anchr_id,
        sq_stg_sap_dtl_n_rec.finc_lob_cd,
        sq_stg_sap_dtl_n_rec.opert_co_cd,
        sq_stg_sap_dtl_n_rec.co_rol_plyr_anchr_id,
        sq_stg_sap_dtl_n_rec.dac_cd,
        sq_stg_sap_dtl_n_rec.acct_ent_knd_cd,
        sq_stg_sap_dtl_n_rec.pol_yy,
        sq_stg_sap_dtl_n_rec.inv_co_cd_err,
        sq_stg_sap_dtl_n_rec.inv_doc_typ_err,
        sq_stg_sap_dtl_n_rec.inv_finc_lob_err,
        sq_stg_sap_dtl_n_rec.sap_co_cd_not_fnd_err,
        sq_stg_sap_dtl_n_rec.inv_acct_err,
        sq_stg_sap_dtl_n_rec.cmtn_ind,
        sq_stg_sap_dtl_n_rec.cmtn_agmt_anchr_id,
        sq_stg_sap_dtl_n_rec.assm_reins_agmt_anchr_id,
        sq_stg_sap_dtl_n_rec.cede_reins_agmt_anchr_id,
        sq_stg_sap_dtl_n_rec.reins_not_fnd_err,
        sq_stg_sap_dtl_n_rec.reins_agmt_anchr_id,
        sq_stg_sap_dtl_n_rec.reins_agmt_knd_cd,
        sq_stg_sap_dtl_n_rec.org_id_err,
        sq_stg_sap_dtl_n_rec.reins_typ,
        sq_stg_sap_dtl_n_rec.za_grp_cd,
        sq_stg_sap_dtl_n_rec.dac_mismatch_err,
        sq_stg_sap_dtl_n_rec.reins_fein_nbr,
        sq_stg_sap_dtl_n_rec.reins_sub_agnt,
        sq_stg_sap_dtl_n_rec.par_rt,
        sq_stg_sap_dtl_n_rec.rcar_anchr_id,
        sq_stg_sap_dtl_n_rec.sub_agnt_amt,
        sq_stg_sap_dtl_n_rec.sub_agnt_not_fnd_err,
        sq_stg_sap_dtl_n_rec.afsi_err,
        sq_stg_sap_dtl_n_rec.trd_ptnr_rol_plyr_anchr_id,
        sq_stg_sap_dtl_n_rec.inv_trd_ptnr_err,
        sq_stg_sap_dtl_n_rec.zrptr_sapd_id,
        sq_stg_sap_dtl_n_rec.src_sys_cd,
        sq_stg_sap_dtl_n_rec.sys_prcg_cd,
        sq_stg_sap_dtl_n_rec.bil_sys_cd,
        sq_stg_sap_dtl_n_rec.exc_cd,
        sq_stg_sap_dtl_n_rec.acct_dervn_finc_acct_nbr,
        sq_stg_sap_dtl_n_rec.acct_dervn_knd_cd,
        sq_stg_sap_dtl_n_rec.onset_offset_ind,
        sq_stg_sap_dtl_n_rec.pl_loc_ori_cd,
        sq_stg_sap_dtl_n_rec.retro_ratg_cd,
        sq_stg_sap_dtl_n_rec.ntr_of_provsn_cd,
        sq_stg_sap_dtl_n_rec.drvd_acct_anchr_id,
        sq_stg_sap_dtl_n_rec.acct_dervn_src_ind,
        sq_stg_sap_dtl_n_rec.mon_acct_typ_id,
        sq_stg_sap_dtl_n_rec.finc_acct_typ_id,
        sq_stg_sap_dtl_n_rec.finc_adj_typ_id,
        sq_stg_sap_dtl_n_rec.pers_rol_plyr_anchr_id,
        sq_stg_sap_dtl_n_rec.sts_cd,
        sq_stg_sap_dtl_n_rec.bal_amt,
        sq_stg_sap_dtl_n_rec.distb_amt,
        sq_stg_sap_dtl_n_rec.bal_dt,
        sq_stg_sap_dtl_n_rec.actv_rec_ind,
        sq_stg_sap_dtl_n_rec.eff_to_tistmp,
        sq_stg_sap_dtl_n_rec.finc_ins_typ_cd,
        r_exp_set_audit_cols.out_eff_fm_tistmp as eff_fm_tistmp,
        r_exp_set_audit_cols.out_batch_id as batch_id,
        r_exp_set_audit_cols.out_pop_info_id as pop_info_id,
        current_timestamp as out_etl_load_tistmp,
        sq_stg_sap_dtl_n_rec.fsri_rol_plyr_anchr_id
    from sq_stg_sap_dtl_n_rec
-- Manually join with r_EXP_SET_AUDIT_COLS
),
agg_acct_id as (
    select
        assm_reins_agmt_anchr_id,
        cede_reins_agmt_anchr_id,
        cmtn_agmt_anchr_id,
        cmtn_ind,
        co_rol_plyr_anchr_id,
        prem_postal_regn_anchr_id,
        rcar_anchr_id,
        trd_ptnr_rol_plyr_anchr_id,
        pol_yy,
        acc_yy,
        bil_sys_cd,
        cst_ctr_id,
        cst_ctr_lvl1_nm,
        cst_ctr_lvl2_nm,
        dac_cd,
        finc_doc_typ_cd,
        exc_cd,
        acct_dervn_finc_acct_nbr,
        acct_dervn_knd_cd,
        opert_co_cd,
        finc_ins_typ_cd,
        finc_lob_cd,
        org_id,
        onset_offset_ind,
        pl_loc_ori_cd,
        retro_ratg_cd,
        pol_yy as pol_yy1,
        prft_ctr_id,
        prft_ctr_lvl1_nm,
        prft_ctr_lvl2_nm,
        mon_acct_typ_id,
        finc_acct_typ_id,
        actv_rec_ind,
        eff_to_tistmp,
        eff_fm_tistmp,
        batch_id,
        pop_info_id,
        out_etl_load_tistmp as etl_load_tistmp,
        fsri_rol_plyr_anchr_id
    from exp_set_vals
    qualify
        row_number() over (
            partition by
                assm_reins_agmt_anchr_id,
                cede_reins_agmt_anchr_id,
                cmtn_agmt_anchr_id,
                cmtn_ind,
                co_rol_plyr_anchr_id,
                prem_postal_regn_anchr_id,
                rcar_anchr_id,
                trd_ptnr_rol_plyr_anchr_id,
                pol_yy,
                acc_yy,
                bil_sys_cd,
                cst_ctr_id,
                cst_ctr_lvl1_nm,
                cst_ctr_lvl2_nm,
                dac_cd,
                finc_doc_typ_cd,
                exc_cd,
                acct_dervn_finc_acct_nbr,
                acct_dervn_knd_cd,
                opert_co_cd,
                finc_ins_typ_cd,
                finc_lob_cd,
                org_id,
                onset_offset_ind,
                pl_loc_ori_cd,
                retro_ratg_cd,
                pol_yy1,
                prft_ctr_id,
                prft_ctr_lvl1_nm,
                prft_ctr_lvl2_nm,
                mon_acct_typ_id,
                finc_acct_typ_id,
                actv_rec_ind,
                fsri_rol_plyr_anchr_id
            order by null
        )
        = 1
),
r_seq_acct_id as (
	CREATE SEQUENCE r_SEQ_ACCT_ID
	START = 0
	INCREMENT = 1;),r_exp_seq_acct_anchr_id as (
    select
        nextval as in_nextval,
        {{ var("seq_acct_anchr_id") }} || in_nextval as out_sequence
    from r_seq_acct_id
),
exp_pass_thru as (
    select
        r_exp_seq_acct_anchr_id.out_sequence as acct_id,
        agg_acct_id.assm_reins_agmt_anchr_id,
        agg_acct_id.cede_reins_agmt_anchr_id,
        agg_acct_id.cmtn_agmt_anchr_id,
        agg_acct_id.cmtn_ind,
        agg_acct_id.co_rol_plyr_anchr_id,
        agg_acct_id.prem_postal_regn_anchr_id,
        agg_acct_id.rcar_anchr_id,
        agg_acct_id.trd_ptnr_rol_plyr_anchr_id,
        agg_acct_id.pol_yy,
        agg_acct_id.acc_yy,
        agg_acct_id.bil_sys_cd,
        agg_acct_id.cst_ctr_id,
        agg_acct_id.cst_ctr_lvl1_nm,
        agg_acct_id.cst_ctr_lvl2_nm,
        agg_acct_id.dac_cd,
        agg_acct_id.finc_doc_typ_cd,
        agg_acct_id.exc_cd,
        agg_acct_id.acct_dervn_finc_acct_nbr,
        agg_acct_id.acct_dervn_knd_cd,
        agg_acct_id.opert_co_cd,
        agg_acct_id.finc_ins_typ_cd,
        agg_acct_id.finc_lob_cd,
        agg_acct_id.org_id,
        agg_acct_id.onset_offset_ind,
        agg_acct_id.pl_loc_ori_cd,
        agg_acct_id.retro_ratg_cd,
        agg_acct_id.pol_yy1,
        agg_acct_id.prft_ctr_id,
        agg_acct_id.prft_ctr_lvl1_nm,
        agg_acct_id.prft_ctr_lvl2_nm,
        agg_acct_id.eff_to_tistmp,
        agg_acct_id.eff_fm_tistmp,
        agg_acct_id.batch_id,
        agg_acct_id.pop_info_id,
        agg_acct_id.etl_load_tistmp,
        agg_acct_id.mon_acct_typ_id,
        agg_acct_id.finc_acct_typ_id,
        agg_acct_id.actv_rec_ind,
        'N' as retro_cede_ind,
        agg_acct_id.fsri_rol_plyr_anchr_id
    from agg_acct_id
-- Manually join with r_EXP_SEQ_ACCT_ANCHR_ID
),
select
    acct_id as acct_id,
    mon_acct_typ_id as typ_id,
    pop_info_id as pop_info_id,
    pop_info_id as updt_pop_info_id,
    batch_id as bat_id
from exp_pass_thru
