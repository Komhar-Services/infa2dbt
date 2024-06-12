{{
	config(
		materialized = "incremental"
	)
}}

with
pers as (select * from {{ source("dbaall", "pers") }}),
work_prcg_cyc_dt as (select * from {{ source("dbaall", "work_prcg_cyc_dt") }}),
stg_sap_dtl as (select * from {{ source("dbaall", "stg_sap_dtl") }}),
typ as (select * from {{ source("dbaall", "typ") }}),
acct_dervn as (select * from {{ source("dbaall", "acct_dervn") }}),
sq_stg_sap_dtl_y_rec as (
    select
        b.ent_usr_id as ent_usr_id,
        b.ent_usr_nm as ent_usr_nm,
        b.gledgr_post_dt as gledgr_post_dt,
        b.finc_co_cd as finc_co_cd,
        b.finc_acct_nbr as finc_acct_nbr,
        b.new_co_cd as new_co_cd,
        b.ent_dt as ent_dt,
        b.finc_doc_typ_cd as finc_doc_typ_cd,
        b.doc_nbr as doc_nbr,
        b.free_frm_txt_desc as free_frm_txt_desc,
        b.free_frm_hdr_txt_desc as free_frm_hdr_txt_desc,
        b.rvrsl_dt as rvrsl_dt,
        b.deb_cr_cd as deb_cr_cd,
        b.org_id as org_id,
        b.prft_ctr_id as prft_ctr_id,
        b.prft_ctr_lvl1_nm as prft_ctr_lvl1_nm,
        b.prft_ctr_lvl2_nm as prft_ctr_lvl2_nm,
        b.cst_ctr_id as cst_ctr_id,
        b.cst_ctr_lvl1_nm as cst_ctr_lvl1_nm,
        b.cst_ctr_lvl2_nm as cst_ctr_lvl2_nm,
        b.yy_lob_cd as yy_lob_cd,
        b.ln_itm_txt_desc as ln_itm_txt_desc,
        b.st_cd as st_cd,
        b.acc_yy as acc_yy,
        b.mvmnt_typ_cd as mvmnt_typ_cd,
        b.reinsr_id as reinsr_id,
        b.reins_agmt as reins_agmt,
        b.sub_agnt as sub_agnt,
        b.trd_ptnr_cd as trd_ptnr_cd,
        case when b.amt is null then 0 else b.amt end as amt,
        b.home_ofc_lob_cd as home_ofc_lob_cd,
        b.prem_postal_regn_anchr_id as prem_postal_regn_anchr_id,
        b.finc_lob_cd as finc_lob_cd,
        b.opert_co_cd as opert_co_cd,
        b.co_rol_plyr_anchr_id as co_rol_plyr_anchr_id,
        b.dac_cd as dac_cd,
        b.acct_ent_knd_cd as acct_ent_knd_cd,
        b.pol_yy as pol_yy,
        b.inv_co_cd_err as inv_co_cd_err,
        b.inv_doc_typ_err as inv_doc_typ_err,
        b.inv_finc_lob_err as inv_finc_lob_err,
        b.sap_co_cd_not_fnd_err as sap_co_cd_not_fnd_err,
        b.inv_acct_err as inv_acct_err,
        b.cmtn_ind as cmtn_ind,
        b.cmtn_agmt_anchr_id as cmtn_agmt_anchr_id,
        b.assm_reins_agmt_anchr_id as assm_reins_agmt_anchr_id,
        b.cede_reins_agmt_anchr_id as cede_reins_agmt_anchr_id,
        b.reins_not_fnd_err as reins_not_fnd_err,
        b.reins_agmt_anchr_id as reins_agmt_anchr_id,
        b.reins_agmt_knd_cd as reins_agmt_knd_cd,
        b.org_id_err as org_id_err,
        b.reins_typ as reins_typ,
        b.za_grp_cd as za_grp_cd,
        b.dac_mismatch_err as dac_mismatch_err,
        b.reins_fein_nbr as reins_fein_nbr,
        b.reins_sub_agnt as reins_sub_agnt,
        b.par_rt as par_rt,
        b.rcar_anchr_id as rcar_anchr_id,
        case when b.sub_agnt_amt is null then 0 else b.sub_agnt_amt end as sub_agnt_amt,
        b.sub_agnt_not_fnd_err as sub_agnt_not_fnd_err,
        b.afsi_err as afsi_err,
        b.trd_ptnr_rol_plyr_anchr_id as trd_ptnr_rol_plyr_anchr_id,
        b.inv_trd_ptnr_err as inv_trd_ptnr_err,
        b.zrptr_sapd_id as zrptr_sapd_id,
        trim(b.src_sys_cd) as src_sys_cd,
        trim(b.sys_prcg_cd) as sys_prcg_cd,
        b.bil_sys_cd,
        b.exc_cd,
        b.acct_dervn_finc_acct_nbr,
        b.acct_dervn_knd_cd,
        b.onset_offset_ind,
        b.pl_loc_ori_cd,
        b.retro_ratg_cd,
        b.ntr_of_provsn_cd,
        b.drvd_acct_anchr_id as drvd_acct_anchr_id,
        case
            when b.drvd_acct_anchr_id is null then 'N' else 'Y'
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
        trim(wpcd.finc_perd_ccyy_mm) as finc_perd_ccyy_mm,
        -- C2Z Phase 2
        case
            when b.tran_sub_agnt_amt is null then 0 else b.tran_sub_agnt_amt
        end as tran_sub_agnt_amt,
        case
            when b.contr_sub_agnt_amt is null then 0 else b.contr_sub_agnt_amt
        end as contr_sub_agnt_amt,
        b.crcy_cd as crcy_cd,
        b.tran_crcy_cd as tran_crcy_cd,
        b.contr_crcy_cd as contr_crcy_cd
    from
        (
            select
                b.ent_usr_id as ent_usr_id,
                b.ent_usr_nm as ent_usr_nm,
                b.gledgr_post_dt as gledgr_post_dt,
                b.finc_co_cd as finc_co_cd,
                b.finc_acct_nbr as finc_acct_nbr,
                b.new_co_cd as new_co_cd,
                b.ent_dt as ent_dt,
                b.finc_doc_typ_cd as finc_doc_typ_cd,
                b.doc_nbr as doc_nbr,
                b.free_frm_txt_desc as free_frm_txt_desc,
                b.free_frm_hdr_txt_desc as free_frm_hdr_txt_desc,
                b.rvrsl_dt as rvrsl_dt,
                b.deb_cr_cd as deb_cr_cd,
                b.org_id as org_id,
                b.prft_ctr_id as prft_ctr_id,
                b.prft_ctr_lvl1_nm as prft_ctr_lvl1_nm,
                b.prft_ctr_lvl2_nm as prft_ctr_lvl2_nm,
                b.cst_ctr_id as cst_ctr_id,
                b.cst_ctr_lvl1_nm as cst_ctr_lvl1_nm,
                b.cst_ctr_lvl2_nm as cst_ctr_lvl2_nm,
                b.yy_lob_cd as yy_lob_cd,
                b.ln_itm_txt_desc as ln_itm_txt_desc,
                b.st_cd as st_cd,
                b.acc_yy as acc_yy,
                b.mvmnt_typ_cd as mvmnt_typ_cd,
                b.reinsr_id as reinsr_id,
                b.reins_agmt as reins_agmt,
                b.sub_agnt as sub_agnt,
                b.trd_ptnr_cd as trd_ptnr_cd,
                b.amt as amt,
                b.home_ofc_lob_cd as home_ofc_lob_cd,
                b.prem_postal_regn_anchr_id as prem_postal_regn_anchr_id,
                b.finc_lob_cd as finc_lob_cd,
                b.opert_co_cd as opert_co_cd,
                b.co_rol_plyr_anchr_id as co_rol_plyr_anchr_id,
                b.dac_cd as dac_cd,
                b.acct_ent_knd_cd as acct_ent_knd_cd,
                b.pol_yy as pol_yy,
                b.inv_co_cd_err as inv_co_cd_err,
                b.inv_doc_typ_err as inv_doc_typ_err,
                b.inv_finc_lob_err as inv_finc_lob_err,
                b.sap_co_cd_not_fnd_err as sap_co_cd_not_fnd_err,
                b.inv_acct_err as inv_acct_err,
                b.cmtn_ind as cmtn_ind,
                b.cmtn_agmt_anchr_id as cmtn_agmt_anchr_id,
                b.assm_reins_agmt_anchr_id as assm_reins_agmt_anchr_id,
                b.cede_reins_agmt_anchr_id as cede_reins_agmt_anchr_id,
                b.reins_not_fnd_err as reins_not_fnd_err,
                b.reins_agmt_anchr_id as reins_agmt_anchr_id,
                b.reins_agmt_knd_cd as reins_agmt_knd_cd,
                b.org_id_err as org_id_err,
                b.reins_typ as reins_typ,
                b.za_grp_cd as za_grp_cd,
                b.dac_mismatch_err as dac_mismatch_err,
                b.reins_fein_nbr as reins_fein_nbr,
                b.reins_sub_agnt as reins_sub_agnt,
                b.par_rt as par_rt,
                b.rcar_anchr_id as rcar_anchr_id,
                b.sub_agnt_amt as sub_agnt_amt,
                b.sub_agnt_not_fnd_err as sub_agnt_not_fnd_err,
                b.afsi_err as afsi_err,
                b.trd_ptnr_rol_plyr_anchr_id as trd_ptnr_rol_plyr_anchr_id,
                b.inv_trd_ptnr_err as inv_trd_ptnr_err,
                b.zrptr_sapd_id as zrptr_sapd_id,
                b.src_sys_cd as src_sys_cd,
                b.sys_prcg_cd as sys_prcg_cd,
                b.bil_sys_cd as bil_sys_cd,
                b.exc_cd as exc_cd,
                b.acct_dervn_finc_acct_nbr as acct_dervn_finc_acct_nbr,
                b.acct_dervn_knd_cd as acct_dervn_knd_cd,
                b.onset_offset_ind as onset_offset_ind,
                b.pl_loc_ori_cd as pl_loc_ori_cd,
                b.retro_ratg_cd as retro_ratg_cd,
                b.ntr_of_provsn_cd as ntr_of_provsn_cd,
                b.reins_sprd_err as reins_sprd_err,
                b.etl_proc_ind as etl_proc_ind,
                acct_dervn.drvd_acct_anchr_id as drvd_acct_anchr_id,
                -- C2Z Phase 2
                b.tran_sub_agnt_amt as tran_sub_agnt_amt,
                b.contr_sub_agnt_amt as contr_sub_agnt_amt,
                b.crcy_cd as crcy_cd,
                b.tran_crcy_cd as tran_crcy_cd,
                b.contr_crcy_cd as contr_crcy_cd

            from
                (
                    select
                        coalesce(stg.ent_usr_id, 'null') as ent_usr_id,
                        coalesce(stg.ent_usr_nm, 'null') as ent_usr_nm,
                        stg.gledgr_post_dt as gledgr_post_dt,
                        coalesce(stg.finc_co_cd, 'null') as finc_co_cd,
                        coalesce(
                            substr(stg.finc_acct_nbr, 5, 6), 'null'
                        ) as finc_acct_nbr,
                        coalesce(stg.new_co_cd, 'null') as new_co_cd,
                        stg.ent_dt as ent_dt,
                        coalesce(stg.finc_doc_typ_cd, 'null') as finc_doc_typ_cd,
                        coalesce(stg.doc_nbr, 'null') as doc_nbr,
                        coalesce(stg.free_frm_txt_desc, 'null') as free_frm_txt_desc,
                        coalesce(
                            stg.free_frm_hdr_txt_desc, 'null'
                        ) as free_frm_hdr_txt_desc,
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
                        stg.finc_lob_cd as finc_lob_cd,
                        stg.opert_co_cd as opert_co_cd,
                        stg.co_rol_plyr_anchr_id as co_rol_plyr_anchr_id,
                        stg.dac_cd as dac_cd,
                        stg.acct_ent_knd_cd as acct_ent_knd_cd,
                        stg.pol_yy as pol_yy,
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
                            substr(stg.finc_acct_nbr, 5, 6), 'null'
                        ) as acct_dervn_finc_acct_nbr,
                        'SAPE' as acct_dervn_knd_cd,
                        'null' as onset_offset_ind,
                        'null' as pl_loc_ori_cd,
                        'null' as retro_ratg_cd,
                        'null' as ntr_of_provsn_cd,
                        stg.reins_sprd_err as reins_sprd_err,
                        stg.etl_proc_ind as etl_proc_ind,
                        -- C2Z Phase 2
                        stg.tran_sub_agnt_amt as tran_sub_agnt_amt,
                        stg.contr_sub_agnt_amt as contr_sub_agnt_amt,
                        replace(stg.crcy_cd, '"', '') as crcy_cd,
                        replace(stg.tran_crcy_cd, '"', '') as tran_crcy_cd,
                        replace(stg.contr_crcy_cd, '"', '') as contr_crcy_cd,
                        stg.reinsr_finc_serv_rol_for_reins_rol_plyr_anchr_id  -- Added for REMOD
                    from stg_sap_dtl stg
                ) b
            left outer join
                acct_dervn acct_dervn
                on (
                    b.assm_reins_agmt_anchr_id = acct_dervn.assm_reins_agmt_anchr_id
                    and b.cede_reins_agmt_anchr_id = acct_dervn.cede_reins_agmt_anchr_id
                    and b.cmtn_agmt_anchr_id = acct_dervn.cmtn_agmt_anchr_id
                    and b.co_rol_plyr_anchr_id = acct_dervn.co_rol_plyr_anchr_id
                    and b.prem_postal_regn_anchr_id
                    = acct_dervn.prem_postal_regn_anchr_id
                    and b.trd_ptnr_rol_plyr_anchr_id
                    = acct_dervn.trd_ptnr_rol_plyr_anchr_id
                    and trim(b.cst_ctr_id) = trim(acct_dervn.cst_ctr_id)
                    and trim(b.cst_ctr_lvl1_nm) = trim(acct_dervn.cst_ctr_txt_1)
                    and trim(b.cst_ctr_lvl2_nm) = trim(acct_dervn.cst_ctr_txt_2)
                    and trim(b.finc_doc_typ_cd) = trim(acct_dervn.doc_typ_cd)
                    and trim(b.finc_lob_cd) = trim(acct_dervn.finc_lob_cd)
                    and trim(b.org_id) = trim(acct_dervn.low_lvl_org_id)
                    and trim(b.pl_loc_ori_cd) = trim(acct_dervn.pl_loc_ori_cd)
                    and trim(b.prft_ctr_id) = trim(acct_dervn.prft_ctr_id)
                    and trim(b.dac_cd) = trim(acct_dervn.dac_cd)
                    and trim(b.prft_ctr_lvl1_nm) = trim(acct_dervn.prft_ctr_txt_1)
                    and trim(b.prft_ctr_lvl2_nm) = trim(acct_dervn.prft_ctr_txt_2)
                    and trim(b.onset_offset_ind) = trim(acct_dervn.onset_offset_ind)
                    and trim(b.cmtn_ind) = trim(acct_dervn.cmtn_ind)
                    and trim(b.acc_yy) = trim(acct_dervn.acc_ccyy)
                    and b.rcar_anchr_id = acct_dervn.reins_cmtn_agmt_rel_id
                    and trim(b.dac_cd) = trim(acct_dervn.dac_cd)
                    and trim(b.finc_lob_cd) = trim(acct_dervn.finc_lob_cd)
                    and trim(b.pol_yy) = trim(acct_dervn.pol_ccyy)
                    and trim(b.retro_ratg_cd) = trim(acct_dervn.retro_ratg_cd)
                    and trim(b.bil_sys_cd) = trim(acct_dervn.bil_sys_cd)
                    and trim(acct_dervn.knd_cd) = 'SAPE'
                    and acct_dervn.actv_rec_ind = 'Y'
                    and trim(b.acct_dervn_finc_acct_nbr)
                    = trim(acct_dervn.finc_acct_nbr)
                    and acct_dervn.typ_id in (
                        select typ_id
                        from typ
                        where
                            trim(description) = 'Financial Account'
                            and actv_rec_ind = 'Y'
                    )
                    and acct_dervn.actv_rec_ind = 'Y'
                    and acct_dervn.finc_servs_rol_for_reins_reinsr_rol_plyr_anchr_id
                    = b.reinsr_finc_serv_rol_for_reins_rol_plyr_anchr_id
                )  -- Added for Remod
        ) b
    left outer join
        pers pers
        on (
            trim(pers.extl_refr_cd) = trim(b.ent_usr_id)
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
    left outer join
        (
            select finc_perd_ccyy_mm as finc_perd_ccyy_mm
            from work_prcg_cyc_dt
            where
                typ_id in (
                    select typ_id
                    from typ
                    where nm = 'MONTHLY_REINSURANCE' and actv_rec_ind = 'Y'
                )
                and actv_rec_ind = 'Y'
        ) wpcd
        on 1 = 1
    where
        b.drvd_acct_anchr_id is not null
        and trim(b.reins_sprd_err) = '000'
        and b.etl_proc_ind = 'N'
        and trim(b.reins_not_fnd_err) = '000'  -- Added for CR 12221 on 11/10/2013
        and trim(b.inv_doc_typ_err) <> '066'  -- Added for CR 17802
        and trim(b.inv_acct_err) <> '065'  -- Added for CR 17802),
r_exp_set_audit_cols1 as (
    select
        ent_usr_nm as in_dummy,
        -- *INF*: TO_DATE('2999-12-31-00.00.00.000000', 'YYYY-MM-DD HH24:MI:SS NS')
        to_timestamp(
            '2999-12-31-00.00.00.000000', 'YYYY-MM-DD HH24:MI:SS NS'
        ) as out_eff_to_tistmp,
        current_timestamp as out_eff_fm_tistmp,
        {{ var("batch_id") }} as out_batch_id,
        {{ var("pop_info_id") }} as out_pop_info_id
    from sq_stg_sap_dtl_y_rec
)
exp_set_val as (
    select
        sq_stg_sap_dtl_y_rec.ent_usr_id,
        sq_stg_sap_dtl_y_rec.ent_usr_nm,
        sq_stg_sap_dtl_y_rec.gledgr_post_dt,
        sq_stg_sap_dtl_y_rec.finc_co_cd,
        sq_stg_sap_dtl_y_rec.finc_acct_nbr,
        sq_stg_sap_dtl_y_rec.new_co_cd,
        sq_stg_sap_dtl_y_rec.ent_dt,
        sq_stg_sap_dtl_y_rec.finc_doc_typ_cd,
        sq_stg_sap_dtl_y_rec.doc_nbr,
        sq_stg_sap_dtl_y_rec.free_frm_txt_desc,
        sq_stg_sap_dtl_y_rec.free_frm_hdr_txt_desc,
        sq_stg_sap_dtl_y_rec.rvrsl_dt,
        sq_stg_sap_dtl_y_rec.deb_cr_cd,
        sq_stg_sap_dtl_y_rec.org_id,
        sq_stg_sap_dtl_y_rec.prft_ctr_id,
        sq_stg_sap_dtl_y_rec.prft_ctr_lvl1_nm,
        sq_stg_sap_dtl_y_rec.prft_ctr_lvl2_nm,
        sq_stg_sap_dtl_y_rec.cst_ctr_id,
        sq_stg_sap_dtl_y_rec.cst_ctr_lvl1_nm,
        sq_stg_sap_dtl_y_rec.cst_ctr_lvl2_nm,
        sq_stg_sap_dtl_y_rec.yy_lob_cd,
        sq_stg_sap_dtl_y_rec.ln_itm_txt_desc,
        sq_stg_sap_dtl_y_rec.st_cd,
        sq_stg_sap_dtl_y_rec.acc_yy,
        sq_stg_sap_dtl_y_rec.mvmnt_typ_cd,
        sq_stg_sap_dtl_y_rec.reinsr_id,
        sq_stg_sap_dtl_y_rec.reins_agmt,
        sq_stg_sap_dtl_y_rec.sub_agnt,
        sq_stg_sap_dtl_y_rec.trd_ptnr_cd,
        sq_stg_sap_dtl_y_rec.amt,
        sq_stg_sap_dtl_y_rec.home_ofc_lob_cd,
        sq_stg_sap_dtl_y_rec.prem_postal_regn_anchr_id,
        sq_stg_sap_dtl_y_rec.finc_lob_cd,
        sq_stg_sap_dtl_y_rec.opert_co_cd,
        sq_stg_sap_dtl_y_rec.co_rol_plyr_anchr_id,
        sq_stg_sap_dtl_y_rec.dac_cd,
        sq_stg_sap_dtl_y_rec.acct_ent_knd_cd,
        sq_stg_sap_dtl_y_rec.pol_yy,
        sq_stg_sap_dtl_y_rec.inv_co_cd_err,
        sq_stg_sap_dtl_y_rec.inv_doc_typ_err,
        sq_stg_sap_dtl_y_rec.inv_finc_lob_err,
        sq_stg_sap_dtl_y_rec.sap_co_cd_not_fnd_err,
        sq_stg_sap_dtl_y_rec.inv_acct_err,
        sq_stg_sap_dtl_y_rec.cmtn_ind,
        sq_stg_sap_dtl_y_rec.cmtn_agmt_anchr_id,
        sq_stg_sap_dtl_y_rec.assm_reins_agmt_anchr_id,
        sq_stg_sap_dtl_y_rec.cede_reins_agmt_anchr_id,
        sq_stg_sap_dtl_y_rec.reins_not_fnd_err,
        sq_stg_sap_dtl_y_rec.reins_agmt_anchr_id,
        sq_stg_sap_dtl_y_rec.reins_agmt_knd_cd,
        sq_stg_sap_dtl_y_rec.org_id_err,
        sq_stg_sap_dtl_y_rec.reins_typ,
        sq_stg_sap_dtl_y_rec.za_grp_cd,
        sq_stg_sap_dtl_y_rec.dac_mismatch_err,
        sq_stg_sap_dtl_y_rec.reins_fein_nbr,
        sq_stg_sap_dtl_y_rec.reins_sub_agnt,
        sq_stg_sap_dtl_y_rec.par_rt,
        sq_stg_sap_dtl_y_rec.rcar_anchr_id,
        sq_stg_sap_dtl_y_rec.sub_agnt_amt,
        sq_stg_sap_dtl_y_rec.sub_agnt_not_fnd_err,
        sq_stg_sap_dtl_y_rec.afsi_err,
        sq_stg_sap_dtl_y_rec.trd_ptnr_rol_plyr_anchr_id,
        sq_stg_sap_dtl_y_rec.inv_trd_ptnr_err,
        sq_stg_sap_dtl_y_rec.zrptr_sapd_id,
        sq_stg_sap_dtl_y_rec.src_sys_cd,
        sq_stg_sap_dtl_y_rec.sys_prcg_cd,
        sq_stg_sap_dtl_y_rec.bil_sys_cd,
        sq_stg_sap_dtl_y_rec.exc_cd,
        sq_stg_sap_dtl_y_rec.acct_dervn_finc_acct_nbr,
        sq_stg_sap_dtl_y_rec.acct_dervn_knd_cd,
        sq_stg_sap_dtl_y_rec.onset_offset_ind,
        sq_stg_sap_dtl_y_rec.pl_loc_ori_cd,
        sq_stg_sap_dtl_y_rec.retro_ratg_cd,
        sq_stg_sap_dtl_y_rec.ntr_of_provsn_cd,
        sq_stg_sap_dtl_y_rec.drvd_acct_anchr_id,
        sq_stg_sap_dtl_y_rec.acct_dervn_src_ind,
        sq_stg_sap_dtl_y_rec.mon_acct_typ_id,
        sq_stg_sap_dtl_y_rec.finc_acct_typ_id,
        sq_stg_sap_dtl_y_rec.finc_adj_typ_id,
        sq_stg_sap_dtl_y_rec.pers_rol_plyr_anchr_id,
        sq_stg_sap_dtl_y_rec.sts_cd,
        sq_stg_sap_dtl_y_rec.bal_amt,
        sq_stg_sap_dtl_y_rec.distb_amt,
        sq_stg_sap_dtl_y_rec.bal_dt,
        sq_stg_sap_dtl_y_rec.actv_rec_ind,
        sq_stg_sap_dtl_y_rec.eff_to_tistmp,
        sq_stg_sap_dtl_y_rec.finc_ins_typ_cd,
        r_exp_set_audit_cols1.out_eff_fm_tistmp as eff_fm_tistmp,
        r_exp_set_audit_cols1.out_batch_id as bat_id,
        r_exp_set_audit_cols1.out_pop_info_id as pop_info_id,
        sq_stg_sap_dtl_y_rec.finc_perd_ccyy_mm,
        sq_stg_sap_dtl_y_rec.tran_sub_agnt_amt,
        sq_stg_sap_dtl_y_rec.contr_sub_agnt_amt,
        sq_stg_sap_dtl_y_rec.crcy_cd,
        sq_stg_sap_dtl_y_rec.tran_crcy_cd,
        sq_stg_sap_dtl_y_rec.contr_crcy_cd
    from sq_stg_sap_dtl_y_rec
-- Manually join with r_EXP_SET_AUDIT_COLS1
),
agg_mntry_acct as (
    select
        drvd_acct_anchr_id,
        finc_acct_typ_id,
        ntr_of_provsn_cd,
        finc_acct_nbr,
        bal_amt,
        distb_amt,
        bal_dt,
        sts_cd,
        actv_rec_ind,
        eff_fm_tistmp,
        eff_to_tistmp,
        pop_info_id,
        bat_id
    from exp_set_val
    qualify
        row_number() over (
            partition by
                drvd_acct_anchr_id,
                finc_acct_typ_id,
                ntr_of_provsn_cd,
                finc_acct_nbr,
                bal_amt,
                distb_amt,
                bal_dt,
                sts_cd,
                actv_rec_ind,
                eff_fm_tistmp,
                eff_to_tistmp,
                pop_info_id,
                bat_id
            order by null
        )
        = 1
),
r_seq_mntry_acct_id as (
	CREATE SEQUENCE r_SEQ_MNTRY_ACCT_ID
	START = 0
	INCREMENT = 1;),exp_pass_thrugh as (
    select
        drvd_acct_anchr_id,
        finc_acct_typ_id,
        ntr_of_provsn_cd,
        finc_acct_nbr,
        bal_amt,
        distb_amt,
        bal_dt,
        sts_cd,
        actv_rec_ind,
        -- *INF*:
        -- :LKP.R_LKP_MNTRY_ACCT(DRVD_ACCT_ANCHR_ID,FINC_ACCT_TYP_ID,LTRIM(RTRIM(FINC_ACCT_NBR)))
        r_lkp_mntry_acct_drvd_acct_anchr_id_finc_acct_typ_id_ltrim_rtrim_finc_acct_nbr.acct_id
        as v_acct_id,
        -- *INF*: IIF(isnull(v_ACCT_ID),'Y','N')
        iff(v_acct_id is null, 'Y', 'N') as o_mntry_acct_ins_flg,
        eff_fm_tistmp,
        eff_to_tistmp,
        pop_info_id,
        bat_id,
        r_seq_mntry_acct_id.nextval as mntry_acct_id
    from agg_mntry_acct
    left join
        r_lkp_mntry_acct r_lkp_mntry_acct_drvd_acct_anchr_id_finc_acct_typ_id_ltrim_rtrim_finc_acct_nbr
        on r_lkp_mntry_acct_drvd_acct_anchr_id_finc_acct_typ_id_ltrim_rtrim_finc_acct_nbr.acct_id
        = drvd_acct_anchr_id
        and r_lkp_mntry_acct_drvd_acct_anchr_id_finc_acct_typ_id_ltrim_rtrim_finc_acct_nbr.extl_refr_cd
        = finc_acct_typ_id
        and r_lkp_mntry_acct_drvd_acct_anchr_id_finc_acct_typ_id_ltrim_rtrim_finc_acct_nbr.typ_id
        = ltrim(rtrim(finc_acct_nbr))

),
fil_mntry_acct_ins as (
    select
        mntry_acct_id,
        drvd_acct_anchr_id,
        finc_acct_typ_id,
        ntr_of_provsn_cd,
        finc_acct_nbr,
        bal_amt,
        distb_amt,
        bal_dt,
        sts_cd,
        actv_rec_ind,
        eff_fm_tistmp,
        eff_to_tistmp,
        pop_info_id,
        bat_id,
        o_mntry_acct_ins_flg
    from exp_pass_thrugh
    where o_mntry_acct_ins_flg = 'Y'
),
select
    mntry_acct_id as mntry_acct_id,
    drvd_acct_anchr_id as acct_id,
    finc_acct_typ_id as typ_id,
    ntr_of_provsn_cd as ntr_of_provsn_cd,
    finc_acct_nbr as extl_refr_cd,
    bal_amt as bal_amt,
    distb_amt as distb_amt,
    bal_dt as bal_dt,
    sts_cd as sts_cd,
    actv_rec_ind as actv_rec_ind,
    eff_fm_tistmp as eff_fm_tistmp,
    eff_to_tistmp as eff_to_tistmp,
    pop_info_id as pop_info_id,
    pop_info_id as updt_pop_info_id,
    bat_id as bat_id
from fil_mntry_acct_ins
