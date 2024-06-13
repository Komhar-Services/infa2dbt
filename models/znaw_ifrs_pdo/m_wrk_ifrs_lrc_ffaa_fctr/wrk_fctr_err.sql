{{ config(materialized="incremental") }}

with
    sq_wrk_ifrs_lrc_ffaa_fctr as (
        select
            {{ var("dbownerwrk") }}.dbaall.SEQ_IFRS_LRC_FFAA_FCTR_PK.nextval as ifrs_lrc_ffaa_fctr_pk,
            upper(
                md5(
                    rtrim(wilc.orgl_drvd_pol_yy)
                    || rtrim(wilc.cl_lkd_yld_curve_crcy_cd)
                    || rtrim('TRUE')
                )
            ) as fctr_bnry_id,
            wift.fctr_typ_id,
            wift.fctr_typ,
            null as orgl_port_cd,
            null as orgl_contr_set_cd,
            null as orgl_contr_typ_nm,
            wilc.orgl_drvd_pol_yy,
            wilc.cl_lkd_yld_curve_crcy_cd,
            'TRUE' as intr_accrtn_flg,
            null as locl_crcy_alt_busn_key,
            null as tran_crcy_alt_busn_key,
            wift.exec_seq,
            cast(
                (
                    CASE
                        WHEN iaf.interest_accretion_factor IS NOT NULL THEN
                            POWER((1 + CAST(iaf.numrtr_rsk_free_rt AS float8)), (CAST(iaf.yld_curve_age_numrtr AS float8) / 12))
                        WHEN wilc.orgl_drvd_pol_yy > max_iaf.yld_curve_eval_yr THEN
                            POWER((1 + CAST(max_iaf.numrtr_rsk_free_rt AS float8)), (CAST(max_iaf.yld_curve_age_numrtr AS float8) / 12))
                        ELSE
                            POWER((1 + CAST(min_iaf.numrtr_rsk_free_rt AS float8)), (CAST(min_iaf.yld_curve_age_numrtr AS float8) / 12))
                    END
                ) as numeric(15, 12)
            ) as fctr,
            wift.fctr_mltplr,
            wift.fctr_adjt,
            date('2999-12-31') as eff_to_tistmp,
            {{ var("pop_info_id") }} as pop_info_id,
            {{ var("pop_info_id") }} as updt_pop_info_id,
            {{ var("batch_id") }} as bat_id,
            current_timestamp as eff_fm_tistmp
        from
            (
                select distinct orgl_drvd_pol_yy, cl_lkd_yld_curve_crcy_cd
                from {{ source('dbaall','wrk_ifrs_lrc_ffaa_pol_lvl_calc') }}
                where exec_seq = 0
            ) wilc
        left join
            {{ source('dbaall','wrk_actu_lrc_cl_lkd_yc_intr_accrtn_fctr') }} iaf
            on wilc.orgl_drvd_pol_yy = iaf.yld_curve_eval_yr
            and wilc.cl_lkd_yld_curve_crcy_cd = iaf.crcy_cd
        left join
            (
                select *
                from {{ source('dbaall','wrk_actu_lrc_cl_lkd_yc_intr_accrtn_fctr') }}
                where
                    (yld_curve_eval_yr, crcy_cd) in (
                        select max(yld_curve_eval_yr), crcy_cd
                        from {{ source('dbaall','wrk_actu_lrc_cl_lkd_yc_intr_accrtn_fctr') }}
                        group by crcy_cd
                    )
            ) max_iaf
            on wilc.cl_lkd_yld_curve_crcy_cd = max_iaf.crcy_cd
        left join
            (
                select *
                from {{ source('dbaall','wrk_actu_lrc_cl_lkd_yc_intr_accrtn_fctr') }}
                where
                    (yld_curve_eval_yr, crcy_cd) in (
                        select min(yld_curve_eval_yr), crcy_cd
                        from {{ source('dbaall','wrk_actu_lrc_cl_lkd_yc_intr_accrtn_fctr') }}
                        group by crcy_cd
                    )
            ) min_iaf
            on wilc.cl_lkd_yld_curve_crcy_cd = min_iaf.crcy_cd
        inner join
            {{ source('dbaall','wrk_ifrs_fctr_typ') }} wift
            on wift.fctr_typ = 'LRC_POL_LVL_FINC_CMPNT_FCTR'
            and wift.ifrs_calc_typ = 'LRC_FFAA_POL_LVL'
            and wift.prcs_ind = 'Y'
    ),
    exp_set_val as (
        select
            ifrs_lrc_ffaa_fctr_pk,
            fctr_bnry_id,
            fctr_typ_id,
            fctr_typ,
            orgl_port_cd,
            orgl_contr_set_cd,
            orgl_contr_typ_nm,
            orgl_drvd_pol_yy,
            cl_lkd_yld_curve_crcy_cd,
            exec_seq,
            fctr,
            '{{ var("tbl_tech_nm") }}' as o_tbl_tech_nm,
            fctr_mltplr,
            fctr_adjt,
            eff_to_tistmp,
            pop_info_id,
            updt_pop_info_id,
            bat_id,
            current_timestamp as out_eff_fm_tistmp,
            intr_accrtn_flg,
            locl_crcy_alt_busn_key,
            tran_crcy_alt_busn_key,
            eff_fm_tistmp,
            ROW_NUMBER() OVER(order by null) rn
        from sq_wrk_ifrs_lrc_ffaa_fctr
    ),
    mapplet_cte as (
        select
            o_tbl_tech_nm as tbl_tech_nm,
            fctr_typ,
            fctr,
            null as fctr_typ1,
            null as fctr1,
            null as fctr_typ2,
            null as fctr2,
            null as fctr_typ3,
            null as fctr3,
            null as fctr_typ4,
            null as fctr4,
            null as fctr_typ5,
            null as fctr5,
            null as fctr_typ6,
            null as fctr6,
            null as fctr_typ7,
            null as fctr7,
            null as fctr_typ8,
            null as fctr8,
            null as fctr_typ9,
            null as fctr9,
            null as fctr_typ10,
            null as fctr10,
            null as fctr_typ11,
            null as fctr11,
            null as fctr_typ12,
            null as fctr12,
            null as fctr_typ13,
            null as fctr13,
            null as fctr_typ14,
            null as fctr14,
            null as fctr_typ15,
            null as fctr15,
            rn
        from exp_set_val
    ),
    mplt_fctr_tlrnc_chk as (
        
        {{ mplt_fctr_tlrnc_chk() }}
    ),
    filtar_tolerance as (
        select
            mplt_fctr_tlrnc_chk.subj_area,
            mplt_fctr_tlrnc_chk.tbl_tech_nm1 as tbl_tech_nm,
            mplt_fctr_tlrnc_chk.attr_tech_nm,
            mplt_fctr_tlrnc_chk.orig_fctr,
            mplt_fctr_tlrnc_chk.ovrrdn_fctr,
            mplt_fctr_tlrnc_chk.ovrrdn_flg,
            mplt_fctr_tlrnc_chk.err_desc,
            exp_set_val.eff_to_tistmp as eff_to_tistmp2,
            exp_set_val.orgl_port_cd,
            exp_set_val.eff_fm_tistmp
        from exp_set_val
        inner join mplt_fctr_tlrnc_chk using(rn)
        -- Manually join with mplt_FCTR_TLRNC_CHK
        where ovrrdn_flg != 'Y'
    ), 
    exp_set_err_val as (
        select
            subj_area,
            tbl_tech_nm,
            attr_tech_nm,
            -1 as iter_id,
            to_timestamp(
                '{{ var("ml_finc_prd") }}' || '01', 'yyyymmdd'
            ) as finc_cl_perd_dt,
            orig_fctr,
            ovrrdn_fctr,
            err_desc,
            'POL_LVL_BNRY_ID' as pol_lvl_bnry_id,
            eff_to_tistmp2 as eff_to_tistmp,
            {{ var("pop_info_id") }} as pop_info_id,
            {{ var("pop_info_id") }} as updt_pop_info_id,
            {{ var("batch_id") }} as bat_id,
            current_timestamp as out_eff_fm_tistmp,
            orgl_port_cd,
            eff_fm_tistmp
        from filtar_tolerance
    )
select
    subj_area,
    tbl_tech_nm,
    attr_tech_nm,
    iter_id,
    finc_cl_perd_dt,
    orig_fctr as orgl_fctr_val,
    ovrrdn_fctr as ovrid_fctr_val,
    err_desc,
    orgl_port_cd as pol_lvl_bnry_id,
    eff_fm_tistmp,
    eff_to_tistmp,
    pop_info_id,
    updt_pop_info_id,
    bat_id
from exp_set_err_val

