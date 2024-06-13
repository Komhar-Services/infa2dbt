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
    )
    select
    exp_set_val.ifrs_lrc_ffaa_fctr_pk,
    exp_set_val.fctr_bnry_id,
    exp_set_val.fctr_typ_id,
    exp_set_val.fctr_typ,
    exp_set_val.orgl_port_cd,
    exp_set_val.orgl_contr_set_cd,
    exp_set_val.orgl_contr_typ_nm,
    exp_set_val.orgl_drvd_pol_yy,
    exp_set_val.cl_lkd_yld_curve_crcy_cd,
    exp_set_val.intr_accrtn_flg,
    exp_set_val.locl_crcy_alt_busn_key,
    exp_set_val.tran_crcy_alt_busn_key,
    exp_set_val.exec_seq,
    mplt_fctr_tlrnc_chk.ovrrdn_fctr as fctr,
    exp_set_val.fctr_mltplr,
    exp_set_val.fctr_adjt,
    exp_set_val.eff_fm_tistmp,
    exp_set_val.eff_to_tistmp,
    exp_set_val.pop_info_id,
    exp_set_val.updt_pop_info_id,
    exp_set_val.bat_id
from exp_set_val
inner join mplt_fctr_tlrnc_chk using(rn)