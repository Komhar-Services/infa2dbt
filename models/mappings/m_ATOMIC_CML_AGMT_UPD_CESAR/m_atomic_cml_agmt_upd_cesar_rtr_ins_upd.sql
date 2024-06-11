{{ config(materialized="table") }}

with
    ulkp_work_prcg_cyc_dt as (select * from {{ ref("ulkp_work_prcg_cyc_dt") }}),
    cml_agmt as (select * from {{ source("dbaall", "cml_agmt") }}),
    stg_pol_lvl_covg as (select * from {{ source("dbaall", "stg_pol_lvl_covg") }}),
    sq_stg_pol_lvl_covg_cml_agmt as (
        select
            cml_agmt_id,
            agmt_anchr_id,
            trim(pol_nbr),
            pol_eff_dt,
            typ_id,
            actv_rec_ind,
            inmy_agmt_anchr_id,
            acqn_cd,
            adt_freq_cd,
            bndl_cd,
            can_dt,
            can_rsn_cd,
            ciid_match_ind,
            cms_pty_id,
            cpan_ind,
            cust_nbr,
            dac_cd,
            divd_par_cd,
            exc_cd,
            instl_cd,
            trim(modu_nbr),
            mp_cd,
            plnd_end_dt,
            pol_contr_typ_cd,
            pol_sym_cd,
            port_ind,
            pri_nmd_insd_nm,
            prcg_ofc_cd,
            prdr_src_cd,
            ratg_ori_cd,
            reins_waqs_trty_cd,
            renl_ind,
            retro_ratg_cd,
            rsk_typ_cd,
            self_insd_retn_ind,
            sic_cd,
            src_pol_nbr,
            trim(src_pol_eff_dt),
            src_pol_sym_cd,
            sts_cd,
            undg_pgm_cd,
            v_inv_pool_ind,
            case
                when eff_fm_tistmp > '2021-06-10'
                then to_date('20210329', 'YYYYMMDD')
                else eff_fm_tistmp
            end as eff_fm_tistmp,
            eff_to_tistmp,
            bil_typ_cd,
            incp_dt,
            agmt_term_in_mnth,
            contr_type_cd,
            mkt_cd,
            perf_prot_ind,
            sale_dt,
            pymt_pln_cd,
            mstr_pol_nbr,
            tail_expi_dt,
            src_mstr_pol_nbr,
            src_pymt_pln_cd,
            src_ext_rpt_perd_dt,
            to_char(src_date, 'YYYYMMDD') as src_date,
            polhldr_entprs_id,
            polhldr_site_duns_nbr,
            parnt_child_contr_typ_cd,
            pri_nmd_insd_fein_nbr,
            ratg_bas_cd,
            src_mstr_pol_cd,
            wc_ratg_bas_cd,
            /* NEW FIELDS*/
            zi_pol_sys_contr_id,
            cede_ptnr_ctry_anchr_id,
            assm_ptnr_ctry_pol_id,
            assm_us_pol_nbr,
            frnt_cd,
            drvd_contr_crcy_cd,
            non_renl_rsn_cd,
            src_ips_contr_id,
            src_ptnr_ctry_abbr,
            src_ptnr_ctry_pol_id,
            src_ptnr_ctry_pol_nbr,
            src_frntg_cd,
            src_non_renl_rsn_cd,
            /* Added as part of Surety Enablement*/
            genl_ln_of_busn_cd,
            frgn_prdr_cd  -- As part of CR 5964 - CR 6309             
        from
            (
                select
                    ca.cml_agmt_id,
                    ca.agmt_anchr_id,
                    trim(ca.pol_nbr) as pol_nbr,
                    ca.pol_eff_dt,
                    ca.typ_id,
                    ca.actv_rec_ind,
                    ca.inmy_agmt_anchr_id,
                    ca.acqn_cd,
                    ca.adt_freq_cd,
                    ca.bndl_cd,
                    ca.can_dt,
                    ca.can_rsn_cd,
                    ca.ciid_match_ind,
                    ca.cms_pty_id,
                    ca.cpan_ind,
                    ca.cust_nbr,
                    ca.dac_cd,
                    ca.divd_par_cd,
                    ca.exc_cd,
                    ca.instl_cd,
                    ca.modu_nbr,
                    ca.mp_cd,
                    ca.plnd_end_dt,
                    ca.pol_contr_typ_cd,
                    ca.pol_sym_cd,
                    ca.port_ind,
                    ca.pri_nmd_insd_nm,
                    ca.prcg_ofc_cd,
                    ca.prdr_src_cd,
                    ca.ratg_ori_cd,
                    ca.reins_waqs_trty_cd,
                    ca.renl_ind,
                    ca.retro_ratg_cd,
                    ca.rsk_typ_cd,
                    ca.self_insd_retn_ind,
                    ca.sic_cd,
                    ca.src_pol_nbr,
                    ca.src_pol_eff_dt,
                    ca.src_pol_sym_cd,
                    ca.sts_cd,
                    ca.undg_pgm_cd,
                    ca.v_inv_pool_ind,
                    date(ca.eff_fm_tistmp) as eff_fm_tistmp,
                    date(ca.eff_to_tistmp) as eff_to_tistmp,
                    ca.bil_typ_cd,
                    ca.incp_dt,
                    ca.agmt_term_in_mnth,
                    ca.contr_type_cd,
                    ca.mkt_cd,
                    ca.perf_prot_ind,
                    ca.sale_dt,
                    trim(ca.pymt_pln_cd) as pymt_pln_cd,
                    trim(ca.mstr_pol_nbr) as mstr_pol_nbr,
                    ca.tail_expi_dt,
                    trim(src.mstr_pol_nbr) as src_mstr_pol_nbr,
                    trim(src.pymt_pln_cd) as src_pymt_pln_cd,
                    src.ext_rpt_perd_dt as src_ext_rpt_perd_dt,
                    src.ext_rpt_perd_dt as src_date,
                    trim(ca.polhldr_entprs_id) as polhldr_entprs_id,
                    trim(ca.polhldr_site_duns_nbr) as polhldr_site_duns_nbr,
                    trim(ca.parnt_child_contr_typ_cd) as parnt_child_contr_typ_cd,
                    trim(ca.pri_nmd_insd_fein_nbr) as pri_nmd_insd_fein_nbr,
                    trim(ca.ratg_bas_cd) as ratg_bas_cd,
                    trim(src.mstr_pol_cd) as src_mstr_pol_cd,
                    src.wc_ratg_bas_cd,
                    /* NEWFIELDS*/
                    trim(ca.zi_pol_sys_contr_id) as zi_pol_sys_contr_id,
                    trim(ca.cede_ptnr_ctry_anchr_id) as cede_ptnr_ctry_anchr_id,
                    trim(ca.assm_ptnr_ctry_pol_id) as assm_ptnr_ctry_pol_id,
                    trim(ca.assm_us_pol_nbr) as assm_us_pol_nbr,
                    trim(ca.frnt_cd) as frnt_cd,
                    trim(ca.drvd_contr_crcy_cd) as drvd_contr_crcy_cd,
                    trim(ca.non_renl_rsn_cd) as non_renl_rsn_cd,
                    trim(src.ips_contr_id) as src_ips_contr_id,
                    trim(src.ptnr_ctry_abbr) as src_ptnr_ctry_abbr,
                    trim(src.ptnr_ctry_pol_id) as src_ptnr_ctry_pol_id,
                    trim(src.ptnr_ctry_pol_nbr) as src_ptnr_ctry_pol_nbr,
                    trim(src.frntg_cd) as src_frntg_cd,
                    trim(src.non_renl_rsn_cd) as src_non_renl_rsn_cd,
                    /* Added as part of Surety Enablement*/
                    case
                        when
                            trim(ca.genl_ln_of_busn_cd) = ''
                            or ca.genl_ln_of_busn_cd is null
                        then 'null'
                        else genl_ln_of_busn_cd
                    end as genl_ln_of_busn_cd,
                    case
                        when trim(ca.frgn_prdr_cd) = '' or ca.frgn_prdr_cd is null
                        then 'null'
                        else frgn_prdr_cd
                    end as frgn_prdr_cd  -- Added as part of CR 5964,CR 6309
                from
                    cml_agmt ca,
                    (
                        select *
                        from
                            (
                                select
                                    pol_nbr,
                                    pol_eff_dt,
                                    mstr_pol_nbr,
                                    pymt_pln_cd,
                                    ext_rpt_perd_dt,
                                    modu_nbr,
                                    mstr_pol_cd,
                                    wc_ratg_bas_cd,
                                    ips_contr_id,
                                    ptnr_ctry_abbr,
                                    ptnr_ctry_pol_id,
                                    ptnr_ctry_pol_nbr,
                                    frntg_cd,
                                    non_renl_rsn_cd,
                                    row_number() over (
                                        partition by pol_nbr, pol_eff_dt, modu_nbr
                                        order by acty_seq_nbr desc
                                    ) as row_num
                                from stg_pol_lvl_covg
                            ) a
                        where row_num = 1
                    ) src
                where
                    trim(ca.pol_nbr) = trim(src.pol_nbr)
                    and ca.src_pol_eff_dt = to_char(src.pol_eff_dt, 'YYYYMMDD')
                    and ca.modu_nbr = src.modu_nbr
                    and ca.actv_rec_ind = 'Y'
            ) part
    ),
    lkp_curr_code as (select * from {{ ref("lkp_curr_code") }}),
    mapplet_cte as (
        select src_ptnr_ctry_abbr as in_ctry_cd, from sq_stg_pol_lvl_covg_cml_agmt
    ),
    shortcut_to_mplt_valdt_ctry_cd as (
        select * from {{ ref("shortcut_to_mplt_valdt_ctry_cd") }}
    ),
    lkp_geog_area as (select * from {{ ref("lkp_geog_area") }}),
    exp_prp_cml_agmt as (
        select
            sq_stg_pol_lvl_covg_cml_agmt.pol_sym_cd as in_pol_sym_cd,
            sq_stg_pol_lvl_covg_cml_agmt.src_mstr_pol_nbr as in_src_mstr_pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.src_pymt_pln_cd as in_src_pymt_pln_cd,
            sq_stg_pol_lvl_covg_cml_agmt.src_ext_rpt_perd_dt as in_src_ext_rpt_perd_dt,
            sq_stg_pol_lvl_covg_cml_agmt.src_wc_ratg_bas_cd,
            sq_stg_pol_lvl_covg_cml_agmt.src_mstr_pol_cd,
            sq_stg_pol_lvl_covg_cml_agmt.src_ips_contr_id,
            sq_stg_pol_lvl_covg_cml_agmt.src_ptnr_ctry_abbr,
            lkp_geog_area.plc_anchr_id as src_plc_anchr_id,
            sq_stg_pol_lvl_covg_cml_agmt.src_ptnr_ctry_pol_id,
            sq_stg_pol_lvl_covg_cml_agmt.src_ptnr_ctry_pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.src_frntg_cd,
            lkp_curr_code.trans_crcy_abbr as src_pol_lvl_crcy_cd,
            lkp_curr_code.local_crcy_abbr as lkp_local_crcy_abbr,
            sq_stg_pol_lvl_covg_cml_agmt.src_non_renl_rsn_cd,
            sq_stg_pol_lvl_covg_cml_agmt.pymt_pln_cd,
            sq_stg_pol_lvl_covg_cml_agmt.mstr_pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.tail_expi_dt,
            sq_stg_pol_lvl_covg_cml_agmt.eff_fm_tistmp,
            sq_stg_pol_lvl_covg_cml_agmt.parnt_child_contr_typ_cd,
            sq_stg_pol_lvl_covg_cml_agmt.pri_nmd_insd_fein_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.ratg_bas_cd,
            sq_stg_pol_lvl_covg_cml_agmt.assm_ptnr_ctry_pol_id,
            sq_stg_pol_lvl_covg_cml_agmt.assm_us_pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.cede_ptnr_ctry_anchr_id,
            sq_stg_pol_lvl_covg_cml_agmt.drvd_contr_crcy_cd,
            sq_stg_pol_lvl_covg_cml_agmt.frnt_cd,
            sq_stg_pol_lvl_covg_cml_agmt.non_renl_rsn_cd,
            sq_stg_pol_lvl_covg_cml_agmt.zi_pol_sys_contr_id,
            -- *INF*: IIF(ISNULL(in_SRC_MSTR_POL_NBR) OR
            -- LTRIM(RTRIM(in_SRC_MSTR_POL_NBR))='','null',in_SRC_MSTR_POL_NBR)
            iff(
                in_src_mstr_pol_nbr is null or ltrim(rtrim(in_src_mstr_pol_nbr)) = '',
                'null',
                in_src_mstr_pol_nbr
            ) as v_src_mstr_pol_nbr,
            -- *INF*: IIF(ISNULL(in_SRC_PYMT_PLN_CD) OR
            -- LTRIM(RTRIM(in_SRC_PYMT_PLN_CD))='','null',in_SRC_PYMT_PLN_CD)
            iff(
                in_src_pymt_pln_cd is null or ltrim(rtrim(in_src_pymt_pln_cd)) = '',
                'null',
                in_src_pymt_pln_cd
            ) as v_src_pymt_pln_cd,
            -- *INF*: IIF(ISNULL(in_SRC_EXT_RPT_PERD_DT),
            -- TO_DATE('2999-12-31',
            -- 'YYYY-MM-DD'),in_SRC_EXT_RPT_PERD_DT)
            iff(
                in_src_ext_rpt_perd_dt is null,
                to_timestamp('2999-12-31', 'YYYY-MM-DD'),
                in_src_ext_rpt_perd_dt
            ) as v_src_ext_rpt_perd_dt,
            -- *INF*: :LKP.ULKP_WORK_PRCG_CYC_DT('Y')
            ulkp_work_prcg_cyc_dt__y.pst_dt as v_wpcd_src_pst_dt,
            -- *INF*: IIF(SRC_MSTR_POL_CD='M' AND
            -- ltrim(rtrim(in_POL_SYM_CD))='WC','CMWC',IIF(SRC_MSTR_POL_CD='S','CSWC',IIF(SRC_MSTR_POL_CD='C','CERT',IIF(SRC_MSTR_POL_CD='M' AND ltrim(rtrim(in_POL_SYM_CD))<>'WC','MCMM','null'))))
            iff(
                src_mstr_pol_cd = 'M' and ltrim(rtrim(in_pol_sym_cd)) = 'WC',
                'CMWC',
                iff(
                    src_mstr_pol_cd = 'S',
                    'CSWC',
                    iff(
                        src_mstr_pol_cd = 'C',
                        'CERT',
                        iff(
                            src_mstr_pol_cd = 'M'
                            and ltrim(rtrim(in_pol_sym_cd)) <> 'WC',
                            'MCMM',
                            'null'
                        )
                    )
                )
            ) as v_mstr_pol_cd,
            -- *INF*: IIF(ISNULL(LTRIM(RTRIM(SRC_WC_RATG_BAS_CD))) OR
            -- LTRIM(RTRIM(SRC_WC_RATG_BAS_CD)) ='null' or
            -- length(LTRIM(RTRIM(SRC_WC_RATG_BAS_CD)) )=0 ,'null' ,
            -- LTRIM(RTRIM(SRC_WC_RATG_BAS_CD))  )
            iff(
                ltrim(rtrim(src_wc_ratg_bas_cd)) is null
                or ltrim(rtrim(src_wc_ratg_bas_cd)) = 'null'
                or length(ltrim(rtrim(src_wc_ratg_bas_cd))) = 0,
                'null',
                ltrim(rtrim(src_wc_ratg_bas_cd))
            ) as v_src_wc_ratg_bas_cd,
            -- *INF*: IIF((ISNULL(SRC_IPS_CONTR_ID) OR
            -- LTRIM(RTRIM(SRC_IPS_CONTR_ID))=''),'null',SRC_IPS_CONTR_ID)
            iff(
                (src_ips_contr_id is null or ltrim(rtrim(src_ips_contr_id)) = ''),
                'null',
                src_ips_contr_id
            ) as v_ips_contr_id,
            -- *INF*: IIF(ISNULL(SRC_PTNR_CTRY_POL_NBR) OR
            -- LTRIM(RTRIM(SRC_PTNR_CTRY_POL_NBR))='','null',SRC_PTNR_CTRY_POL_NBR)
            iff(
                src_ptnr_ctry_pol_nbr is null
                or ltrim(rtrim(src_ptnr_ctry_pol_nbr)) = '',
                'null',
                src_ptnr_ctry_pol_nbr
            ) as v_ptnr_ctry_pol_nbr,
            -- *INF*: IIF(ISNULL(SRC_PLC_ANCHR_ID) ,234151,SRC_PLC_ANCHR_ID)
            iff(src_plc_anchr_id is null, 234151, src_plc_anchr_id) as v_plc_anchr_id,
            -- *INF*: IIF(ISNULL(SRC_PTNR_CTRY_POL_ID) OR
            -- LTRIM(RTRIM(SRC_PTNR_CTRY_POL_ID))='','null',SRC_PTNR_CTRY_POL_ID)
            iff(
                src_ptnr_ctry_pol_id is null or ltrim(rtrim(src_ptnr_ctry_pol_id)) = '',
                'null',
                src_ptnr_ctry_pol_id
            ) as v_ptnr_ctry_pol_id,
            -- *INF*: IIF(ISNULL(SRC_FRNTG_CD) OR
            -- LTRIM(RTRIM(SRC_FRNTG_CD))='','null',SRC_FRNTG_CD)
            iff(
                src_frntg_cd is null or ltrim(rtrim(src_frntg_cd)) = '',
                'null',
                src_frntg_cd
            ) as v_frntg_cd,
            -- *INF*: DECODE(TRUE,
            -- NOT ISNULL(SRC_POL_LVL_CRCY_CD) AND
            -- LENGTH(LTRIM(RTRIM(SRC_POL_LVL_CRCY_CD)))>0,SRC_POL_LVL_CRCY_CD,
            -- NOT ISNULL(LKP_LOCAL_CRCY_ABBR) AND
            -- LENGTH(LTRIM(RTRIM(LKP_LOCAL_CRCY_ABBR)))>0
            -- ,LKP_LOCAL_CRCY_ABBR,DRVD_CONTR_CRCY_CD)
            decode(
                true,
                src_pol_lvl_crcy_cd is null
                and length(ltrim(rtrim(src_pol_lvl_crcy_cd))) not > 0,
                src_pol_lvl_crcy_cd,
                lkp_local_crcy_abbr is null
                and length(ltrim(rtrim(lkp_local_crcy_abbr))) not > 0,
                lkp_local_crcy_abbr,
                drvd_contr_crcy_cd
            ) as v_pol_lvl_crcy_cd_drvd_contr,
            -- *INF*: IIF((NOT ISNULL(v_POL_LVL_CRCY_CD_DRVD_CONTR) OR
            -- LENGTH(v_POL_LVL_CRCY_CD_DRVD_CONTR)>0),
            -- LTRIM(RTRIM(v_POL_LVL_CRCY_CD_DRVD_CONTR)),'USD')
            iff(
                (
                    v_pol_lvl_crcy_cd_drvd_contr is null
                    or length(v_pol_lvl_crcy_cd_drvd_contr) not > 0
                ),
                ltrim(rtrim(v_pol_lvl_crcy_cd_drvd_contr)),
                'USD'
            ) as v_pol_lvl_crcy_cd,
            -- *INF*: IIF(ISNULL(SRC_NON_RENL_RSN_CD) OR
            -- LTRIM(RTRIM(SRC_NON_RENL_RSN_CD))='','null',SRC_NON_RENL_RSN_CD)
            iff(
                src_non_renl_rsn_cd is null or ltrim(rtrim(src_non_renl_rsn_cd)) = '',
                'null',
                src_non_renl_rsn_cd
            ) as v_non_renl_rsn_cd,
            -- *INF*: IIF(
            -- (
            -- (LTRIM(RTRIM(PYMT_PLN_CD))=LTRIM(RTRIM(v_SRC_PYMT_PLN_CD))) AND 
            -- (LTRIM(RTRIM(MSTR_POL_NBR))=LTRIM(RTRIM(v_SRC_MSTR_POL_NBR))) AND 
            -- (TAIL_EXPI_DT=v_SRC_EXT_RPT_PERD_DT) AND 
            -- (LTRIM(RTRIM(PARNT_CHILD_CONTR_TYP_CD))=LTRIM(RTRIM(v_MSTR_POL_CD))) AND 
            -- (LTRIM(RTRIM(RATG_BAS_CD))=LTRIM(RTRIM(v_SRC_WC_RATG_BAS_CD))) AND 
            -- (LTRIM(RTRIM(ZI_POL_SYS_CONTR_ID))=LTRIM(RTRIM(v_IPS_CONTR_ID))) AND 
            -- (CEDE_PTNR_CTRY_ANCHR_ID=v_PLC_ANCHR_ID) AND 
            -- (LTRIM(RTRIM(ASSM_PTNR_CTRY_POL_ID))=LTRIM(RTRIM(v_PTNR_CTRY_POL_ID)))
            -- AND
            -- (LTRIM(RTRIM(ASSM_US_POL_NBR))=LTRIM(RTRIM(v_PTNR_CTRY_POL_NBR))) AND 
            -- (LTRIM(RTRIM(FRNT_CD))=LTRIM(RTRIM(v_FRNTG_CD))) AND 
            -- (LTRIM(RTRIM(DRVD_CONTR_CRCY_CD))=LTRIM(RTRIM(v_POL_LVL_CRCY_CD))) AND
            -- (LTRIM(RTRIM(NON_RENL_RSN_CD))=LTRIM(RTRIM(v_NON_RENL_RSN_CD)))
            -- ),'N','U')
            iff(
                (
                    (ltrim(rtrim(pymt_pln_cd)) = ltrim(rtrim(v_src_pymt_pln_cd)))
                    and (ltrim(rtrim(mstr_pol_nbr)) = ltrim(rtrim(v_src_mstr_pol_nbr)))
                    and (tail_expi_dt = v_src_ext_rpt_perd_dt)
                    and (
                        ltrim(rtrim(parnt_child_contr_typ_cd))
                        = ltrim(rtrim(v_mstr_pol_cd))
                    )
                    and (ltrim(rtrim(ratg_bas_cd)) = ltrim(rtrim(v_src_wc_ratg_bas_cd)))
                    and (
                        ltrim(rtrim(zi_pol_sys_contr_id)) = ltrim(rtrim(v_ips_contr_id))
                    )
                    and (cede_ptnr_ctry_anchr_id = v_plc_anchr_id)
                    and (
                        ltrim(rtrim(assm_ptnr_ctry_pol_id))
                        = ltrim(rtrim(v_ptnr_ctry_pol_id))
                    )
                    and (
                        ltrim(rtrim(assm_us_pol_nbr))
                        = ltrim(rtrim(v_ptnr_ctry_pol_nbr))
                    )
                    and (ltrim(rtrim(frnt_cd)) = ltrim(rtrim(v_frntg_cd)))
                    and (
                        ltrim(rtrim(drvd_contr_crcy_cd))
                        = ltrim(rtrim(v_pol_lvl_crcy_cd))
                    )
                    and (
                        ltrim(rtrim(non_renl_rsn_cd)) = ltrim(rtrim(v_non_renl_rsn_cd))
                    )
                ),
                'N',
                'U'
            ) as v_cml_chng_flg,
            v_cml_chng_flg as out_cml_chng_flg,
            -- *INF*: IIF(TRUNC(EFF_FM_TISTMP) = TRUNC(v_WPCD_SRC_PST_DT)
            -- ,IIF(v_CML_CHNG_FLG <> 'N'
            -- ,'U','D')
            -- ,'I')
            iff(
                trunc(eff_fm_tistmp) = trunc(v_wpcd_src_pst_dt),
                iff(v_cml_chng_flg <> 'N', 'U', 'D'),
                'I'
            ) as out_expins_upd_flag,
            v_src_mstr_pol_nbr as out_src_mstr_pol_nbr,
            v_src_pymt_pln_cd as out_src_pymt_pln_cd,
            v_src_ext_rpt_perd_dt as out_src_ext_rpt_perd_dt,
            v_mstr_pol_cd as out_mstr_pol_cd,
            v_src_wc_ratg_bas_cd as out_src_wc_ratg_bas_cd,
            v_ips_contr_id as out_ips_contr_id,
            v_plc_anchr_id as out_plc_anchr_id,
            v_ptnr_ctry_pol_id as out_ptnr_ctry_pol_id,
            v_ptnr_ctry_pol_nbr as out_ptnr_ctry_pol_nbr,
            v_frntg_cd as out_frntg_cd,
            v_pol_lvl_crcy_cd as out_pol_lvl_crcy_cd,
            v_non_renl_rsn_cd as out_non_renl_rsn_cd
        from sq_stg_pol_lvl_covg_cml_agmt
        left join
            lkp_curr_code
            on lkp_curr_code.pol_nbr = sq_stg_pol_lvl_covg_cml_agmt.pol_nbr
            and lkp_curr_code.pol_eff_dt = sq_stg_pol_lvl_covg_cml_agmt.src_pol_eff_dt
            and lkp_curr_code.modu_nbr = sq_stg_pol_lvl_covg_cml_agmt.modu_nbr
        left join
            lkp_geog_area
            on lkp_geog_area.abbr = shortcut_to_mplt_valdt_ctry_cd.out_ctry_cd
        left join
            ulkp_work_prcg_cyc_dt ulkp_work_prcg_cyc_dt__y
            on ulkp_work_prcg_cyc_dt__y.actv_rec_ind = 'Y'

    ),
    r_exp_set_audit_cols as (
        select
            src_date as in_dummy,
            -- *INF*: TO_DATE('2999-12-31-00.00.00.000000', 'YYYY-MM-DD HH24:MI:SS NS')
            to_timestamp(
                '2999-12-31-00.00.00.000000', 'YYYY-MM-DD HH24:MI:SS NS'
            ) as out_eff_to_tistmp,
            current_timestamp as out_eff_fm_tistmp,
            {{ var("batch_id") }} as out_batch_id,
            {{ var("pop_info_id") }} as out_pop_info_id
        from sq_stg_pol_lvl_covg_cml_agmt
    ),
    rtr_ins_upd as (
        select
            sq_stg_pol_lvl_covg_cml_agmt.cml_agmt_id,
            sq_stg_pol_lvl_covg_cml_agmt.agmt_anchr_id,
            sq_stg_pol_lvl_covg_cml_agmt.pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.pol_eff_dt,
            sq_stg_pol_lvl_covg_cml_agmt.typ_id,
            sq_stg_pol_lvl_covg_cml_agmt.actv_rec_ind,
            sq_stg_pol_lvl_covg_cml_agmt.inmy_agmt_anchr_id,
            sq_stg_pol_lvl_covg_cml_agmt.acqn_cd,
            sq_stg_pol_lvl_covg_cml_agmt.adt_freq_cd,
            sq_stg_pol_lvl_covg_cml_agmt.bndl_cd,
            sq_stg_pol_lvl_covg_cml_agmt.can_dt,
            sq_stg_pol_lvl_covg_cml_agmt.can_rsn_cd,
            sq_stg_pol_lvl_covg_cml_agmt.ciid_match_ind,
            sq_stg_pol_lvl_covg_cml_agmt.cms_pty_id,
            sq_stg_pol_lvl_covg_cml_agmt.cpan_ind,
            sq_stg_pol_lvl_covg_cml_agmt.cust_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.dac_cd,
            sq_stg_pol_lvl_covg_cml_agmt.divd_par_cd,
            sq_stg_pol_lvl_covg_cml_agmt.exc_cd,
            sq_stg_pol_lvl_covg_cml_agmt.instl_cd,
            sq_stg_pol_lvl_covg_cml_agmt.modu_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.mp_cd,
            sq_stg_pol_lvl_covg_cml_agmt.plnd_end_dt,
            sq_stg_pol_lvl_covg_cml_agmt.pol_contr_typ_cd,
            sq_stg_pol_lvl_covg_cml_agmt.pol_sym_cd,
            sq_stg_pol_lvl_covg_cml_agmt.port_ind,
            sq_stg_pol_lvl_covg_cml_agmt.pri_nmd_insd_nm,
            sq_stg_pol_lvl_covg_cml_agmt.prcg_ofc_cd,
            sq_stg_pol_lvl_covg_cml_agmt.prdr_src_cd,
            sq_stg_pol_lvl_covg_cml_agmt.ratg_ori_cd,
            sq_stg_pol_lvl_covg_cml_agmt.reins_waqs_trty_cd,
            sq_stg_pol_lvl_covg_cml_agmt.renl_ind,
            sq_stg_pol_lvl_covg_cml_agmt.retro_ratg_cd,
            sq_stg_pol_lvl_covg_cml_agmt.rsk_typ_cd,
            sq_stg_pol_lvl_covg_cml_agmt.self_insd_retn_ind,
            sq_stg_pol_lvl_covg_cml_agmt.sic_cd,
            sq_stg_pol_lvl_covg_cml_agmt.src_pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.src_pol_eff_dt,
            sq_stg_pol_lvl_covg_cml_agmt.src_pol_sym_cd,
            sq_stg_pol_lvl_covg_cml_agmt.sts_cd,
            sq_stg_pol_lvl_covg_cml_agmt.undg_pgm_cd,
            sq_stg_pol_lvl_covg_cml_agmt.v_inv_pool_ind,
            sq_stg_pol_lvl_covg_cml_agmt.eff_fm_tistmp,
            sq_stg_pol_lvl_covg_cml_agmt.eff_to_tistmp,
            sq_stg_pol_lvl_covg_cml_agmt.bil_typ_cd,
            sq_stg_pol_lvl_covg_cml_agmt.incp_dt,
            sq_stg_pol_lvl_covg_cml_agmt.agmt_term_in_mnth,
            sq_stg_pol_lvl_covg_cml_agmt.contr_type_cd,
            sq_stg_pol_lvl_covg_cml_agmt.mkt_cd,
            sq_stg_pol_lvl_covg_cml_agmt.perf_prot_ind,
            sq_stg_pol_lvl_covg_cml_agmt.sale_dt,
            sq_stg_pol_lvl_covg_cml_agmt.pymt_pln_cd,
            sq_stg_pol_lvl_covg_cml_agmt.mstr_pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.tail_expi_dt,
            exp_prp_cml_agmt.out_src_mstr_pol_nbr as src_mstr_pol_nbr,
            exp_prp_cml_agmt.out_src_pymt_pln_cd as src_pymt_pln_cd,
            exp_prp_cml_agmt.out_src_ext_rpt_perd_dt as src_ext_rpt_perd_dt,
            r_exp_set_audit_cols.out_eff_to_tistmp,
            r_exp_set_audit_cols.out_eff_fm_tistmp,
            r_exp_set_audit_cols.out_batch_id,
            r_exp_set_audit_cols.out_pop_info_id,
            exp_prp_cml_agmt.out_cml_chng_flg,
            exp_prp_cml_agmt.out_expins_upd_flag,
            sq_stg_pol_lvl_covg_cml_agmt.polhldr_entprs_id,
            sq_stg_pol_lvl_covg_cml_agmt.polhldr_site_duns_nbr,
            exp_prp_cml_agmt.out_mstr_pol_cd,
            exp_prp_cml_agmt.out_src_wc_ratg_bas_cd,
            exp_prp_cml_agmt.pri_nmd_insd_fein_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.src_ips_contr_id as assm_ptnr_ctry_pol_id,
            sq_stg_pol_lvl_covg_cml_agmt.src_ptnr_ctry_abbr as assm_us_pol_nbr,
            sq_stg_pol_lvl_covg_cml_agmt.src_ptnr_ctry_pol_id
            as cede_ptnr_ctry_anchr_id,
            sq_stg_pol_lvl_covg_cml_agmt.src_ptnr_ctry_pol_nbr as drvd_contr_crcy_cd,
            sq_stg_pol_lvl_covg_cml_agmt.src_frntg_cd as frnt_cd,
            sq_stg_pol_lvl_covg_cml_agmt.src_non_renl_rsn_cd as non_renl_rsn_cd,
            sq_stg_pol_lvl_covg_cml_agmt.zi_pol_sys_contr_id,
            exp_prp_cml_agmt.out_ips_contr_id,
            exp_prp_cml_agmt.out_plc_anchr_id,
            exp_prp_cml_agmt.out_ptnr_ctry_pol_id,
            exp_prp_cml_agmt.out_ptnr_ctry_pol_nbr,
            exp_prp_cml_agmt.out_frntg_cd,
            exp_prp_cml_agmt.out_pol_lvl_crcy_cd,
            exp_prp_cml_agmt.out_non_renl_rsn_cd,
            sq_stg_pol_lvl_covg_cml_agmt.genl_ln_of_busn_cd,
            sq_stg_pol_lvl_covg_cml_agmt.frgn_prdr_cd
        from exp_prp_cml_agmt
    -- Manually join with SQ_STG_POL_LVL_COVG_CML_AGMT
    -- Manually join with r_EXP_SET_AUDIT_COLS
    )
select *
from rtr_ins_upd
