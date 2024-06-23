{{
	config(
		materialized = "incremental"
	)
}}

with
    sq_wrk_dim_afd_attr_dervn_inmt as (
        with
            c_input as (
                select
                    dafd.actu_and_finc_dervn_alt_busn_key
                    as actu_and_finc_dervn_alt_busn_key,
                    dafd.ins_pol_and_rlup_alt_busn_key as ins_pol_and_rlup_alt_busn_key,
                    rtrim(
                        case
                            when wafp.org_gp_cd = 'UNK' then 'null' else wafp.org_gp_cd
                        end
                    ) as ins_typ_inp_unq_bnry_id_1,
                    rtrim(
                        substr(
                            case
                                when fdcl.finc_za_covg_cd = 'UNK'
                                then 'null'
                                else fdcl.finc_za_covg_cd
                            end,
                            1,
                            2
                        )
                    ) as finc_org_unq_bnry_id,
                    rtrim(
                        substr(
                            case
                                when fdcl.finc_za_covg_cd = 'UNK'
                                then 'null'
                                else fdcl.finc_za_covg_cd
                            end,
                            3,
                            2
                        )
                    ) as finc_lob_unq_bnry_id,
                    rtrim(
                        substr(
                            case
                                when fdcl.finc_za_covg_cd = 'UNK'
                                then 'null'
                                else fdcl.finc_za_covg_cd
                            end,
                            5,
                            2
                        )
                    ) as ibnr_sfam_unq_bnry_id,
                    rtrim(
                        case
                            when wafp.cur_prdr_id = 'UNK'
                            then 'null'
                            else wafp.cur_prdr_id
                        end
                    ) as acqn_cd,
                    rtrim(
                        case
                            when wafp.bndl_cd in ('B', 'U') then wafp.bndl_cd else ''
                        end
                    ) as bndl_cd,
                    rtrim(
                        case
                            when ddac.dir_assm_cede_cd = 'UNK'
                            then 'null'
                            else ddac.dir_assm_cede_cd
                        end
                    ) as captv_ind,
                    rtrim(
                        case
                            when daccr.ult_finc_reins_gp_cd = 'UNK'
                            then 'null'
                            else daccr.ult_finc_reins_gp_cd
                        end
                    ) as clr_hous_pol_catg_cd,
                    rtrim(dio.rvrs_flw_busn_ind) as cls_cd,
                    rtrim(
                        case when di.sap_co_cd = 'UNK' then 'null' else di.sap_co_cd end
                    ) as cmtn_agmt_nbr,
                    rtrim(
                        case
                            when wafp.cnvt_pol_nbr = 'UNK'
                            then 'null'
                            else wafp.cnvt_pol_nbr
                        end
                    ) as cnvt_pol_nbr,
                    rtrim(
                        case
                            when wafp.cust_nbr = 'UNK' then 'null' else wafp.cust_nbr
                        end
                    ) as covg_ln,
                    rtrim(
                        case
                            when daccr.ult_ibnr_fam_reins_gp_cd = 'UNK'
                            then 'null'
                            else daccr.ult_ibnr_fam_reins_gp_cd
                        end
                    ) as cpan_ind,
                    rtrim(
                        case
                            when drr.retro_ratg_cd = 'UNK'
                            then 'null'
                            else drr.retro_ratg_cd
                        end
                    ) as crop_reins_yy,
                    rtrim(
                        case
                            when substr(dafd.finc_acc_expo_yy, 1, 4) <= '1991'
                            then '99'
                            else
                                case
                                    when dmp.mlt_prl_cd = 'UNK'
                                    then 'null'
                                    else dmp.mlt_prl_cd
                                end
                        end
                    ) as cur_prdr_id,
                    case
                        when wafp.captv_ind = 'Y' or daccr.captv_ind = 'Y'
                        then 'Y'
                        else 'N'
                    end as cust_nbr,
                    rtrim(
                        substr(
                            case when de.exc_cd = 'UNK' then 'null' else de.exc_cd end,
                            1,
                            4
                        )
                    ) as dir_assm_cede_cd,
                    rtrim(
                        case
                            when wafp.pol_sym_cd = 'UNK'
                            then 'null'
                            when wafp.pol_sym_cd = 'NULL'
                            then 'null'
                            else wafp.pol_sym_cd
                        end
                    ) as divd_par_cd,
                    rtrim(
                        case
                            when di.opert_co_cd = '58'
                            then 'null'
                            else
                                case
                                    when wafp.self_insd_retn_ind = 'UNK'
                                    then 'null'
                                    else wafp.self_insd_retn_ind
                                end
                        end
                    ) as drvd_finc_opert_co_cd,
                    rtrim(
                        case
                            when wafp.undg_pgm_cd = 'UNK'
                            then 'null'
                            else wafp.undg_pgm_cd
                        end
                    ) as drvd_opert_co_cd,
                    rtrim(
                        case when ds.st_nbr = 'UNK' then 'null' else ds.st_nbr end
                    ) as exc_cd,
                    rtrim(
                        case
                            when
                                (
                                    dafd.typ_ins_cd = 'F&I'
                                    and ddac.dir_assm_cede_cd = '4'
                                    and di.opert_co_cd = '44'
                                )
                            then '40'
                            when
                                (
                                    dafd.typ_ins_cd = 'F&I'
                                    and ddac.dir_assm_cede_cd = '4'
                                    and wafp.opert_co_cd = '44'
                                    and di.opert_co_cd = '40'
                                )
                            then '44'
                            when
                                (
                                    dafd.typ_ins_cd = 'F&I'
                                    and di.opert_co_cd in ('47', '48')
                                )
                            then '40'
                            when di.opert_co_cd in ('32', '37', '38')
                            then di.opert_co_cd
                            else fdi.opert_co_cd
                        end
                    ) as finc_acc_expo_cc_yy,
                    rtrim(
                        case
                            when
                                (
                                    dafd.typ_ins_cd = 'F&I'
                                    and ddac.dir_assm_cede_cd = '4'
                                    and di.opert_co_cd = '44'
                                )
                            then '40'
                            when
                                (
                                    dafd.typ_ins_cd = 'F&I'
                                    and ddac.dir_assm_cede_cd = '4'
                                    and wafp.opert_co_cd = '44'
                                    and di.opert_co_cd = '40'
                                )
                            then '44'
                            when
                                (
                                    dafd.typ_ins_cd = 'F&I'
                                    and di.opert_co_cd in ('47', '48')
                                )
                            then '40'
                            else di.opert_co_cd
                        end
                    ) as finc_covg_ln,
                    rtrim(
                        case
                            when wafp.max_ded_cd = 'UNK'
                            then 'null'
                            else wafp.max_ded_cd
                        end
                    ) as finc_maj_ln,
                    rtrim(dafd.crop_reins_yy) as finc_sub_ln,
                    substr(dafd.finc_acc_expo_yy, 1, 4) as finc_opert_co_cd,
                    rtrim(
                        case
                            when di.opert_co_cd = '58'
                            then 'null'
                            else
                                case
                                    when wafp.cpan_ind = 'UNK'
                                    then 'null'
                                    else wafp.cpan_ind
                                end
                        end
                    ) as finc_za_covg_cd,
                    rtrim(
                        case
                            when dchpc.clr_hous_pol_catg_cd = 'UNK'
                            then 'null'
                            else dchpc.clr_hous_pol_catg_cd
                        end
                    ) as intrm_finc_low_lvl_org_id,
                    rtrim(
                        case
                            when wafp.divd_par_cd = 'UNK'
                            then 'null'
                            else wafp.divd_par_cd
                        end
                    ) as low_lvl_org_id,
                    rtrim(
                        case
                            when dca.cmtn_agmt_nbr = 'UNK'
                            then 'null'
                            else substr(dca.cmtn_agmt_nbr, 1, 5)
                        end
                    ) as maj_ln,
                    rtrim(
                        case
                            when dafd.susp_ind = 'UNK' then 'null' else dafd.susp_ind
                        end
                    ) as max_ded_cd,
                    rtrim(
                        case
                            when wafp.unclct_ded_pol_ind = 'UNK'
                            then 'null'
                            else wafp.unclct_ded_pol_ind
                        end
                    ) as mlt_prl_cd,
                    rtrim(
                        case
                            when dio.org_lvl_2_id = 'UNK'
                            then 'null'
                            else dio.org_lvl_2_id
                        end
                    ) as mkt_bskt_grp_cd,
                    rtrim(
                        case when wafp.acqn_cd = 'UNK' then 'null' else wafp.acqn_cd end
                    ) as org_gp_cd,
                    rtrim(
                        case
                            when daccr.ult_reins_agmt_nbr = 'UNK'
                            then 'null'
                            else daccr.ult_reins_agmt_nbr
                        end
                    ) as org_lvl_2_id,
                    rtrim(
                        case
                            when fdcl.finc_za_covg_cd = 'UNK'
                            then 'null'
                            else fdcl.finc_za_covg_cd
                        end
                    ) as pol_sym_cd,
                    rtrim(
                        case
                            when dio.org_lvl_1_id = '214541'
                            then '214541'
                            when wafp.low_lvl_org_id = '481543'
                            then 'WAQS1'
                            when wafp.low_lvl_org_id = '716110'
                            then 'WAQS2'
                            when wafp.low_lvl_org_id = '419811'
                            then 'WAQSM1'
                            else dio.finc_org_id
                        end
                    ) as reins_waqs_trty_cd,
                    rtrim(
                        case
                            when wafp.src_pol_eff_dt = 'UNK'
                            then 'null'
                            else wafp.src_pol_eff_dt
                        end
                    ) as retro_ratg_cd,
                    rtrim(
                        substr(
                            case
                                when dcl.za_covg_cd = 'UNK'
                                then 'null'
                                else dcl.za_covg_cd
                            end,
                            1,
                            2
                        )
                    ) as rvrs_flw_busn_ind,
                    rtrim(
                        substr(
                            case
                                when dcl.za_covg_cd = 'UNK'
                                then 'null'
                                else dcl.za_covg_cd
                            end,
                            3,
                            2
                        )
                    ) as sap_co_cd,
                    rtrim(
                        substr(
                            case
                                when dcl.za_covg_cd = 'UNK'
                                then 'null'
                                else dcl.za_covg_cd
                            end,
                            5,
                            2
                        )
                    ) as self_insd_retn_ind,
                    rtrim(
                        case when dclc.cls_cd = 'UNK' then 'null' else dclc.cls_cd end
                    ) as sic_cd,
                    rtrim(
                        case
                            when wafp.mkt_bskt_grp_cd = 'UNK'
                            then 'null'
                            else wafp.mkt_bskt_grp_cd
                        end
                    ) as src_pol_eff_dt,
                    rtrim(wafp.sic_cd) as st_nbr,
                    rtrim(
                        case
                            when di.opert_co_cd = '58'
                            then 'N'
                            else
                                case
                                    when wafp.reins_waqs_trty_cd = 'UNK'
                                    then 'null'
                                    else wafp.reins_waqs_trty_cd
                                end
                        end
                    ) as sub_ln,
                    rtrim(wafp.low_lvl_org_id) as susp_ind,
                    rtrim(fdi.opert_co_cd) as ult_finc_reins_gp_cd
                from dim_actu_and_finc_dervn_mm as dafd  /* 7107964487 */
                inner join
                    dim_assm_cede_cmtn_reins_brdg_mm as daccr
                    on dafd.cede_reins_agmt_alt_busn_key
                    = daccr.cede_reins_agmt_alt_busn_key
                    and dafd.assm_reins_agmt_alt_busn_key
                    = daccr.assm_reins_agmt_alt_busn_key
                    and dafd.cmtn_agmt_alt_busn_key = daccr.cmtn_agmt_alt_busn_key
                    and daccr.late_actv_rec_ind = 'Y'
                    and dafd.late_actv_rec_ind = 'Y'
                inner join
                    work_actu_and_fin_pol as wafp
                    on dafd.ins_pol_and_rlup_alt_busn_key
                    = wafp.ins_pol_and_rlup_alt_busn_key
                    and wafp.late_actv_rec_ind = 'Y'
                inner join
                    dim_dir_assm_cede as ddac
                    on ddac.dir_assm_cede_alt_busn_key = dafd.dir_assm_cede_alt_busn_key
                    and ddac.late_actv_rec_ind = 'Y'
                inner join
                    dim_covg_lob_cls_late_actv as dclc
                    on dafd.covg_lob_cls_alt_busn_key = dclc.covg_lob_cls_alt_busn_key
                    and dclc.late_actv_rec_ind = 'Y'
                inner join
                    dim_covg_lob as dcl
                    on dcl.covg_lob_alt_busn_key = dclc.covg_lob_alt_busn_key
                    and dcl.late_actv_rec_ind = 'Y'
                inner join
                    dim_covg_lob as fdcl
                    on dafd.finc_covg_lob_alt_busn_key = fdcl.covg_lob_alt_busn_key
                    and fdcl.late_actv_rec_ind = 'Y'
                inner join
                    wrk_dim_int_org_actu_adjt as dio
                    on dio.low_lvl_org_id = wafp.low_lvl_org_id
                    and dio.late_actv_rec_ind = 'Y'
                inner join
                    dim_insr as di
                    on di.insr_alt_busn_key = dafd.insr_alt_busn_key
                    and di.late_actv_rec_ind = 'Y'
                inner join
                    dim_insr as fdi
                    on dafd.finc_insr_alt_busn_key = fdi.insr_alt_busn_key
                    and fdi.late_actv_rec_ind = 'Y'
                inner join
                    dim_mlt_prl as dmp
                    on dmp.mlt_prl_alt_busn_key = dafd.mlt_prl_alt_busn_key
                    and dmp.late_actv_rec_ind = 'Y'
                inner join
                    dim_exc_late_actv as de
                    on de.exc_alt_busn_key = dafd.exc_alt_busn_key
                    and de.late_actv_rec_ind = 'Y'
                inner join
                    dim_retro_ratg_late_actv as drr
                    on drr.retro_ratg_alt_busn_key = dafd.retro_ratg_alt_busn_key
                    and drr.late_actv_rec_ind = 'Y'
                inner join
                    dim_st as ds
                    on ds.st_alt_busn_key = dafd.st_alt_busn_key
                    and ds.late_actv_rec_ind = 'Y'
                inner join
                    dim_cmtn_agmt as dca
                    on dca.cmtn_agmt_alt_busn_key = dafd.cmtn_agmt_alt_busn_key
                    and dca.late_actv_rec_ind = 'Y'
                inner join
                    wrk_factls_ins_pol_and_rlup_actu_adjt as fipr
                    on dafd.ins_pol_and_rlup_alt_busn_key
                    = fipr.ins_pol_and_rlup_alt_busn_key
                    and fipr.late_actv_rec_ind = 'Y'
                inner join
                    dim_clr_hous_pol_catg_late_actv as dchpc
                    on fipr.clr_hous_pol_catg_alt_busn_key
                    = dchpc.clr_hous_pol_catg_alt_busn_key
                    and dchpc.late_actv_rec_ind = 'Y'
            )
        select
            c_input.actu_and_finc_dervn_alt_busn_key
            as actu_and_finc_dervn_alt_busn_key,
            c_input.ins_pol_and_rlup_alt_busn_key as ins_pol_and_rlup_alt_busn_key,
            upper(
                md5(
                    rtrim(c_input.org_gp_cd)
                    || rtrim(c_input.finc_maj_ln)
                    || rtrim(c_input.cur_prdr_id)
                    || rtrim(c_input.bndl_cd)
                    || rtrim(c_input.dir_assm_cede_cd)
                    || rtrim(c_input.ult_finc_reins_gp_cd)
                    || rtrim(c_input.rvrs_flw_busn_ind)
                    || rtrim(c_input.sap_co_cd)
                )
            ) as ins_typ_inp_unq_bnry_id_2,
            upper(
                md5(
                    rtrim(c_input.org_gp_cd)
                    || rtrim(c_input.finc_maj_ln)
                    || rtrim(c_input.finc_covg_ln)
                    || rtrim(c_input.cnvt_pol_nbr)
                    || rtrim(c_input.dir_assm_cede_cd)
                    || rtrim(c_input.drvd_finc_opert_co_cd)
                    || rtrim(c_input.finc_acc_expo_cc_yy)
                    || rtrim(c_input.org_lvl_2_id)
                    || rtrim(c_input.acqn_cd)
                    || rtrim(c_input.ult_reins_agmt_nbr)
                    || rtrim(c_input.finc_za_covg_cd)
                    || rtrim(c_input.intrm_finc_low_lvl_org_id)
                    || rtrim(c_input.maj_ln)
                    || rtrim(c_input.src_pol_eff_dt)
                )
            ) as finc_org_unq_bnry_id,
            upper(
                md5(
                    rtrim(c_input.org_gp_cd)
                    || rtrim(c_input.finc_maj_ln)
                    || rtrim(c_input.cur_prdr_id)
                    || rtrim(c_input.bndl_cd)
                    || rtrim(c_input.dir_assm_cede_cd)
                    || rtrim(c_input.finc_sub_ln)
                    || rtrim(c_input.finc_covg_ln)
                    || rtrim(c_input.retro_ratg_cd)
                    || rtrim(c_input.mlt_prl_cd)
                    || rtrim(c_input.captv_ind)
                    || rtrim(c_input.exc_cd)
                    || rtrim(c_input.pol_sym_cd)
                    || rtrim(c_input.drvd_opert_co_cd)
                    || rtrim(c_input.max_ded_cd)
                    || rtrim(c_input.ult_finc_reins_gp_cd)
                )
            ) as finc_lob_unq_bnry_id,
            upper(
                md5(
                    rtrim(c_input.org_gp_cd)
                    || rtrim(c_input.maj_ln)
                    || rtrim(c_input.bndl_cd)
                    || rtrim(c_input.captv_ind)
                    || rtrim(c_input.clr_hous_pol_catg_cd)
                    || rtrim(c_input.cmtn_agmt_nbr)
                    || rtrim(c_input.cnvt_pol_nbr)
                    || rtrim(c_input.covg_ln)
                    || rtrim(c_input.cpan_ind)
                    || rtrim(c_input.crop_reins_yy)
                    || rtrim(c_input.cur_prdr_id)
                    || rtrim(c_input.cust_nbr)
                    || rtrim(c_input.dir_assm_cede_cd)
                    || rtrim(c_input.divd_par_cd)
                    || rtrim(c_input.drvd_opert_co_cd)
                    || rtrim(c_input.exc_cd)
                    || rtrim(c_input.finc_acc_expo_cc_yy)
                    || rtrim(c_input.max_ded_cd)
                    || rtrim(c_input.mlt_prl_cd)
                    || rtrim(c_input.pol_sym_cd)
                    || rtrim(c_input.retro_ratg_cd)
                    || rtrim(c_input.self_insd_retn_ind)
                    || rtrim(c_input.src_pol_eff_dt)
                    || rtrim(c_input.st_nbr)
                    || rtrim(c_input.sub_ln)
                    || rtrim(c_input.susp_ind)
                    || rtrim(c_input.ult_finc_reins_gp_cd)
                    || rtrim(c_input.ult_ibnr_fam_reins_gp_cd)
                    || rtrim(c_input.unclct_ded_pol_ind)
                    || rtrim(c_input.undg_pgm_cd)
                )
            ) as ibnr_sfam_unq_bnry_id,
            c_input.acqn_cd as acqn_cd,
            c_input.bndl_cd as bndl_cd,
            c_input.captv_ind as captv_ind,
            c_input.clr_hous_pol_catg_cd as clr_hous_pol_catg_cd,
            c_input.cls_cd as cls_cd,
            c_input.cmtn_agmt_nbr as cmtn_agmt_nbr,
            c_input.cnvt_pol_nbr as cnvt_pol_nbr,
            c_input.covg_ln as covg_ln,
            c_input.cpan_ind as cpan_ind,
            c_input.crop_reins_yy as crop_reins_yy,
            c_input.cur_prdr_id as cur_prdr_id,
            c_input.cust_nbr as cust_nbr,
            c_input.dir_assm_cede_cd as dir_assm_cede_cd,
            c_input.divd_par_cd as divd_par_cd,
            c_input.drvd_finc_opert_co_cd as drvd_finc_opert_co_cd,
            c_input.drvd_opert_co_cd as drvd_opert_co_cd,
            c_input.exc_cd as exc_cd,
            c_input.finc_acc_expo_cc_yy as finc_acc_expo_cc_yy,
            c_input.finc_covg_ln as finc_covg_ln,
            c_input.finc_maj_ln as finc_maj_ln,
            c_input.finc_sub_ln as finc_sub_ln,
            c_input.finc_opert_co_cd as finc_opert_co_cd,
            c_input.finc_za_covg_cd as finc_za_covg_cd,
            c_input.intrm_finc_low_lvl_org_id as intrm_finc_low_lvl_org_id,
            c_input.low_lvl_org_id as low_lvl_org_id,
            c_input.maj_ln as maj_ln,
            c_input.max_ded_cd as max_ded_cd,
            c_input.mlt_prl_cd as mlt_prl_cd,
            c_input.mkt_bskt_grp_cd as mkt_bskt_grp_cd,
            c_input.org_gp_cd as org_gp_cd,
            c_input.org_lvl_2_id as org_lvl_2_id,
            c_input.pol_sym_cd as pol_sym_cd,
            c_input.reins_waqs_trty_cd as reins_waqs_trty_cd,
            c_input.retro_ratg_cd as retro_ratg_cd,
            c_input.rvrs_flw_busn_ind as rvrs_flw_busn_ind,
            c_input.sap_co_cd as sap_co_cd,
            c_input.self_insd_retn_ind as self_insd_retn_ind,
            c_input.sic_cd as sic_cd,
            c_input.src_pol_eff_dt as src_pol_eff_dt,
            c_input.st_nbr as st_nbr,
            c_input.sub_ln as sub_ln,
            c_input.susp_ind as susp_ind,
            c_input.ult_finc_reins_gp_cd as ult_finc_reins_gp_cd,
            c_input.ult_ibnr_fam_reins_gp_cd as ult_ibnr_fam_reins_gp_cd,
            c_input.ult_reins_agmt_nbr as ult_reins_agmt_nbr,
            c_input.unclct_ded_pol_ind as unclct_ded_pol_ind,
            c_input.undg_pgm_cd as undg_pgm_cd,
            now() as eff_fm_tistmp,
            '2999-12-31' as eff_to_tistmp,
            {{ var("pop_info_id") }} as pop_info_id,
            {{ var("pop_info_id") }} as updt_pop_info_id,
            {{ var("batch_id") }} as bat_id
        from c_input
    ),
    exp_pass_thru as (
        select
            actu_and_finc_dervn_alt_busn_key,
            ins_pol_and_rlup_alt_busn_key,
            ins_typ_inp_unq_bnry_id,
            finc_org_unq_bnry_id,
            finc_lob_unq_bnry_id,
            ibnr_sfam_unq_bnry_id,
            acqn_cd,
            bndl_cd,
            captv_ind,
            clr_hous_pol_catg_cd,
            cls_cd,
            cmtn_agmt_nbr,
            cnvt_pol_nbr,
            covg_ln,
            cpan_ind,
            crop_reins_yy,
            cur_prdr_id,
            cust_nbr,
            dir_assm_cede_cd,
            divd_par_cd,
            drvd_finc_opert_co_cd,
            drvd_opert_co_cd,
            exc_cd,
            finc_acc_expo_cc_yy,
            finc_covg_ln,
            finc_maj_ln,
            finc_sub_ln,
            finc_opert_co_cd,
            finc_za_covg_cd,
            intrm_finc_low_lvl_org_id,
            low_lvl_org_id,
            maj_ln,
            max_ded_cd,
            mlt_prl_cd,
            mkt_bskt_grp_cd,
            org_gp_cd,
            org_lvl_2_id,
            pol_sym_cd,
            reins_waqs_trty_cd,
            retro_ratg_cd,
            rvrs_flw_busn_ind,
            sap_co_cd,
            self_insd_retn_ind,
            sic_cd,
            src_pol_eff_dt,
            st_nbr,
            sub_ln,
            susp_ind,
            ult_finc_reins_gp_cd,
            ult_ibnr_fam_reins_gp_cd,
            ult_reins_agmt_nbr,
            unclct_ded_pol_ind,
            undg_pgm_cd,
            eff_fm_tistmp,
            eff_to_tistmp,
            pop_info_id,
            updt_pop_info_id,
            bat_id
        from sq_wrk_dim_afd_attr_dervn_inmt
    ),
select
    actu_and_finc_dervn_alt_busn_key,
    ins_pol_and_rlup_alt_busn_key,
    ins_typ_inp_unq_bnry_id,
    finc_org_unq_bnry_id,
    finc_lob_unq_bnry_id,
    ibnr_sfam_unq_bnry_id,
    acqn_cd,
    bndl_cd,
    captv_ind,
    clr_hous_pol_catg_cd,
    cls_cd,
    cmtn_agmt_nbr,
    cnvt_pol_nbr,
    covg_ln,
    cpan_ind,
    crop_reins_yy,
    cur_prdr_id,
    cust_nbr,
    dir_assm_cede_cd,
    divd_par_cd,
    drvd_finc_opert_co_cd,
    drvd_opert_co_cd,
    exc_cd,
    finc_acc_expo_cc_yy,
    finc_covg_ln,
    finc_maj_ln,
    finc_sub_ln,
    finc_opert_co_cd,
    finc_za_covg_cd,
    intrm_finc_low_lvl_org_id,
    low_lvl_org_id,
    maj_ln,
    max_ded_cd,
    mlt_prl_cd,
    mkt_bskt_grp_cd,
    org_gp_cd,
    org_lvl_2_id,
    pol_sym_cd,
    reins_waqs_trty_cd,
    retro_ratg_cd,
    rvrs_flw_busn_ind,
    sap_co_cd,
    self_insd_retn_ind,
    sic_cd,
    src_pol_eff_dt,
    st_nbr,
    sub_ln,
    susp_ind,
    ult_finc_reins_gp_cd,
    ult_ibnr_fam_reins_gp_cd,
    ult_reins_agmt_nbr,
    unclct_ded_pol_ind,
    undg_pgm_cd,
    eff_fm_tistmp,
    eff_to_tistmp,
    pop_info_id,
    updt_pop_info_id,
    bat_id
from exp_pass_thru
