{% macro mplt_fctr_tlrnc_chk() -%}
with
    lkp_wrk_fctr_tlrnc as (select * from {{ ref("lkp_wrk_fctr_tlrnc") }}),
    mplt_input as (
        select
            tbl_tech_nm,
            fctr_typ,
            fctr,
            fctr_typ1,
            fctr1,
            fctr_typ2,
            fctr2,
            fctr_typ3,
            fctr3,
            fctr_typ4,
            fctr4,
            fctr_typ5,
            fctr5,
            fctr_typ6,
            fctr6,
            fctr_typ7,
            fctr7,
            fctr_typ8,
            fctr8,
            fctr_typ9,
            fctr9,
            fctr_typ10,
            fctr10,
            fctr_typ11,
            fctr11,
            fctr_typ12,
            fctr12,
            fctr_typ13,
            fctr13,
            fctr_typ14,
            fctr14,
            fctr_typ15,
            fctr15,
            rn
                   from mapplet_cte  
    ),
    exp_tlrnc_chk as (
        select
            mplt_input.tbl_tech_nm,
            fctr_typ,
            fctr as orig_fctr,
            fctr_typ1,
            fctr1 as orig_fctr1,
            fctr_typ2,
            fctr2 as orig_fctr2,
            fctr_typ3,
            fctr3 as orig_fctr3,
            fctr_typ4,
            fctr4 as orig_fctr4,
            fctr_typ5,
            fctr5 as orig_fctr5,
            fctr_typ6,
            fctr6 as orig_fctr6,
            fctr_typ7,
            fctr7 as orig_fctr7,
            fctr_typ8,
            fctr8 as orig_fctr8,
            fctr_typ9,
            fctr9 as orig_fctr9,
            fctr_typ10,
            fctr10 as orig_fctr10,
            fctr_typ11,
            fctr11 as orig_fctr11,
            fctr_typ12,
            fctr12 as orig_fctr12,
            fctr_typ13,
            fctr13 as orig_fctr13,
            fctr_typ14,
            fctr14 as orig_fctr14,
            rn,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ_tbl_tech_nm.r_validation as o_return_lkp,
            -- *INF*: substr( o_return_lkp,1,instr(o_return_lkp , '~' ,1)-1)
            substr(
                o_return_lkp, 1, regexp_instr(o_return_lkp, '~', 1) - 1
            ) as o_subj_area,
            -- *INF*: substr(o_return_lkp, instr(o_return_lkp , '~' ,1)+1
            -- ,instr(o_return_lkp , '~' ,1,2) - instr(o_return_lkp , '~' ,1)-1 )
            substr(
                o_return_lkp,
                regexp_instr(o_return_lkp, '~', 1) + 1,
                regexp_instr(o_return_lkp, '~', 1, 2)
                - regexp_instr(o_return_lkp, '~', 1)
                - 1
            ) as o_tbl_tech_nm,
            -- *INF*: substr(o_return_lkp, instr(o_return_lkp , '~' ,1,2) +1  ,
            -- instr(o_return_lkp , '~' ,1,3)-  instr(o_return_lkp , '~' ,1,2) -1)
            substr(
                o_return_lkp,
                regexp_instr(o_return_lkp, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp, '~', 1, 3)
                - regexp_instr(o_return_lkp, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm,
            -- *INF*: substr(o_return_lkp, instr(o_return_lkp, '~' ,1,3) +1  ,
            -- instr(o_return_lkp , '~' ,1,4)-  instr(o_return_lkp , '~' ,1,3) -1)
            substr(
                o_return_lkp,
                regexp_instr(o_return_lkp, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp, '~', 1, 4)
                - regexp_instr(o_return_lkp, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce,
            -- *INF*: substr(o_return_lkp, instr(o_return_lkp , '~' ,1,4) +1  ,
            -- instr(o_return_lkp , '~' ,1,5)-  instr(o_return_lkp , '~' ,1,4) -1)
            substr(
                o_return_lkp,
                regexp_instr(o_return_lkp, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp, '~', 1, 5)
                - regexp_instr(o_return_lkp, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce,
            -- *INF*: substr(o_return_lkp, instr(o_return_lkp , '~' ,1,5) +1  ,
            -- instr(o_return_lkp , '~' ,1,6)-  instr(o_return_lkp , '~' ,1,5) -1)
            substr(
                o_return_lkp,
                regexp_instr(o_return_lkp, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp, '~', 1, 6)
                - regexp_instr(o_return_lkp, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt,
            -- *INF*: substr(o_return_lkp,instr(o_return_lkp , '~' ,-1,1) +1 )
            substr(
                o_return_lkp, regexp_instr(o_return_lkp, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt,
            -- *INF*: IIF(ORIG_FCTR < v_FCTR_LWR_TLRNCE, v_FCTR_LWR_DFLT
            -- , IIF(ORIG_FCTR > v_FCTR_UPR_TLRNCE, v_FCTR_UPR_DFLT, ORIG_FCTR))
            iff(
                orig_fctr < v_fctr_lwr_tlrnce,
                v_fctr_lwr_dflt,
                iff(orig_fctr > v_fctr_upr_tlrnce, v_fctr_upr_dflt, orig_fctr)
            ) as v_ovrrdn_fctr,
            v_ovrrdn_fctr as out_ovrrdn_fctr,
            -- *INF*: IIF(ORIG_FCTR < v_FCTR_LWR_TLRNCE, 'Y'
            -- , IIF(ORIG_FCTR > v_FCTR_UPR_TLRNCE, 'Y', 'N'))
            iff(
                orig_fctr < v_fctr_lwr_tlrnce,
                'Y',
                iff(orig_fctr > v_fctr_upr_tlrnce, 'Y', 'N')
            ) as out_ovrrdn_flg,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP1,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ1_tbl_tech_nm.r_validation as o_return_lkp1,
            -- *INF*: substr(o_return_lkp1, instr(o_return_lkp1 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp1 , '~' ,1,3)-  instr(o_return_lkp1 , '~' ,1,2) -1)
            substr(
                o_return_lkp1,
                regexp_instr(o_return_lkp1, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp1, '~', 1, 3)
                - regexp_instr(o_return_lkp1, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm1,
            -- *INF*: substr(o_return_lkp1, instr(o_return_lkp1, '~' ,1,3) +1  ,
            -- instr(o_return_lkp1 , '~' ,1,4)-  instr(o_return_lkp1 , '~' ,1,3) -1)
            substr(
                o_return_lkp1,
                regexp_instr(o_return_lkp1, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp1, '~', 1, 4)
                - regexp_instr(o_return_lkp1, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce1,
            -- *INF*: substr(o_return_lkp1, instr(o_return_lkp1 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp1 , '~' ,1,5)-  instr(o_return_lkp1 , '~' ,1,4) -1)
            substr(
                o_return_lkp1,
                regexp_instr(o_return_lkp1, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp1, '~', 1, 5)
                - regexp_instr(o_return_lkp1, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce1,
            -- *INF*: substr(o_return_lkp1, instr(o_return_lkp1 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp1 , '~' ,1,6)-  instr(o_return_lkp1 , '~' ,1,5) -1)
            substr(
                o_return_lkp1,
                regexp_instr(o_return_lkp1, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp1, '~', 1, 6)
                - regexp_instr(o_return_lkp1, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt1,
            -- *INF*: substr(o_return_lkp1,instr(o_return_lkp1 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp1, regexp_instr(o_return_lkp1, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt1,
            -- *INF*: IIF(ORIG_FCTR1 < v_FCTR_LWR_TLRNCE1, v_FCTR_LWR_DFLT1
            -- , IIF(ORIG_FCTR1 > v_FCTR_UPR_TLRNCE1, v_FCTR_UPR_DFLT1, ORIG_FCTR1))
            iff(
                orig_fctr1 < v_fctr_lwr_tlrnce1,
                v_fctr_lwr_dflt1,
                iff(orig_fctr1 > v_fctr_upr_tlrnce1, v_fctr_upr_dflt1, orig_fctr1)
            ) as v_ovrrdn_fctr1,
            v_ovrrdn_fctr1 as out_ovrrdn_fctr1,
            -- *INF*: IIF(ORIG_FCTR1 < v_FCTR_LWR_TLRNCE1, 'Y'
            -- , IIF(ORIG_FCTR1 > v_FCTR_UPR_TLRNCE1, 'Y', 'N'))
            iff(
                orig_fctr1 < v_fctr_lwr_tlrnce1,
                'Y',
                iff(orig_fctr1 > v_fctr_upr_tlrnce1, 'Y', 'N')
            ) as out_ovrrdn_flg1,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP2,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ2_tbl_tech_nm.r_validation as o_return_lkp2,
            -- *INF*: substr(o_return_lkp2, instr(o_return_lkp2 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp2 , '~' ,1,3)-  instr(o_return_lkp2 , '~' ,1,2) -1)
            substr(
                o_return_lkp2,
                regexp_instr(o_return_lkp2, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp2, '~', 1, 3)
                - regexp_instr(o_return_lkp2, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm2,
            -- *INF*: substr(o_return_lkp2, instr(o_return_lkp2, '~' ,1,3) +1  ,
            -- instr(o_return_lkp2 , '~' ,1,4)-  instr(o_return_lkp2 , '~' ,1,3) -1)
            substr(
                o_return_lkp2,
                regexp_instr(o_return_lkp2, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp2, '~', 1, 4)
                - regexp_instr(o_return_lkp2, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce2,
            -- *INF*: substr(o_return_lkp2, instr(o_return_lkp2 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp2 , '~' ,1,5)-  instr(o_return_lkp2 , '~' ,1,4) -1)
            substr(
                o_return_lkp2,
                regexp_instr(o_return_lkp2, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp2, '~', 1, 5)
                - regexp_instr(o_return_lkp2, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce2,
            -- *INF*: substr(o_return_lkp2, instr(o_return_lkp2 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp2 , '~' ,1,6)-  instr(o_return_lkp2 , '~' ,1,5) -1)
            substr(
                o_return_lkp2,
                regexp_instr(o_return_lkp2, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp2, '~', 1, 6)
                - regexp_instr(o_return_lkp2, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt2,
            -- *INF*: substr(o_return_lkp2,instr(o_return_lkp2 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp2, regexp_instr(o_return_lkp2, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt2,
            -- *INF*: IIF(ORIG_FCTR2 < v_FCTR_LWR_TLRNCE2, v_FCTR_LWR_DFLT2
            -- , IIF(ORIG_FCTR2 > v_FCTR_UPR_TLRNCE2, v_FCTR_UPR_DFLT2, ORIG_FCTR2))
            iff(
                orig_fctr2 < v_fctr_lwr_tlrnce2,
                v_fctr_lwr_dflt2,
                iff(orig_fctr2 > v_fctr_upr_tlrnce2, v_fctr_upr_dflt2, orig_fctr2)
            ) as v_ovrrdn_fctr2,
            v_ovrrdn_fctr2 as out_ovrrdn_fctr2,
            -- *INF*: IIF(ORIG_FCTR2 < v_FCTR_LWR_TLRNCE2, 'Y'
            -- , IIF(ORIG_FCTR2 > v_FCTR_UPR_TLRNCE2, 'Y', 'N'))
            iff(
                orig_fctr2 < v_fctr_lwr_tlrnce2,
                'Y',
                iff(orig_fctr2 > v_fctr_upr_tlrnce2, 'Y', 'N')
            ) as out_ovrrdn_flg2,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP3,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ3_tbl_tech_nm.r_validation as o_return_lkp3,
            -- *INF*: substr(o_return_lkp3, instr(o_return_lkp3 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp3 , '~' ,1,3)-  instr(o_return_lkp3 , '~' ,1,2) -1)
            substr(
                o_return_lkp3,
                regexp_instr(o_return_lkp3, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp3, '~', 1, 3)
                - regexp_instr(o_return_lkp3, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm3,
            -- *INF*: substr(o_return_lkp3, instr(o_return_lkp3, '~' ,1,3) +1  ,
            -- instr(o_return_lkp3 , '~' ,1,4)-  instr(o_return_lkp3 , '~' ,1,3) -1)
            substr(
                o_return_lkp3,
                regexp_instr(o_return_lkp3, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp3, '~', 1, 4)
                - regexp_instr(o_return_lkp3, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce3,
            -- *INF*: substr(o_return_lkp3, instr(o_return_lkp3 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp3 , '~' ,1,5)-  instr(o_return_lkp3 , '~' ,1,4) -1)
            substr(
                o_return_lkp3,
                regexp_instr(o_return_lkp3, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp3, '~', 1, 5)
                - regexp_instr(o_return_lkp3, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce3,
            -- *INF*: substr(o_return_lkp3, instr(o_return_lkp3 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp3 , '~' ,1,6)-  instr(o_return_lkp3 , '~' ,1,5) -1)
            substr(
                o_return_lkp3,
                regexp_instr(o_return_lkp3, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp3, '~', 1, 6)
                - regexp_instr(o_return_lkp3, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt3,
            -- *INF*: substr(o_return_lkp3,instr(o_return_lkp3 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp3, regexp_instr(o_return_lkp3, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt3,
            -- *INF*: IIF(ORIG_FCTR3 < v_FCTR_LWR_TLRNCE3, v_FCTR_LWR_DFLT3
            -- , IIF(ORIG_FCTR3 > v_FCTR_UPR_TLRNCE3, v_FCTR_UPR_DFLT3, ORIG_FCTR3))
            iff(
                orig_fctr3 < v_fctr_lwr_tlrnce3,
                v_fctr_lwr_dflt3,
                iff(orig_fctr3 > v_fctr_upr_tlrnce3, v_fctr_upr_dflt3, orig_fctr3)
            ) as v_ovrrdn_fctr3,
            v_ovrrdn_fctr3 as out_ovrrdn_fctr3,
            -- *INF*: IIF(ORIG_FCTR3 < v_FCTR_LWR_TLRNCE3, 'Y'
            -- , IIF(ORIG_FCTR3 > v_FCTR_UPR_TLRNCE3, 'Y', 'N'))
            iff(
                orig_fctr3 < v_fctr_lwr_tlrnce3,
                'Y',
                iff(orig_fctr3 > v_fctr_upr_tlrnce3, 'Y', 'N')
            ) as out_ovrrdn_flg3,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP4,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ4_tbl_tech_nm.r_validation as o_return_lkp4,
            -- *INF*: substr(o_return_lkp4, instr(o_return_lkp4 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp4 , '~' ,1,3)-  instr(o_return_lkp4 , '~' ,1,2) -1)
            substr(
                o_return_lkp4,
                regexp_instr(o_return_lkp4, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp4, '~', 1, 3)
                - regexp_instr(o_return_lkp4, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm4,
            -- *INF*: substr(o_return_lkp4, instr(o_return_lkp4, '~' ,1,3) +1  ,
            -- instr(o_return_lkp4 , '~' ,1,4)-  instr(o_return_lkp4 , '~' ,1,3) -1)
            substr(
                o_return_lkp4,
                regexp_instr(o_return_lkp4, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp4, '~', 1, 4)
                - regexp_instr(o_return_lkp4, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce4,
            -- *INF*: substr(o_return_lkp4, instr(o_return_lkp4 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp4 , '~' ,1,5)-  instr(o_return_lkp4 , '~' ,1,4) -1)
            substr(
                o_return_lkp4,
                regexp_instr(o_return_lkp4, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp4, '~', 1, 5)
                - regexp_instr(o_return_lkp4, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce4,
            -- *INF*: substr(o_return_lkp4, instr(o_return_lkp4 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp4 , '~' ,1,6)-  instr(o_return_lkp4 , '~' ,1,5) -1)
            substr(
                o_return_lkp4,
                regexp_instr(o_return_lkp4, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp4, '~', 1, 6)
                - regexp_instr(o_return_lkp4, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt4,
            -- *INF*: substr(o_return_lkp4,instr(o_return_lkp4 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp4, regexp_instr(o_return_lkp4, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt4,
            -- *INF*: IIF(ORIG_FCTR4 < v_FCTR_LWR_TLRNCE4, v_FCTR_LWR_DFLT4
            -- , IIF(ORIG_FCTR4 > v_FCTR_UPR_TLRNCE4, v_FCTR_UPR_DFLT4, ORIG_FCTR4))
            iff(
                orig_fctr4 < v_fctr_lwr_tlrnce4,
                v_fctr_lwr_dflt4,
                iff(orig_fctr4 > v_fctr_upr_tlrnce4, v_fctr_upr_dflt4, orig_fctr4)
            ) as v_ovrrdn_fctr4,
            v_ovrrdn_fctr4 as out_ovrrdn_fctr4,
            -- *INF*: IIF(ORIG_FCTR4 < v_FCTR_LWR_TLRNCE4, 'Y'
            -- , IIF(ORIG_FCTR4 > v_FCTR_UPR_TLRNCE4, 'Y', 'N'))
            iff(
                orig_fctr4 < v_fctr_lwr_tlrnce4,
                'Y',
                iff(orig_fctr4 > v_fctr_upr_tlrnce4, 'Y', 'N')
            ) as out_ovrrdn_flg4,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP5,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ5_tbl_tech_nm.r_validation as o_return_lkp5,
            -- *INF*: substr(o_return_lkp5, instr(o_return_lkp5 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp5 , '~' ,1,3)-  instr(o_return_lkp5 , '~' ,1,2) -1)
            substr(
                o_return_lkp5,
                regexp_instr(o_return_lkp5, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp5, '~', 1, 3)
                - regexp_instr(o_return_lkp5, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm5,
            -- *INF*: substr(o_return_lkp5, instr(o_return_lkp5, '~' ,1,3) +1  ,
            -- instr(o_return_lkp5 , '~' ,1,4)-  instr(o_return_lkp5 , '~' ,1,3) -1)
            substr(
                o_return_lkp5,
                regexp_instr(o_return_lkp5, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp5, '~', 1, 4)
                - regexp_instr(o_return_lkp5, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce5,
            -- *INF*: substr(o_return_lkp5, instr(o_return_lkp5 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp5 , '~' ,1,5)-  instr(o_return_lkp5 , '~' ,1,4) -1)
            substr(
                o_return_lkp5,
                regexp_instr(o_return_lkp5, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp5, '~', 1, 5)
                - regexp_instr(o_return_lkp5, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce5,
            -- *INF*: substr(o_return_lkp5, instr(o_return_lkp5 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp5 , '~' ,1,6)-  instr(o_return_lkp5 , '~' ,1,5) -1)
            substr(
                o_return_lkp5,
                regexp_instr(o_return_lkp5, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp5, '~', 1, 6)
                - regexp_instr(o_return_lkp5, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt5,
            -- *INF*: substr(o_return_lkp5,instr(o_return_lkp5 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp5, regexp_instr(o_return_lkp5, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt5,
            -- *INF*: IIF(ORIG_FCTR5 < v_FCTR_LWR_TLRNCE5, v_FCTR_LWR_DFLT5
            -- , IIF(ORIG_FCTR5 > v_FCTR_UPR_TLRNCE5, v_FCTR_UPR_DFLT5, ORIG_FCTR5))
            iff(
                orig_fctr5 < v_fctr_lwr_tlrnce5,
                v_fctr_lwr_dflt5,
                iff(orig_fctr5 > v_fctr_upr_tlrnce5, v_fctr_upr_dflt5, orig_fctr5)
            ) as v_ovrrdn_fctr5,
            v_ovrrdn_fctr5 as out_ovrrdn_fctr5,
            -- *INF*: IIF(ORIG_FCTR5 < v_FCTR_LWR_TLRNCE5, 'Y'
            -- , IIF(ORIG_FCTR5 > v_FCTR_UPR_TLRNCE5, 'Y', 'N'))
            iff(
                orig_fctr5 < v_fctr_lwr_tlrnce5,
                'Y',
                iff(orig_fctr5 > v_fctr_upr_tlrnce5, 'Y', 'N')
            ) as out_ovrrdn_flg5,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP6,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ6_tbl_tech_nm.r_validation as o_return_lkp6,
            -- *INF*: substr(o_return_lkp6, instr(o_return_lkp6 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp6 , '~' ,1,3)-  instr(o_return_lkp6 , '~' ,1,2) -1)
            substr(
                o_return_lkp6,
                regexp_instr(o_return_lkp6, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp6, '~', 1, 3)
                - regexp_instr(o_return_lkp6, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm6,
            -- *INF*: substr(o_return_lkp6, instr(o_return_lkp6, '~' ,1,3) +1  ,
            -- instr(o_return_lkp6 , '~' ,1,4)-  instr(o_return_lkp6 , '~' ,1,3) -1)
            substr(
                o_return_lkp6,
                regexp_instr(o_return_lkp6, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp6, '~', 1, 4)
                - regexp_instr(o_return_lkp6, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce6,
            -- *INF*: substr(o_return_lkp6, instr(o_return_lkp6 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp6 , '~' ,1,5)-  instr(o_return_lkp6 , '~' ,1,4) -1)
            substr(
                o_return_lkp6,
                regexp_instr(o_return_lkp6, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp6, '~', 1, 5)
                - regexp_instr(o_return_lkp6, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce6,
            -- *INF*: substr(o_return_lkp6, instr(o_return_lkp6 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp6 , '~' ,1,6)-  instr(o_return_lkp6 , '~' ,1,5) -1)
            substr(
                o_return_lkp6,
                regexp_instr(o_return_lkp6, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp6, '~', 1, 6)
                - regexp_instr(o_return_lkp6, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt6,
            -- *INF*: substr(o_return_lkp6,instr(o_return_lkp6 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp6, regexp_instr(o_return_lkp6, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt6,
            -- *INF*: IIF(ORIG_FCTR6 < v_FCTR_LWR_TLRNCE6, v_FCTR_LWR_DFLT6
            -- , IIF(ORIG_FCTR6 > v_FCTR_UPR_TLRNCE6, v_FCTR_UPR_DFLT6, ORIG_FCTR6))
            iff(
                orig_fctr6 < v_fctr_lwr_tlrnce6,
                v_fctr_lwr_dflt6,
                iff(orig_fctr6 > v_fctr_upr_tlrnce6, v_fctr_upr_dflt6, orig_fctr6)
            ) as v_ovrrdn_fctr6,
            v_ovrrdn_fctr6 as out_ovrrdn_fctr6,
            -- *INF*: IIF(ORIG_FCTR6 < v_FCTR_LWR_TLRNCE6, 'Y'
            -- , IIF(ORIG_FCTR6 > v_FCTR_UPR_TLRNCE6, 'Y', 'N'))
            iff(
                orig_fctr6 < v_fctr_lwr_tlrnce6,
                'Y',
                iff(orig_fctr6 > v_fctr_upr_tlrnce6, 'Y', 'N')
            ) as out_ovrrdn_flg6,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP7,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ7_tbl_tech_nm.r_validation as o_return_lkp7,
            -- *INF*: substr(o_return_lkp7, instr(o_return_lkp7 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp7 , '~' ,1,3)-  instr(o_return_lkp7 , '~' ,1,2) -1)
            substr(
                o_return_lkp7,
                regexp_instr(o_return_lkp7, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp7, '~', 1, 3)
                - regexp_instr(o_return_lkp7, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm7,
            -- *INF*: substr(o_return_lkp7, instr(o_return_lkp7, '~' ,1,3) +1  ,
            -- instr(o_return_lkp7 , '~' ,1,4)-  instr(o_return_lkp7 , '~' ,1,3) -1)
            substr(
                o_return_lkp7,
                regexp_instr(o_return_lkp7, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp7, '~', 1, 4)
                - regexp_instr(o_return_lkp7, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce7,
            -- *INF*: substr(o_return_lkp7, instr(o_return_lkp7 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp7 , '~' ,1,5)-  instr(o_return_lkp7 , '~' ,1,4) -1)
            substr(
                o_return_lkp7,
                regexp_instr(o_return_lkp7, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp7, '~', 1, 5)
                - regexp_instr(o_return_lkp7, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce7,
            -- *INF*: substr(o_return_lkp7, instr(o_return_lkp7 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp7 , '~' ,1,6)-  instr(o_return_lkp7 , '~' ,1,5) -1)
            substr(
                o_return_lkp7,
                regexp_instr(o_return_lkp7, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp7, '~', 1, 6)
                - regexp_instr(o_return_lkp7, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt7,
            -- *INF*: substr(o_return_lkp7,instr(o_return_lkp7 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp7, regexp_instr(o_return_lkp7, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt7,
            -- *INF*: IIF(ORIG_FCTR7 < v_FCTR_LWR_TLRNCE7, v_FCTR_LWR_DFLT7
            -- , IIF(ORIG_FCTR7 > v_FCTR_UPR_TLRNCE7, v_FCTR_UPR_DFLT7, ORIG_FCTR7))
            iff(
                orig_fctr7 < v_fctr_lwr_tlrnce7,
                v_fctr_lwr_dflt7,
                iff(orig_fctr7 > v_fctr_upr_tlrnce7, v_fctr_upr_dflt7, orig_fctr7)
            ) as v_ovrrdn_fctr7,
            v_ovrrdn_fctr7 as out_ovrrdn_fctr7,
            -- *INF*: IIF(ORIG_FCTR7 < v_FCTR_LWR_TLRNCE7, 'Y'
            -- , IIF(ORIG_FCTR7 > v_FCTR_UPR_TLRNCE7, 'Y', 'N'))
            iff(
                orig_fctr7 < v_fctr_lwr_tlrnce7,
                'Y',
                iff(orig_fctr7 > v_fctr_upr_tlrnce7, 'Y', 'N')
            ) as out_ovrrdn_flg7,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP8,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ8_tbl_tech_nm.r_validation as o_return_lkp8,
            -- *INF*: substr(o_return_lkp8, instr(o_return_lkp8 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp8 , '~' ,1,3)-  instr(o_return_lkp8 , '~' ,1,2) -1)
            substr(
                o_return_lkp8,
                regexp_instr(o_return_lkp8, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp8, '~', 1, 3)
                - regexp_instr(o_return_lkp8, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm8,
            -- *INF*: substr(o_return_lkp8, instr(o_return_lkp8, '~' ,1,3) +1  ,
            -- instr(o_return_lkp8 , '~' ,1,4)-  instr(o_return_lkp8 , '~' ,1,3) -1)
            substr(
                o_return_lkp8,
                regexp_instr(o_return_lkp8, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp8, '~', 1, 4)
                - regexp_instr(o_return_lkp8, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce8,
            -- *INF*: substr(o_return_lkp8, instr(o_return_lkp8 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp8 , '~' ,1,5)-  instr(o_return_lkp8 , '~' ,1,4) -1)
            substr(
                o_return_lkp8,
                regexp_instr(o_return_lkp8, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp8, '~', 1, 5)
                - regexp_instr(o_return_lkp8, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce8,
            -- *INF*: substr(o_return_lkp8, instr(o_return_lkp8 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp8 , '~' ,1,6)-  instr(o_return_lkp8 , '~' ,1,5) -1)
            substr(
                o_return_lkp8,
                regexp_instr(o_return_lkp8, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp8, '~', 1, 6)
                - regexp_instr(o_return_lkp8, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt8,
            -- *INF*: substr(o_return_lkp8,instr(o_return_lkp8 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp8, regexp_instr(o_return_lkp8, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt8,
            -- *INF*: IIF(ORIG_FCTR8 < v_FCTR_LWR_TLRNCE8, v_FCTR_LWR_DFLT8
            -- , IIF(ORIG_FCTR8 > v_FCTR_UPR_TLRNCE8, v_FCTR_UPR_DFLT8, ORIG_FCTR8))
            iff(
                orig_fctr8 < v_fctr_lwr_tlrnce8,
                v_fctr_lwr_dflt8,
                iff(orig_fctr8 > v_fctr_upr_tlrnce8, v_fctr_upr_dflt8, orig_fctr8)
            ) as v_ovrrdn_fctr8,
            v_ovrrdn_fctr8 as out_ovrrdn_fctr8,
            -- *INF*: IIF(ORIG_FCTR8 < v_FCTR_LWR_TLRNCE8, 'Y'
            -- , IIF(ORIG_FCTR8 > v_FCTR_UPR_TLRNCE8, 'Y', 'N'))
            iff(
                orig_fctr8 < v_fctr_lwr_tlrnce8,
                'Y',
                iff(orig_fctr8 > v_fctr_upr_tlrnce8, 'Y', 'N')
            ) as out_ovrrdn_flg8,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP9,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ9_tbl_tech_nm.r_validation as o_return_lkp9,
            -- *INF*: substr(o_return_lkp9, instr(o_return_lkp9 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp9 , '~' ,1,3)-  instr(o_return_lkp9 , '~' ,1,2) -1)
            substr(
                o_return_lkp9,
                regexp_instr(o_return_lkp9, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp9, '~', 1, 3)
                - regexp_instr(o_return_lkp9, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm9,
            -- *INF*: substr(o_return_lkp9, instr(o_return_lkp9, '~' ,1,3) +1  ,
            -- instr(o_return_lkp9 , '~' ,1,4)-  instr(o_return_lkp9 , '~' ,1,3) -1)
            substr(
                o_return_lkp9,
                regexp_instr(o_return_lkp9, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp9, '~', 1, 4)
                - regexp_instr(o_return_lkp9, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce9,
            -- *INF*: substr(o_return_lkp9, instr(o_return_lkp9 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp9 , '~' ,1,5)-  instr(o_return_lkp9 , '~' ,1,4) -1)
            substr(
                o_return_lkp9,
                regexp_instr(o_return_lkp9, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp9, '~', 1, 5)
                - regexp_instr(o_return_lkp9, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce9,
            -- *INF*: substr(o_return_lkp9, instr(o_return_lkp9 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp9 , '~' ,1,6)-  instr(o_return_lkp9 , '~' ,1,5) -1)
            substr(
                o_return_lkp9,
                regexp_instr(o_return_lkp9, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp9, '~', 1, 6)
                - regexp_instr(o_return_lkp9, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt9,
            -- *INF*: substr(o_return_lkp9,instr(o_return_lkp9 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp9, regexp_instr(o_return_lkp9, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt9,
            -- *INF*: IIF(ORIG_FCTR9 < v_FCTR_LWR_TLRNCE9, v_FCTR_LWR_DFLT9
            -- , IIF(ORIG_FCTR9 > v_FCTR_UPR_TLRNCE9, v_FCTR_UPR_DFLT9, ORIG_FCTR9))
            iff(
                orig_fctr9 < v_fctr_lwr_tlrnce9,
                v_fctr_lwr_dflt9,
                iff(orig_fctr9 > v_fctr_upr_tlrnce9, v_fctr_upr_dflt9, orig_fctr9)
            ) as v_ovrrdn_fctr9,
            v_ovrrdn_fctr9 as out_ovrrdn_fctr9,
            -- *INF*: IIF(ORIG_FCTR9 < v_FCTR_LWR_TLRNCE9, 'Y'
            -- , IIF(ORIG_FCTR9 > v_FCTR_UPR_TLRNCE9, 'Y', 'N'))
            iff(
                orig_fctr9 < v_fctr_lwr_tlrnce9,
                'Y',
                iff(orig_fctr9 > v_fctr_upr_tlrnce9, 'Y', 'N')
            ) as out_ovrrdn_flg9,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP10,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ10_tbl_tech_nm.r_validation as o_return_lkp10,
            -- *INF*: substr(o_return_lkp10, instr(o_return_lkp10 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp10 , '~' ,1,3)-  instr(o_return_lkp10 , '~' ,1,2) -1)
            substr(
                o_return_lkp10,
                regexp_instr(o_return_lkp10, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp10, '~', 1, 3)
                - regexp_instr(o_return_lkp10, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm10,
            -- *INF*: substr(o_return_lkp10, instr(o_return_lkp10, '~' ,1,3) +1  ,
            -- instr(o_return_lkp10 , '~' ,1,4)-  instr(o_return_lkp10 , '~' ,1,3) -1)
            substr(
                o_return_lkp10,
                regexp_instr(o_return_lkp10, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp10, '~', 1, 4)
                - regexp_instr(o_return_lkp10, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce10,
            -- *INF*: substr(o_return_lkp10, instr(o_return_lkp10 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp10 , '~' ,1,5)-  instr(o_return_lkp10 , '~' ,1,4) -1)
            substr(
                o_return_lkp10,
                regexp_instr(o_return_lkp10, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp10, '~', 1, 5)
                - regexp_instr(o_return_lkp10, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce10,
            -- *INF*: substr(o_return_lkp10, instr(o_return_lkp10 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp10 , '~' ,1,6)-  instr(o_return_lkp10 , '~' ,1,5) -1)
            substr(
                o_return_lkp10,
                regexp_instr(o_return_lkp10, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp10, '~', 1, 6)
                - regexp_instr(o_return_lkp10, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt10,
            -- *INF*: substr(o_return_lkp10,instr(o_return_lkp10 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp10, regexp_instr(o_return_lkp10, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt10,
            -- *INF*: IIF(ORIG_FCTR10 < v_FCTR_LWR_TLRNCE10, v_FCTR_LWR_DFLT10
            -- , IIF(ORIG_FCTR10 > v_FCTR_UPR_TLRNCE10, v_FCTR_UPR_DFLT10, ORIG_FCTR10))
            iff(
                orig_fctr10 < v_fctr_lwr_tlrnce10,
                v_fctr_lwr_dflt10,
                iff(orig_fctr10 > v_fctr_upr_tlrnce10, v_fctr_upr_dflt10, orig_fctr10)
            ) as v_ovrrdn_fctr10,
            v_ovrrdn_fctr10 as out_ovrrdn_fctr10,
            -- *INF*: IIF(ORIG_FCTR10 < v_FCTR_LWR_TLRNCE10, 'Y'
            -- , IIF(ORIG_FCTR10 > v_FCTR_UPR_TLRNCE10, 'Y', 'N'))
            iff(
                orig_fctr10 < v_fctr_lwr_tlrnce10,
                'Y',
                iff(orig_fctr10 > v_fctr_upr_tlrnce10, 'Y', 'N')
            ) as out_ovrrdn_flg10,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP11,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ11_tbl_tech_nm.r_validation as o_return_lkp11,
            -- *INF*: substr(o_return_lkp11, instr(o_return_lkp11 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp11 , '~' ,1,3)-  instr(o_return_lkp11 , '~' ,1,2) -1)
            substr(
                o_return_lkp11,
                regexp_instr(o_return_lkp11, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp11, '~', 1, 3)
                - regexp_instr(o_return_lkp11, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm11,
            -- *INF*: substr(o_return_lkp11, instr(o_return_lkp11, '~' ,1,3) +1  ,
            -- instr(o_return_lkp11 , '~' ,1,4)-  instr(o_return_lkp11 , '~' ,1,3) -1)
            substr(
                o_return_lkp11,
                regexp_instr(o_return_lkp11, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp11, '~', 1, 4)
                - regexp_instr(o_return_lkp11, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce11,
            -- *INF*: substr(o_return_lkp11, instr(o_return_lkp11 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp11 , '~' ,1,5)-  instr(o_return_lkp11 , '~' ,1,4) -1)
            substr(
                o_return_lkp11,
                regexp_instr(o_return_lkp11, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp11, '~', 1, 5)
                - regexp_instr(o_return_lkp11, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce11,
            -- *INF*: substr(o_return_lkp11, instr(o_return_lkp11 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp11 , '~' ,1,6)-  instr(o_return_lkp11 , '~' ,1,5) -1)
            substr(
                o_return_lkp11,
                regexp_instr(o_return_lkp11, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp11, '~', 1, 6)
                - regexp_instr(o_return_lkp11, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt11,
            -- *INF*: substr(o_return_lkp11,instr(o_return_lkp11 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp11, regexp_instr(o_return_lkp11, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt11,
            -- *INF*: IIF(ORIG_FCTR11 < v_FCTR_LWR_TLRNCE11, v_FCTR_LWR_DFLT11
            -- , IIF(ORIG_FCTR11 > v_FCTR_UPR_TLRNCE11, v_FCTR_UPR_DFLT11, ORIG_FCTR11))
            iff(
                orig_fctr11 < v_fctr_lwr_tlrnce11,
                v_fctr_lwr_dflt11,
                iff(orig_fctr11 > v_fctr_upr_tlrnce11, v_fctr_upr_dflt11, orig_fctr11)
            ) as v_ovrrdn_fctr11,
            v_ovrrdn_fctr11 as out_ovrrdn_fctr11,
            -- *INF*: IIF(ORIG_FCTR11 < v_FCTR_LWR_TLRNCE11, 'Y'
            -- , IIF(ORIG_FCTR11 > v_FCTR_UPR_TLRNCE11, 'Y', 'N'))
            iff(
                orig_fctr11 < v_fctr_lwr_tlrnce11,
                'Y',
                iff(orig_fctr11 > v_fctr_upr_tlrnce11, 'Y', 'N')
            ) as out_ovrrdn_flg11,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP12,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ12_tbl_tech_nm.r_validation as o_return_lkp12,
            -- *INF*: substr(o_return_lkp12, instr(o_return_lkp12 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp12 , '~' ,1,3)-  instr(o_return_lkp12 , '~' ,1,2) -1)
            substr(
                o_return_lkp12,
                regexp_instr(o_return_lkp12, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp12, '~', 1, 3)
                - regexp_instr(o_return_lkp12, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm12,
            -- *INF*: substr(o_return_lkp12, instr(o_return_lkp12, '~' ,1,3) +1  ,
            -- instr(o_return_lkp12 , '~' ,1,4)-  instr(o_return_lkp12 , '~' ,1,3) -1)
            substr(
                o_return_lkp12,
                regexp_instr(o_return_lkp12, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp12, '~', 1, 4)
                - regexp_instr(o_return_lkp12, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce12,
            -- *INF*: substr(o_return_lkp12, instr(o_return_lkp12 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp12 , '~' ,1,5)-  instr(o_return_lkp12 , '~' ,1,4) -1)
            substr(
                o_return_lkp12,
                regexp_instr(o_return_lkp12, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp12, '~', 1, 5)
                - regexp_instr(o_return_lkp12, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce12,
            -- *INF*: substr(o_return_lkp12, instr(o_return_lkp12 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp12 , '~' ,1,6)-  instr(o_return_lkp12 , '~' ,1,5) -1)
            substr(
                o_return_lkp12,
                regexp_instr(o_return_lkp12, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp12, '~', 1, 6)
                - regexp_instr(o_return_lkp12, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt12,
            -- *INF*: substr(o_return_lkp12,instr(o_return_lkp12 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp12, regexp_instr(o_return_lkp12, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt12,
            -- *INF*: IIF(ORIG_FCTR12 < v_FCTR_LWR_TLRNCE12, v_FCTR_LWR_DFLT12
            -- , IIF(ORIG_FCTR12 > v_FCTR_UPR_TLRNCE12, v_FCTR_UPR_DFLT12, ORIG_FCTR12))
            iff(
                orig_fctr12 < v_fctr_lwr_tlrnce12,
                v_fctr_lwr_dflt12,
                iff(orig_fctr12 > v_fctr_upr_tlrnce12, v_fctr_upr_dflt12, orig_fctr12)
            ) as v_ovrrdn_fctr12,
            v_ovrrdn_fctr12 as out_ovrrdn_fctr12,
            -- *INF*: IIF(ORIG_FCTR12 < v_FCTR_LWR_TLRNCE12, 'Y'
            -- , IIF(ORIG_FCTR12 > v_FCTR_UPR_TLRNCE12, 'Y', 'N'))
            iff(
                orig_fctr12 < v_fctr_lwr_tlrnce12,
                'Y',
                iff(orig_fctr12 > v_fctr_upr_tlrnce12, 'Y', 'N')
            ) as out_ovrrdn_flg12,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP13,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ13_tbl_tech_nm.r_validation as o_return_lkp13,
            -- *INF*: substr(o_return_lkp13, instr(o_return_lkp13 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp13 , '~' ,1,3)-  instr(o_return_lkp13 , '~' ,1,2) -1)
            substr(
                o_return_lkp13,
                regexp_instr(o_return_lkp13, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp13, '~', 1, 3)
                - regexp_instr(o_return_lkp13, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm13,
            -- *INF*: substr(o_return_lkp13, instr(o_return_lkp13, '~' ,1,3) +1  ,
            -- instr(o_return_lkp13 , '~' ,1,4)-  instr(o_return_lkp13 , '~' ,1,3) -1)
            substr(
                o_return_lkp13,
                regexp_instr(o_return_lkp13, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp13, '~', 1, 4)
                - regexp_instr(o_return_lkp13, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce13,
            -- *INF*: substr(o_return_lkp13, instr(o_return_lkp13 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp13 , '~' ,1,5)-  instr(o_return_lkp13 , '~' ,1,4) -1)
            substr(
                o_return_lkp13,
                regexp_instr(o_return_lkp13, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp13, '~', 1, 5)
                - regexp_instr(o_return_lkp13, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce13,
            -- *INF*: substr(o_return_lkp13, instr(o_return_lkp13 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp13 , '~' ,1,6)-  instr(o_return_lkp13 , '~' ,1,5) -1)
            substr(
                o_return_lkp13,
                regexp_instr(o_return_lkp13, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp13, '~', 1, 6)
                - regexp_instr(o_return_lkp13, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt13,
            -- *INF*: substr(o_return_lkp13,instr(o_return_lkp13 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp13, regexp_instr(o_return_lkp13, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt13,
            -- *INF*: IIF(ORIG_FCTR13 < v_FCTR_LWR_TLRNCE13, v_FCTR_LWR_DFLT13
            -- , IIF(ORIG_FCTR13 > v_FCTR_UPR_TLRNCE13, v_FCTR_UPR_DFLT13, ORIG_FCTR13))
            iff(
                orig_fctr13 < v_fctr_lwr_tlrnce13,
                v_fctr_lwr_dflt13,
                iff(orig_fctr13 > v_fctr_upr_tlrnce13, v_fctr_upr_dflt13, orig_fctr13)
            ) as v_ovrrdn_fctr13,
            v_ovrrdn_fctr13 as out_ovrrdn_fctr13,
            -- *INF*: IIF(ORIG_FCTR13 < v_FCTR_LWR_TLRNCE13, 'Y'
            -- , IIF(ORIG_FCTR13 > v_FCTR_UPR_TLRNCE13, 'Y', 'N'))
            iff(
                orig_fctr13 < v_fctr_lwr_tlrnce13,
                'Y',
                iff(orig_fctr13 > v_fctr_upr_tlrnce13, 'Y', 'N')
            ) as out_ovrrdn_flg13,
            -- *INF*: :LKP.LKP_WRK_FCTR_TLRNC(FCTR_TYP14,TBL_TECH_NM)
            lkp_wrk_fctr_tlrnc_fctr_typ14_tbl_tech_nm.r_validation as o_return_lkp14,
            -- *INF*: substr(o_return_lkp14, instr(o_return_lkp14 , '~' ,1,2) +1  ,
            -- instr(o_return_lkp14 , '~' ,1,3)-  instr(o_return_lkp14 , '~' ,1,2) -1)
            substr(
                o_return_lkp14,
                regexp_instr(o_return_lkp14, '~', 1, 2) + 1,
                regexp_instr(o_return_lkp14, '~', 1, 3)
                - regexp_instr(o_return_lkp14, '~', 1, 2)
                - 1
            ) as o_attr_tech_nm14,
            -- *INF*: substr(o_return_lkp14, instr(o_return_lkp14, '~' ,1,3) +1  ,
            -- instr(o_return_lkp14 , '~' ,1,4)-  instr(o_return_lkp14 , '~' ,1,3) -1)
            substr(
                o_return_lkp14,
                regexp_instr(o_return_lkp14, '~', 1, 3) + 1,
                regexp_instr(o_return_lkp14, '~', 1, 4)
                - regexp_instr(o_return_lkp14, '~', 1, 3)
                - 1
            ) as v_fctr_lwr_tlrnce14,
            -- *INF*: substr(o_return_lkp14, instr(o_return_lkp14 , '~' ,1,4) +1  ,
            -- instr(o_return_lkp14 , '~' ,1,5)-  instr(o_return_lkp14 , '~' ,1,4) -1)
            substr(
                o_return_lkp14,
                regexp_instr(o_return_lkp14, '~', 1, 4) + 1,
                regexp_instr(o_return_lkp14, '~', 1, 5)
                - regexp_instr(o_return_lkp14, '~', 1, 4)
                - 1
            ) as v_fctr_upr_tlrnce14,
            -- *INF*: substr(o_return_lkp14, instr(o_return_lkp14 , '~' ,1,5) +1  ,
            -- instr(o_return_lkp14 , '~' ,1,6)-  instr(o_return_lkp14 , '~' ,1,5) -1)
            substr(
                o_return_lkp14,
                regexp_instr(o_return_lkp14, '~', 1, 5) + 1,
                regexp_instr(o_return_lkp14, '~', 1, 6)
                - regexp_instr(o_return_lkp14, '~', 1, 5)
                - 1
            ) as v_fctr_lwr_dflt14,
            -- *INF*: substr(o_return_lkp14,instr(o_return_lkp14 , '~' ,-1,1) +1 )
            substr(
                o_return_lkp14, regexp_instr(o_return_lkp14, '~', - 1, 1) + 1
            ) as v_fctr_upr_dflt14,
            -- *INF*: IIF(ORIG_FCTR14 < v_FCTR_LWR_TLRNCE14, v_FCTR_LWR_DFLT14
            -- , IIF(ORIG_FCTR14 > v_FCTR_UPR_TLRNCE14, v_FCTR_UPR_DFLT14, ORIG_FCTR14))
            iff(
                orig_fctr14 < v_fctr_lwr_tlrnce14,
                v_fctr_lwr_dflt14,
                iff(orig_fctr14 > v_fctr_upr_tlrnce14, v_fctr_upr_dflt14, orig_fctr14)
            ) as v_ovrrdn_fctr14,
            v_ovrrdn_fctr14 as out_ovrrdn_fctr14,
            -- *INF*: IIF(ORIG_FCTR14 < v_FCTR_LWR_TLRNCE14, 'Y'
            -- , IIF(ORIG_FCTR14 > v_FCTR_UPR_TLRNCE14, 'Y', 'N'))
            iff(
                orig_fctr14 < v_fctr_lwr_tlrnce14,
                'Y',
                iff(orig_fctr14 > v_fctr_upr_tlrnce14, 'Y', 'N')
            ) as out_ovrrdn_flg14,
            'Factor has exceeded its limit' as out_err_desc
        from mplt_input
        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ_tbl_tech_nm.attr_tech_nm = fctr_typ
            and lkp_wrk_fctr_tlrnc_fctr_typ_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ1_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ1_tbl_tech_nm.attr_tech_nm = fctr_typ1
            and lkp_wrk_fctr_tlrnc_fctr_typ1_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ2_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ2_tbl_tech_nm.attr_tech_nm = fctr_typ2
            and lkp_wrk_fctr_tlrnc_fctr_typ2_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ3_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ3_tbl_tech_nm.attr_tech_nm = fctr_typ3
            and lkp_wrk_fctr_tlrnc_fctr_typ3_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ4_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ4_tbl_tech_nm.attr_tech_nm = fctr_typ4
            and lkp_wrk_fctr_tlrnc_fctr_typ4_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ5_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ5_tbl_tech_nm.attr_tech_nm = fctr_typ5
            and lkp_wrk_fctr_tlrnc_fctr_typ5_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ6_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ6_tbl_tech_nm.attr_tech_nm = fctr_typ6
            and lkp_wrk_fctr_tlrnc_fctr_typ6_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ7_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ7_tbl_tech_nm.attr_tech_nm = fctr_typ7
            and lkp_wrk_fctr_tlrnc_fctr_typ7_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ8_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ8_tbl_tech_nm.attr_tech_nm = fctr_typ8
            and lkp_wrk_fctr_tlrnc_fctr_typ8_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ9_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ9_tbl_tech_nm.attr_tech_nm = fctr_typ9
            and lkp_wrk_fctr_tlrnc_fctr_typ9_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ10_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ10_tbl_tech_nm.attr_tech_nm = fctr_typ10
            and lkp_wrk_fctr_tlrnc_fctr_typ10_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ11_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ11_tbl_tech_nm.attr_tech_nm = fctr_typ11
            and lkp_wrk_fctr_tlrnc_fctr_typ11_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ12_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ12_tbl_tech_nm.attr_tech_nm = fctr_typ12
            and lkp_wrk_fctr_tlrnc_fctr_typ12_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ13_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ13_tbl_tech_nm.attr_tech_nm = fctr_typ13
            and lkp_wrk_fctr_tlrnc_fctr_typ13_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

        left join
            lkp_wrk_fctr_tlrnc lkp_wrk_fctr_tlrnc_fctr_typ14_tbl_tech_nm
            on lkp_wrk_fctr_tlrnc_fctr_typ14_tbl_tech_nm.attr_tech_nm = fctr_typ14
            and lkp_wrk_fctr_tlrnc_fctr_typ14_tbl_tech_nm.tbl_tech_nm = mplt_input.tbl_tech_nm

    )
select
    o_subj_area as subj_area,
    o_tbl_tech_nm as tbl_tech_nm1,
    o_attr_tech_nm as attr_tech_nm,
    orig_fctr,
    out_ovrrdn_fctr as ovrrdn_fctr,
    out_ovrrdn_flg as ovrrdn_flg,
    out_err_desc as err_desc,
    o_attr_tech_nm1 as attr_tech_nm1,
    orig_fctr1,
    out_ovrrdn_fctr1 as ovrrdn_fctr1,
    out_ovrrdn_flg1 as ovrrdn_flg1,
    o_attr_tech_nm2 as attr_tech_nm2,
    orig_fctr2,
    out_ovrrdn_fctr2 as ovrrdn_fctr2,
    out_ovrrdn_flg2 as ovrrdn_flg2,
    o_attr_tech_nm3 as attr_tech_nm3,
    orig_fctr3,
    out_ovrrdn_fctr3 as ovrrdn_fctr3,
    out_ovrrdn_flg3 as ovrrdn_flg3,
    o_attr_tech_nm4 as attr_tech_nm4,
    orig_fctr4,
    out_ovrrdn_fctr4 as ovrrdn_fctr4,
    out_ovrrdn_flg4 as ovrrdn_flg4,
    o_attr_tech_nm5 as attr_tech_nm5,
    orig_fctr5,
    out_ovrrdn_fctr5 as ovrrdn_fctr5,
    out_ovrrdn_flg5 as ovrrdn_flg5,
    o_attr_tech_nm6 as attr_tech_nm6,
    orig_fctr6,
    out_ovrrdn_fctr6 as ovrrdn_fctr6,
    out_ovrrdn_flg6 as ovrrdn_flg6,
    o_attr_tech_nm7 as attr_tech_nm7,
    orig_fctr7,
    out_ovrrdn_fctr7 as ovrrdn_fctr7,
    out_ovrrdn_flg7 as ovrrdn_flg7,
    o_attr_tech_nm8 as attr_tech_nm8,
    orig_fctr8,
    out_ovrrdn_fctr8 as ovrrdn_fctr8,
    out_ovrrdn_flg8 as ovrrdn_flg8,
    o_attr_tech_nm9 as attr_tech_nm9,
    orig_fctr9,
    out_ovrrdn_fctr9 as ovrrdn_fctr9,
    out_ovrrdn_flg9 as ovrrdn_flg9,
    o_attr_tech_nm10 as attr_tech_nm10,
    orig_fctr10,
    out_ovrrdn_fctr10 as ovrrdn_fctr10,
    out_ovrrdn_flg10 as ovrrdn_flg10,
    o_attr_tech_nm11 as attr_tech_nm11,
    orig_fctr11,
    out_ovrrdn_fctr11 as ovrrdn_fctr11,
    out_ovrrdn_flg11 as ovrrdn_flg11,
    o_attr_tech_nm12 as attr_tech_nm12,
    orig_fctr12,
    out_ovrrdn_fctr12 as ovrrdn_fctr12,
    out_ovrrdn_flg12 as ovrrdn_flg12,
    o_attr_tech_nm13 as attr_tech_nm13,
    orig_fctr13,
    out_ovrrdn_fctr13 as ovrrdn_fctr13,
    out_ovrrdn_flg13 as ovrrdn_flg13,
    o_attr_tech_nm14 as attr_tech_nm14,
    orig_fctr14,
    out_ovrrdn_fctr14 as ovrrdn_fctr14,
    out_ovrrdn_flg14 as ovrrdn_flg14,
    rn
from exp_tlrnc_chk


{%- endmacro %}