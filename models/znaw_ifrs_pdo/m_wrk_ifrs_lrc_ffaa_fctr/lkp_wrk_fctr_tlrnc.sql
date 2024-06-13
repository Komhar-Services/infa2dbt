{{ config(materialized="view") }}

with wrk_fctr_tlrnc as (select * from {{ source("dbaall", "wrk_fctr_tlrnc") }})
select
    r_validation,
    subj_area,
    tbl_tech_nm,
    attr_tech_nm,
    fctr_lwr_tlrnce,
    fctr_upr_tlrnce,
    fctr_lwr_dflt,
    fctr_upr_dflt
from
    (
        select
            (
                wrk_fctr_tlrnc.subj_area
                || '~'
                || wrk_fctr_tlrnc.tbl_tech_nm
                || '~'
                || wrk_fctr_tlrnc.attr_tech_nm
                || '~'
                || wrk_fctr_tlrnc.fctr_lwr_tlrnce
                || '~'
                || wrk_fctr_tlrnc.fctr_upr_tlrnce
                || '~'
                || wrk_fctr_tlrnc.fctr_lwr_dflt
                || '~'
                || wrk_fctr_tlrnc.fctr_upr_dflt
            ) as r_validation,
            wrk_fctr_tlrnc.subj_area as subj_area,
            wrk_fctr_tlrnc.tbl_tech_nm as tbl_tech_nm,
            wrk_fctr_tlrnc.fctr_lwr_tlrnce as fctr_lwr_tlrnce,
            wrk_fctr_tlrnc.fctr_upr_tlrnce as fctr_upr_tlrnce,
            wrk_fctr_tlrnc.fctr_lwr_dflt as fctr_lwr_dflt,
            wrk_fctr_tlrnc.fctr_upr_dflt as fctr_upr_dflt,
            wrk_fctr_tlrnc.attr_tech_nm as attr_tech_nm
        from wrk_fctr_tlrnc
    )
qualify
    row_number() over (partition by attr_tech_nm, tbl_tech_nm order by r_validation) = 1
