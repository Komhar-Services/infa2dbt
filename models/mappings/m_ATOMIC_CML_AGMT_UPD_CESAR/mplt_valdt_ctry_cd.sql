{{ config(materialized="ephemeral") }}

with
    r_lkp_typ_id as (select * from {{ ref("r_lkp_typ_id") }}),
    lkp_ctry_cd as (select * from {{ ref("lkp_ctry_cd") }}),
    mapplet_input as (select in_ctry_cd from mapplet_cte),
    exp_lkp_ctry_cd as (
        select
            in_ctry_cd,
            -- *INF*: :LKP.r_LKP_TYP_ID('COUNTRY')
            r_lkp_typ_id__country.typ_id as v_typ_id,
            -- *INF*: IIF(ISNULL(in_CTRY_CD) OR IS_SPACES (in_CTRY_CD) OR
            -- LTRIM(RTRIM(in_CTRY_CD)) = 'null' OR LTRIM(RTRIM(in_CTRY_CD))='',1,0)
            iff(
                in_ctry_cd is null
                or length(in_ctry_cd) > 0
                and trim(in_ctry_cd) = ''
                or ltrim(rtrim(in_ctry_cd)) = 'null'
                or ltrim(rtrim(in_ctry_cd)) = '',
                1,
                0
            ) as v_invalid_cd,
            -- *INF*: IIF(v_INVALID_CD = 0 , :lkp.LKP_CTRY_CD (v_TYP_ID,
            -- LTRIM(RTRIM(in_CTRY_CD))), 0)
            --
            iff(v_invalid_cd = 0,, 0) as v_valdt_plc_anchr_id_ctry_cd,
            -- *INF*: DECODE(TRUE,v_INVALID_CD = 0 AND (NOT
            -- ISNULL(v_VALDT_PLC_ANCHR_ID_CTRY_CD)),in_CTRY_CD,v_INVALID_CD = 0 AND
            -- (ISNULL(v_VALDT_PLC_ANCHR_ID_CTRY_CD)), '711',v_INVALID_CD = 1, 'null')
            decode(
                true,
                v_invalid_cd = 0 and (v_valdt_plc_anchr_id_ctry_cd is not null),
                in_ctry_cd,
                v_invalid_cd = 0 and (v_valdt_plc_anchr_id_ctry_cd is null),
                '711',
                v_invalid_cd = 1,
                'null'
            ) as v_ctry_cd,
            -- *INF*: :lkp.LKP_CTRY_CD (v_TYP_ID, LTRIM(RTRIM(v_CTRY_CD)))
            '' as out_valdt_plc_anchr_id,
            -- *INF*: IIF(ISNULL(v_VALDT_PLC_ANCHR_ID_CTRY_CD),0,1)
            iff(v_valdt_plc_anchr_id_ctry_cd is null, 0, 1) as out_validflag,
            v_ctry_cd as out_ctry_cd
        from mapplet_input
        left join
            r_lkp_typ_id r_lkp_typ_id__country on r_lkp_typ_id__country.nm = 'COUNTRY'

        left join
            lkp_ctry_cd lkp_ctry_cd_v_typ_id_ltrim_rtrim_in_ctry_cd
            on lkp_ctry_cd_v_typ_id_ltrim_rtrim_in_ctry_cd. = v_typ_id
            and lkp_ctry_cd_v_typ_id_ltrim_rtrim_in_ctry_cd. = ltrim(rtrim(in_ctry_cd))

        left join
            lkp_ctry_cd lkp_ctry_cd_v_typ_id_ltrim_rtrim_v_ctry_cd
            on lkp_ctry_cd_v_typ_id_ltrim_rtrim_v_ctry_cd. = v_typ_id
            and lkp_ctry_cd_v_typ_id_ltrim_rtrim_v_ctry_cd. = ltrim(rtrim(v_ctry_cd))

    ),
select
    in_ctry_cd,
    out_valdt_plc_anchr_id as out_plc_ctry_anchr_id,
    out_validflag,
    out_ctry_cd
from exp_lkp_ctry_cd
