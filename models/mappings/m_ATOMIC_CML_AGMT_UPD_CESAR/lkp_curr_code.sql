{{ config(materialized="view") }}

with
    stg_pol_contr as (select * from {{ source("dbaall", "stg_pol_contr") }}),
    stg_pol_covg as (select * from {{ source("dbaall", "stg_pol_covg") }}),
    stg_acty_log as (select * from {{ source("dbaall", "stg_acty_log") }})
select trans_crcy_abbr, local_crcy_abbr, pol_nbr, pol_eff_dt, modu_nbr
from
    (
        select distinct
            trim(pc.trans_crcy_abbr) as trans_crcy_abbr,
            trim(pc.local_crcy_abbr) as local_crcy_abbr,
            trim(pc.pol_nbr) as pol_nbr,
            to_char(pc.pol_eff_dt, 'YYYYMMDD') as pol_eff_dt,
            trim(pc.modu_nbr) as modu_nbr
        from
            stg_pol_contr pol,
            {{ var("dbownerstg") }}.dbaall.stg_pol_covg pc,
            {{ var("dbownerstg") }}.dbaall.stg_acty_log al
        where
            pc.pol_nbr = al.pol_nbr
            and pc.pol_eff_dt = al.pol_eff_dt
            and pc.modu_nbr = al.modu_nbr
            and pc.acty_seq_nbr = al.acty_seq_nbr
            and pol.pol_nbr = al.pol_nbr
            and pol.pol_eff_dt = al.pol_eff_dt
            and pol.modu_nbr = al.modu_nbr
            and pol.acty_seq_nbr = (
                select max(pol2.acty_seq_nbr)
                from stg_pol_contr pol2
                where
                    pol.pol_nbr = pol2.pol_nbr
                    and pol.pol_eff_dt = pol2.pol_eff_dt
                    and pol.modu_nbr = pol2.modu_nbr
            )
            and pc.pol_nbr >= 'K000000'
            and pc.pol_nbr <= 'P'
    )
qualify
    row_number() over (
        partition by pol_nbr, pol_eff_dt, modu_nbr order by trans_crcy_abbr
    )
    = 1
