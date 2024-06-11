{{ config(materialized="view") }}

with
    work_prcg_cyc_dt as (select * from {{ source("dbaall", "work_prcg_cyc_dt") }}),
    typ as (select * from {{ source("dbaall", "typ") }})
select pst_dt, actv_rec_ind
from
    (
        select
            work_prcg_cyc_dt.actv_rec_ind as actv_rec_ind,
            work_prcg_cyc_dt.pst_dt as pst_dt
        from work_prcg_cyc_dt work_prcg_cyc_dt
        where
            typ_id in (select typ_id from typ where nm = 'DAILY') and actv_rec_ind = 'Y'
    )
qualify row_number() over (partition by actv_rec_ind order by pst_dt) = 1
