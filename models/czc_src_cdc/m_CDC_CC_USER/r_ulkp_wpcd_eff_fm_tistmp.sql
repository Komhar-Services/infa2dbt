{{ config(materialized="view") }}

with work_prcg_cyc_dt as (select * from {{ source("dbaall", "work_prcg_cyc_dt") }})
select mntry_end_prcg_tistmp, dummy
from
    (
        select
            work_prcg_cyc_dt.mntry_end_prcg_tistmp as mntry_end_prcg_tistmp, 1 as dummy
        from work_prcg_cyc_dt work_prcg_cyc_dt
        where typ_id = 547 and actv_rec_ind = 'Y'
    )
qualify row_number() over (partition by dummy order by mntry_end_prcg_tistmp) = 1
