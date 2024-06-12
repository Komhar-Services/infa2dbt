{{ config(materialized="view") }}

with mntry_acct as (select * from {{ source("dbaall", "mntry_acct") }})
select acct_id, extl_refr_cd, typ_id
from
    (
        select
            mntry_acct.acct_id as acct_id,
            mntry_acct.extl_refr_cd as extl_refr_cd,
            mntry_acct.typ_id as typ_id
        from mntry_acct
    )
qualify
    row_number() over (partition by acct_id, extl_refr_cd, typ_id order by acct_id) = 1
