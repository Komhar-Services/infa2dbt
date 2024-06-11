{{ config(materialized="view") }}

with geog_area as (select * from {{ source("", "geog_area") }})
select plc_anchr_id, typ_id, cd
from
    (
        select a.plc_anchr_id as plc_anchr_id, a.typ_id as typ_id, trim(a.cd) as cd
        from geog_area a
        where a.actv_rec_ind = 'Y'
    )
qualify row_number() over (partition by typ_id, cd order by plc_anchr_id) = 1
