{{
	config(
		materialized = "view"
	)
}}

with typ as (select * from {{ source("dbaall","typ") }})
select typ_id, nm
from (select a.typ_id as typ_id, a.nm as nm from typ a)
qualify row_number() over (partition by nm order by typ_id) = 1
