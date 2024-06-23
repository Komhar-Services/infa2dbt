{{
	config(
		materialized = "view"
	)
}}

with geog_area as (select * from {{ source("dbaall","geog_area") }})
select plc_anchr_id, abbr, in_abbr
from (select plc_anchr_id, abbr, in_abbr from geog_area)
qualify row_number() over (partition by abbr order by plc_anchr_id) = 1
