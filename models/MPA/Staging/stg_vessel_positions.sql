{{ config(materialized='incremental',unique_key=['imo_number','position_timestamp'],
    schema='MPA',
    tags=['skip'])}}

select
  imo_number,
  vessel_name,
  call_sign,
  latitude,
  longitude,
  latitude_degrees,
  longitude_degrees,
  speed,
  course,
  heading,
  effective_timestamp as position_timestamp,
  ingested_at
 

from   `photons-377606.MPA.MPA_VesselPositionsSnapshot`
{% if is_incremental() %}
where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
