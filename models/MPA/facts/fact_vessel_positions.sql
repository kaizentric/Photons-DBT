{{ config(materialized='incremental',unique_key=['imo_number','position_timestamp'],
    schema='MPA',
    tags=['skip'])}}

select
  p.imo_number,
  v.vessel_name,
  p.position_timestamp,
  p.latitude,
  p.longitude,
  p.speed,
  p.course,
  p.heading
from {{ ref('stg_vessel_positions') }} p
left join {{ ref('dim_vessels') }} v
  on p.imo_number = v.imo_number

{% if is_incremental() %}
where p.position_timestamp >
  (select max(position_timestamp) from {{ this }})
{% endif %}
