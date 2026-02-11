
{{ config(materialized='incremental', schema="MPA") }}

select
  imo_number,
  vessel_name,
  departed_time,
  location_from,
  location_to,
  fetched_at
 

from   `photons-377606.MPA.vessel_departures` 
{% if is_incremental() %}
where fetched_at > (select max(fetched_at) from {{ this }})
{% endif %}