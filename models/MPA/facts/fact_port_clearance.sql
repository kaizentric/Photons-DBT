{{ config(materialized='incremental', schema="MPA") }}

select
  imo_number,
  vessel_name,
  clearance_date,
  raw_payload,
  fetched_at
 

from   `photons-377606.MPA.port_clearance_certificates` 
{% if is_incremental() %}
where fetched_at > (select max(fetched_at) from {{ this }})
{% endif %}
