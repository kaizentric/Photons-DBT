{{ config(materialized='table', schema="MPA") }}

select
  certificate_number,
  imo_number,
  vessel_name,
  registration_status,
  registration_date,
  validity_date,
  closure_date,
  issue_date
 

from   `photons-377606.MPA.srs_certificates` 
-- {% if is_incremental() %}
-- where fetched_at > (select max(fetched_at) from {{ this }})
-- {% endif %}