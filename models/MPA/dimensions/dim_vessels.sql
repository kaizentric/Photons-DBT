{{ config(materialized='table', schema="MPA") }}

select
  imo_number,
  any_value(vessel_name) as vessel_name,
  any_value(call_sign) as call_sign,
  any_value(flag) as flag,
  any_value(vessel_type) as vessel_type,
  max(ingested_at) as last_updated
from {{ ref('stg_vessels') }}



group by imo_number
