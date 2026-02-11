{{ config(materialized='table', schema="MPA") }}

select
  imo_number,
  vessel_name,
  movement_start_timestamp,
  movement_end_timestamp,
  movement_status,
  movement_type,
  location_from,
  location_to,
  movement_draft,
  movement_height
from {{ ref('stg_vessel_movements') }}
-- {% if is_incremental() %}
-- where movement_start_timestamp > (select max(movement_start_timestamp) from {{ this }})
-- {% endif %}
