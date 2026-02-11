{{ config(materialized='view',schema ="MPA") }}

select
  imo_number,
  vessel_name,
  call_sign,
  flag,
  movement_start_timestamp,
  movement_end_timestamp,
  movement_status,
  movement_type,
  location_from,
  location_to,
  movement_draft,
  movement_height,
  ingested_at
 
from   `photons-377606.MPA.MPA_VesselMovementsbyIMONumber`