{{ config(materialized='view',schema = "MPA") }}

select distinct
  imo_number,
  vessel_name,
  call_sign,
  flag,
  vessel_type,
  vessel_length,
  vessel_breadth,
  vessel_depth,
  gross_tonnage,
  net_tonnage,
  deadweight,
  mmsi_number,
  year_built,
  ingested_at
 

from   `photons-377606.MPA.MPA_VesselParticularsbyIMONumber`
where imo_number is not null
