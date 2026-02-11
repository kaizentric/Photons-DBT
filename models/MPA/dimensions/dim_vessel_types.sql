{{ config(materialized='table', schema="MPA") }}

select distinct
  vessel_type_code,
  vessel_type_description as vessel_type_name,
  effective_timestamp
 
from   `photons-377606.MPA.MPA_VesselTypeDataInJsonFormat`
