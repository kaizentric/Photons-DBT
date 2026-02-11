{{ config(materialized='incremental', schema="MPA") }}

select
  vesselParticulars.imoNumber as imo_number,
  vesselParticulars.vesselName as vessel_name,
  arrivedTime,
  locationFrom,
  locationTo 
 

from   `photons-377606.MPA.vessel_arrivals` 
{% if is_incremental() %}
where arrivedTime > (select max(arrivedTime) from {{ this }})
{% endif %}