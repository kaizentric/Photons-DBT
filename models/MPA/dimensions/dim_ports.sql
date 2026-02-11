{{ config(materialized='table', schema="MPA") }}

select distinct
  port_code,
  port_description as port_name,
  effective_timestamp
 

from   `photons-377606.MPA.MPA_PortCodesDataInJsonFormat`