{{ config(materialized='table', schema="MPA") }}

select
  habitat_code,
  habitat_type,
  geom,
  shape_area,
  shape_len
 
from   `photons-377606.MPA.CoastalandMarineHabitatMapofSingapore`