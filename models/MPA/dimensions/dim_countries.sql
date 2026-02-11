{{ config(materialized='table', schema="MPA") }}

select distinct
  country_code,
  country_description as country_name,
  nationality
from   `photons-377606.MPA.MPA_CountryCodesDataInJsonFormat`