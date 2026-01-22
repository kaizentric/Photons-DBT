{{ config( materialized = 'table', schema = "MPA_External_Tables"
) }}

select
    cast(month as string) as month,
  PARSE_DATE('%Y-%m-%d', CONCAT(month, '-01')) AS sales_date,

    cast(bunker_type as string) as bunker_type,
    cast(bunker_sales as float64) as bunker_sales,
    coalesce(ingested_at, current_timestamp()) as ingested_at
from `photons-377606.MPA_External_Tables.bunker-sales-breakdown-monthly`