{{ 
  config(
    materialized = 'incremental',
     schema = "MPA_External_Tables",
    unique_key = ['month', 'bunker_type'],
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
  ) 
}}

select
    cast(month as string) as month,
  PARSE_DATE('%Y-%m-%d', CONCAT(month, '-01')) AS sales_date,

    cast(bunker_type as string) as bunker_type,
    cast(bunker_sales as float64) as bunker_sales,
    coalesce(ingested_at, current_timestamp()) as ingested_at
from `photons-377606.MPA_External_Tables.bunker-sales-breakdown-monthly`

{% if is_incremental() %}
    -- Only load new or updated records
    WHERE ingested_at > (
      SELECT MAX(ingested_at) FROM {{ this }}
    )
  {% endif %}
-- #incremental

