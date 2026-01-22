{{  
  config(
    materialized = 'incremental',
    schema = 'MPA_External_Tables',
    unique_key = ['month', 'cargo_type_primary', 'cargo_type_secondary'],
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
  ) 
}}

select
    cast(month as string) as month,

    -- Convert month string to DATE (first day of month)
    parse_date('%Y-%m-%d', concat(month, '-01')) as cargo_month_date,

    cast(cargo_type_primary as string) as cargo_type_primary,

    cast(cargo_type_secondary as string) as cargo_type_secondary,

    cast(cargo_throughput as float64) as cargo_throughput,

    coalesce(ingested_at, current_timestamp()) as ingested_at

from `photons-377606.MPA_External_Tables.cargo-throughput-breakdown-monthly-data`

{% if is_incremental() %}
  -- Only load new or updated records
  where ingested_at > (
    select max(ingested_at) from {{ this }}
  )
{% endif %}
