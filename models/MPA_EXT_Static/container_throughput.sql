{{  
  config(
    materialized = 'incremental',
    schema = 'MPA_External_Tables',
    unique_key = ['month'],
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
  ) 
}}

select
    cast(month as string) as month,

    -- Convert month (YYYY-MM) to DATE (first day of month)
    parse_date('%Y-%m-%d', concat(month, '-01')) as throughput_month_date,

    cast(container_throughput as float64) as container_throughput,

    coalesce(ingested_at, current_timestamp()) as ingested_at

from `photons-377606.MPA_External_Tables.container-throughput-monthly`

{% if is_incremental() %}
  -- Load only new or updated records
  where ingested_at > (
    select max(ingested_at) from {{ this }}
  )
{% endif %}
