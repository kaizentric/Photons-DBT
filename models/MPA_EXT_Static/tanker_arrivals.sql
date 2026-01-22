{{  
  config(
    materialized = 'incremental',
    schema = 'MPA_External_Tables',
    unique_key = ['month', 'category'],
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
  ) 
}}

select
    cast(month as string) as month,

    -- Convert YYYY-MM → DATE (first day of month)
    parse_date('%Y-%m-%d', concat(month, '-01')) as reporting_month_date,

    cast(category as string) as category,

    cast(number_of_tankers as int64) as number_of_tankers,

    cast(gross_tonnage as float64) as gross_tonnage,

    coalesce(ingested_at, current_timestamp()) as ingested_at

from `photons-377606.MPA_External_Tables.tanker-arrivals-breakdown-monthly`

{% if is_incremental() %}
  -- Only load new or updated records
  where ingested_at > (
    select max(ingested_at) from {{ this }}
  )
{% endif %}
