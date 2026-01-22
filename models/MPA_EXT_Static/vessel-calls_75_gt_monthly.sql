{{  
  config(
    materialized = 'incremental',
    schema = 'MPA_External_Tables',
    unique_key = ['month', 'purpose_type'],
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
  ) 
}}

select
    cast(month as string) as month,

    -- Convert YYYY-MM → DATE (first day of month)
    parse_date('%Y-%m-%d', concat(month, '-01')) as reporting_month_date,

    cast(purpose_type as string) as purpose_type,

    cast(number_of_vessel_calls as int64) as number_of_vessel_calls,

    cast(gross_tonnage as float64) as gross_tonnage,

    coalesce(ingested_at, current_timestamp()) as ingested_at

from `photons-377606.MPA_External_Tables.vessel-calls-75-gt-monthly`

{% if is_incremental() %}
  -- Load only new or updated records
  where ingested_at > (
    select max(ingested_at) from {{ this }}
  )
{% endif %}
