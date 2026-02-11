{{
  config(
    materialized = 'incremental',
    schema = 'Photons_MPA_External_Tables',
    incremental_strategy = 'merge',
    unique_key = 'reporting_month_date',
    on_schema_change = 'append_new_columns'
  )
}}

SELECT
  COALESCE(
    ct.throughput_month_date,
    st.reporting_month_date
  ) AS reporting_month_date,

  ct.container_throughput,
  st.number_of_vessels,
  st.gross_tonnage,

  COALESCE(ct.ingested_at, st.ingested_at, CURRENT_TIMESTAMP()) AS ingested_at

FROM {{ ref('container_throughput') }} AS ct
FULL OUTER JOIN {{ ref('shipping_tonnage') }} AS st
  ON ct.throughput_month_date = st.reporting_month_date

{% if is_incremental() %}
WHERE
  COALESCE(ct.ingested_at, st.ingested_at) >
  (SELECT MAX(ingested_at) FROM {{ this }})
{% endif %}