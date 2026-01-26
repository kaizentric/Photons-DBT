{{ 
  config(
    materialized = 'table',
     schema = "MPA_External_Tables"
     
  ) 
}}
select
    vc.purpose_type,
    vc.number_of_vessel_calls,
    vc.gross_tonnage,
    vc.ingested_at,
    vc.reporting_month_date,
    bs.bunker_sales,
    ct.cargo_throughput
from {{ ref('vessel-calls_75_gt_monthly') }} vc

left join {{ ref('bunker_sales') }} bs
    on vc.reporting_month_date = bs.sales_date
   and vc.purpose_type = 'Bunkers'

left join {{ ref('cargo_throughput') }} ct
    on vc.reporting_month_date = ct.cargo_month_date
   and vc.purpose_type = 'Cargo'

-- ✅ Exclude rows where BOTH metrics are missing
where not (
    bs.bunker_sales is null
    and ct.cargo_throughput is null
)

order by vc.number_of_vessel_calls desc
