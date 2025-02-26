with trips_data as (

    select
        *,
        extract(year from pickup_datetime) as pickup_year,
        extract(month from pickup_datetime) as pickup_month
    from {{ ref('fact_trips') }}
    where
        fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit card')
)

select
    pickup_year,
    pickup_month,
    service_type,
    percentile_cont(0.97) within group (
        order by fare_amount
    ) as p97,
    percentile_cont(0.95) within group (
        order by fare_amount
    ) as p95,
    percentile_cont(0.90) within group (
        order by fare_amount
    ) as p90
from trips_data
group by service_type, pickup_year, pickup_month
