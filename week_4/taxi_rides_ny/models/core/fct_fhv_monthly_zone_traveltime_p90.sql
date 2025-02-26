with trips as (
    select
        *,
        EXTRACT(
            epoch from (dropoff_datetime - pickup_datetime)
        ) as trip_duration,
        EXTRACT(year from pickup_datetime) as pickup_year,
        EXTRACT(month from pickup_datetime) as pickup_month
    from {{ ref('fact_fhv_trips') }}
)

select
    pickup_year,
    pickup_month,
    pickup_locationid,
    dropoff_locationid,
    pickup_zone,
    dropoff_zone,

    PERCENTILE_CONT(0.90) within group (
        order by trip_duration
    ) as p90

from trips
group by
    pickup_year,
    pickup_month,
    pickup_locationid,
    dropoff_locationid,
    dropoff_zone,
    pickup_zone
