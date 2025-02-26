with trips_data as (
    select * from {{ ref('fact_trips') }}
),

quarterly_data as (
    select
    -- Revenue grouping 
        extract(quarter from pickup_datetime) as revenue_quarter,
        extract(year from pickup_datetime) as revenue_year,
        service_type,

        -- Revenue calculation 
        sum(total_amount) as revenue_quarter_total_amount,

        -- Additional calculations
        count(tripid) as total_monthly_trips,
        avg(passenger_count) as avg_monthly_passenger_count,
        avg(trip_distance) as avg_monthly_trip_distance

    from trips_data
    where extract(year from pickup_datetime) in (2019, 2020)
    group by service_type, revenue_quarter, revenue_year
),

previous_year_data as (
    select
        service_type,
        revenue_quarter,
        revenue_year,
        revenue_quarter_total_amount,
        lag(revenue_quarter_total_amount, 4)
            over (
                partition by service_type
                order by service_type, revenue_year, revenue_quarter
            )
        as last_year_same_quarter
    from quarterly_data
    order by service_type asc, revenue_year desc, revenue_quarter desc
)

select
    service_type,
    revenue_quarter,
    revenue_year,
    revenue_quarter_total_amount,
    last_year_same_quarter,
    (revenue_quarter_total_amount - last_year_same_quarter)
    / last_year_same_quarter as yoy_growth
from previous_year_data
where last_year_same_quarter is not null
order by yoy_growth desc, service_type asc
