with ranks as (
    select
        pickup_zone,
        dropoff_zone,
        p90,
        DENSE_RANK() over (
            partition by pickup_zone
            order by p90 desc
        ) as rank_of_p90
    from {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
    where
        pickup_year = 2019
        and pickup_month = 11
        and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
)

select *
from ranks
where rank_of_p90 = 2
