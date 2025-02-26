{{
    config(
        materialized='table'
    )
}}

with trip_data as (
    select * from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    trip_data.*,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone
from trip_data
inner join dim_zones as pickup_zone
    on trip_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on trip_data.dropoff_locationid = dropoff_zone.locationid
