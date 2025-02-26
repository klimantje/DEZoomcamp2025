{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    unique_row_id as tripid,


    
        
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,


    
        
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    dispatching_base_num,
    sr_flag,
    affiliated_base_number


from {{ source('staging', 'fhv_tripdata_dbt_sources') }}

where dispatching_base_num is not null
