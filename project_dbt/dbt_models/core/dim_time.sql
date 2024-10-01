{{
    config(
        materialized='table'
    )
}}

with combined_trips as (
    SELECT
        *,
        'green' as taxi_type,
    FROM {{ source('staging', 'stg_bigquery__green_cab') }}

    UNION ALL

    SELECT 
        *,
        'yellow' as taxi_type,
    FROM {{ source('staging', 'stg_bigquery__yellow_cab') }}
),
unique_time as (
    SELECT DISTINCT 
    pickup_datetime, 
    dropoff_datetime, 
    trip_duration_minutes
FROM combined_trips
)
SELECT
    ROW_NUMBER() OVER () as trip_time_id,
    pickup_datetime, 
    dropoff_datetime, 
    trip_duration_minutes
FROM unique_time