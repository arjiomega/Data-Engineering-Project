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
unique_distance as (
    SELECT DISTINCT trip_distance_in_miles, trip_distance_in_km
    FROM combined_trips
)
SELECT
    ROW_NUMBER() OVER () as distance_id,
    trip_distance_in_miles,
    trip_distance_in_km
FROM unique_distance
