{{
    config(
        materialized='view'
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
)
SELECT 
    ROW_NUMBER() OVER () as trip_id,
    vendor_id,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,
    dim_time.trip_time_id,
    dim_distance.distance_id,
    data_handling_id,
    payment_method_id,
    trip_type_id,

    combined_trips.passenger_count,
    combined_trips.fare_amount,
    combined_trips.tip_amount,
    combined_trips.tolls_amount,
    combined_trips.extra_fee,
    combined_trips.ehail_fee,
    combined_trips.mta_tax,
    combined_trips.improvement_surcharge,
    combined_trips.congestion_surcharge,
    combined_trips.total_amount


FROM combined_trips

LEFT JOIN {{ ref('dim_time') }} dim_time
    ON combined_trips.pickup_datetime = dim_time.pickup_datetime 
    AND combined_trips.dropoff_datetime = dim_time.dropoff_datetime 

LEFT JOIN {{ ref('dim_distance') }} dim_distance
    ON combined_trips.trip_distance_in_miles = dim_distance.trip_distance_in_miles