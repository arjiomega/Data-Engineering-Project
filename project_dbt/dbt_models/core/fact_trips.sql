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
)
SELECT 
    ROW_NUMBER() OVER () as trip_id,
    combined_trips.vendor_id,
    combined_trips.ratecode_id,
    combined_trips.pickup_location_id,
    combined_trips.dropoff_location_id,
    dim_time.trip_time_id,
    dim_distance.distance_id,
    dim_data_handling.data_handling_id,
    combined_trips.payment_type as payment_method_id,
    combined_trips.trip_type as trip_type_id,

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

LEFT JOIN {{ ref('dim_location') }} dim_location
    ON combined_trips.pickup_location_id = dim_location.location_id

LEFT JOIN {{ ref('dim_data_handling') }} dim_data_handling
    ON combined_trips.record_stored_before_send = dim_data_handling.record_stored_before_send

-- add a test to see if location_ids are in dim_location