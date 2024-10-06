{{
    config(
        materialized='view'
    )
}}

with green_cab_data as (
    select

        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', "'green'"]) }} as trip_id,
        CASE
            WHEN VendorID < 1 OR VendorID > 2 THEN NULL
            ELSE VendorID
        END as vendor_id,
        CASE
            WHEN VendorID=1 THEN 'Creative Mobile Technologies, LLC'
            WHEN VendorID=2 THEN 'VeriFone Inc.'
            ELSE NULL
        END as vendor_description,

        CASE
            WHEN CAST(RatecodeID as INTEGER) < 1 OR CAST(RatecodeID as INTEGER) > 6 THEN NULL
            ELSE CAST(RatecodeID as INTEGER)
        END as ratecode_id,

        CASE
            WHEN CAST(RatecodeID as INTEGER) = 1 THEN 'Standard rate'
            WHEN CAST(RatecodeID as INTEGER) = 2 THEN 'JFK'
            WHEN CAST(RatecodeID as INTEGER) = 3 THEN 'Newark'
            WHEN CAST(RatecodeID as INTEGER) = 4 THEN 'Nassau or Westchester'
            WHEN CAST(RatecodeID as INTEGER) = 5 THEN 'Negotiated Fare'
            WHEN CAST(RatecodeID as INTEGER) = 6 THEN 'Group ride'
            ELSE NULL
        END as ratecode_description,

        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,

        -- timestamps
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,

        -- trip info
        {{ null_negative_and_zero_values('trip_distance') }} AS trip_distance_in_miles,
        {{ null_negative_and_zero_values('ROUND(trip_distance*1.60934, 2)') }} AS trip_distance_in_km,

        TIMESTAMP_DIFF(lpep_dropoff_datetime, lpep_pickup_datetime, MINUTE) as trip_duration_minutes,

        CAST({{ null_negative_and_zero_values('passenger_count') }} as INTEGER) AS passenger_count,

        CASE
            WHEN store_and_fwd_flag='Y' THEN TRUE
            WHEN store_and_fwd_flag='N' THEN FALSE
            ELSE NULL 
        END AS record_stored_before_send,

        CAST(trip_type as INTEGER) as trip_type,
        CASE
            WHEN trip_type=1 THEN 'Street-hail'
            WHEN trip_type=2 THEN 'Dispatch'
            ELSE NULL
        END AS trip_type_description,

        -- payment info
        {{ null_zero_values('CAST(payment_type as INTEGER)') }} as payment_type,
        CASE
            WHEN payment_type=1 THEN 'Credit Card'
            WHEN payment_type=2 THEN 'Cash'
            WHEN payment_type=3 THEN 'No Charge'
            WHEN payment_type=4 THEN 'Dispute'
            WHEN payment_type=5 THEN 'Unknown'
            WHEN payment_type=6 THEN 'Voided Trip'
            ELSE NULL
        END as payment_type_description,

        {{ null_negative_and_zero_values('fare_amount') }} AS fare_amount,

        {{ null_negative_values('tip_amount') }} AS tip_amount,
        {{ null_negative_values('tolls_amount') }} AS tolls_amount,
        {{ null_negative_values('extra') }} AS extra_fee,
        {{ null_negative_values('ehail_fee') }} AS ehail_fee,
        {{ null_negative_values('mta_tax') }} AS mta_tax,
        {{ null_negative_values('improvement_surcharge') }} AS improvement_surcharge,
        {{ null_negative_values('congestion_surcharge') }} AS congestion_surcharge,

        {{ null_negative_and_zero_values('total_amount') }} AS total_amount,

    FROM {{ source('raw', 'raw_green_cab_data') }}
),
green_no_dups as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY pickup_datetime) AS dup_rank
    FROM green_cab_data
)
SELECT 
    trip_id,
    vendor_id,
    vendor_description,
    ratecode_id,
    ratecode_description,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    trip_distance_in_miles,
    trip_distance_in_km,
    trip_duration_minutes,
    passenger_count,
    record_stored_before_send,
    trip_type,
    trip_type_description,
    payment_type,
    payment_type_description,
    fare_amount,
    tip_amount,
    tolls_amount,
    extra_fee,
    ehail_fee,
    mta_tax,
    improvement_surcharge,
    congestion_surcharge,
    total_amount
FROM green_no_dups
WHERE dup_rank = 1