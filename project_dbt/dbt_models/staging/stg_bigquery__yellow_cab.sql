{{
    config(
        materialized='view'
    )
}}

with yellow_cab_data as (
    select

        -- identifiers
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', "'yellow'"]) }} as trip_id,
        CASE
            WHEN VendorID < 1 THEN NULL
            WHEN VendorID > 2 THEN NULL
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

        PULocationID as pickup_location_id,
        DOLocationID as dropoff_location_id,

        -- timestamps
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,

        -- trip info
        {{ null_zero_values('trip_distance') }} AS trip_distance_in_miles,
        {{ null_zero_values('ROUND(trip_distance*1.60934, 2)') }} AS trip_distance_in_km,

        TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) as trip_duration_minutes,

        CAST({{ null_negative_and_zero_values('passenger_count') }} as INTEGER) AS passenger_count,

        CASE
            WHEN store_and_fwd_flag='Y' THEN TRUE
            WHEN store_and_fwd_flag='N' THEN FALSE
            ELSE NULL 
        END AS record_stored_before_send,

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
        {{ null_negative_values('mta_tax') }} AS mta_tax,
        {{ null_negative_values('improvement_surcharge') }} AS improvement_surcharge,
        {{ null_negative_values('congestion_surcharge') }} AS congestion_surcharge,

        {{ null_negative_and_zero_values('total_amount') }} AS total_amount,

    FROM {{ source('raw', 'raw_yellow_cab_data') }}
),
yellow_no_dups as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY pickup_datetime) AS dup_rank
    FROM yellow_cab_data

)
SELECT 
    trip_id,
    vendor_id,
    vendor_description,
    ratecode_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    trip_distance_in_miles,
    trip_distance_in_km,
    trip_duration_minutes,
    passenger_count,
    record_stored_before_send,
    payment_type,
    payment_type_description,
    fare_amount,
    tip_amount,
    tolls_amount,
    extra_fee,
    mta_tax,
    improvement_surcharge,
    congestion_surcharge,
    total_amount
FROM yellow_no_dups
WHERE dup_rank = 1