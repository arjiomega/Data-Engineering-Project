{{
    config(
        materialized='view'
    )
}}
SELECT
    1 AS trip_type_id,
    'Street-hail' AS trip_type_description
UNION ALL
SELECT
    2 AS trip_type_id,
    'Dispatch' AS trip_type_description
