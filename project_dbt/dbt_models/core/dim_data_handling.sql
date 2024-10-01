{{
    config(
        materialized='table'
    )
}}
SELECT
    1 AS data_handling_id,
    TRUE AS record_stored_before_send
UNION ALL
SELECT
    2 AS data_handling_id,
    FALSE AS record_stored_before_send
