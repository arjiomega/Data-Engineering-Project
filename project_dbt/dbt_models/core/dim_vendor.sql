{{
    config(
        materialized='table'
    )
}}
SELECT
    1 AS vendor_id,
    'Creative Mobile Technologies, LLC' AS vendor_description
UNION ALL
SELECT
    2 AS vendor_id,
    'VeriFone Inc.' AS vendor_description
