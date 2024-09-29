{{
    config(
        materialized='view'
    )
}}SELECT
    1 AS ratecode_id,
    'Standard rate' AS ratecode_description
UNION ALL
SELECT
    2 AS vendor_id,
    'JFK' AS ratecode_description
UNION ALL
SELECT
    3 AS vendor_id,
    'Newark' AS ratecode_description
    UNION ALL
SELECT
    4 AS vendor_id,
    'Nassau or Westchester' AS ratecode_description
    UNION ALL
SELECT
    5 AS vendor_id,
    'Negotiated Fare' AS ratecode_description
    UNION ALL
SELECT
    6 AS vendor_id,
    'Group Ride' AS ratecode_description