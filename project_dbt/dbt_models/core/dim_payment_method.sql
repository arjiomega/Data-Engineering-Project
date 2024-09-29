{{
    config(
        materialized='view'
    )
}}
SELECT
    1 AS payment_method_id,
    'Credit Card' AS payment_method_description
UNION ALL
SELECT
    2 AS payment_method_id,
    'Cash' AS payment_method_description
UNION ALL
SELECT
    3 AS payment_method_id,
    'No Charge' AS payment_method_description
    UNION ALL
SELECT
    4 AS payment_method_id,
    'Dispute' AS payment_method_description
    UNION ALL
SELECT
    5 AS payment_method_id,
    'Unknown' AS payment_method_description
    UNION ALL
SELECT
    6 AS payment_method_id,
    'Voided Trip' AS payment_method_description