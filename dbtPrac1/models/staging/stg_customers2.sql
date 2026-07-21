{{ config(materialized='view') }}

SELECT
customer_id,
first_name,
last_name,
signup_date,

CASE WHEN array_contains(customer_segments,'vip') THEN 1 ELSE 0 END AS is_vip,
CASE WHEN array_contains(customer_segments,'newsletter') THEN 1 ELSE 0 END AS is_newsletter,
CASE WHEN array_contains(customer_segments,'loyal') THEN 1 ELSE 0 END AS is_loyal

FROM {{ source('default','customers') }}