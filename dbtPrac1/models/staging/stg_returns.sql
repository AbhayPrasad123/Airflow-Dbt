{{ config(materialized='view') }}

SELECT
order_id,
product_id,
return_reason

FROM {{ source('default','returns') }}