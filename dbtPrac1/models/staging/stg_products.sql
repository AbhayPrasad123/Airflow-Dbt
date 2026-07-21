{{ config(materialized='view') }}

SELECT
product_id,
product_name,
category,
cost

FROM {{ source('default','products') }}