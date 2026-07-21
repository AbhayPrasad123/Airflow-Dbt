{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    order_timestamp,
    order_status,

    item.product_id,
    item.quantity,
    item.price,
    item.discount,

    STRUCT(
        shipping.city,
        shipping.state,
        shipping.country    
    ) AS shipping_info

FROM {{ source('default','orders3') }}

LATERAL VIEW explode(items) t AS item