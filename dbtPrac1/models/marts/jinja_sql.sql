{{ config(materialized='table') }}

SELECT *
FROM {{ ref('stg_orders') }}
WHERE updated_at >= '{{ var("start_date") }}'