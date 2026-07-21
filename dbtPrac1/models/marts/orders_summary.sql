{{ config(materialized='table') }}

select
  date(updated_at) as order_date,
  count(order_id) as total_orders,
  sum(amount) as total_amount
from {{ ref('stg_orders') }}
group by date(updated_at)
