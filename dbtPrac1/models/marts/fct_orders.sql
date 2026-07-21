{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
  order_id,
  updated_at,
  amount
from {{ ref('stg_orders') }}
{{ incremental_where('updated_at') }}