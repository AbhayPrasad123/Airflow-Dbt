{{ config(materialized='view') }}

select *
from default.orders
