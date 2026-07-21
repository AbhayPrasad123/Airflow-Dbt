{{ config(materialized='view') }}

select *
from default.users
