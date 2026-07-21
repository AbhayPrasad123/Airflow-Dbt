{{ config(materialized='table') }}

select id, 'small' as type
from (select explode(sequence(1, 100)) as id)