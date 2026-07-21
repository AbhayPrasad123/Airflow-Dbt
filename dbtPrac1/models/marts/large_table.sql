{{ config(materialized='table') }}

select id, rand() as value
from (select explode(sequence(1, 1000000)) as id)