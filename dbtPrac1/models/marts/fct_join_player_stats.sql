{{ config(
    materialized='incremental',
    unique_key='player_year'
) }}

with cte as(
select
    playername,
    year,
    runs_scored,
    matches_battled,
    strikerate,
    concat(playername,'_',year) as player_year,
    load_timestamp
from {{ ref('ipl_csv_json_join') }}

{% if is_incremental() %}

where year >
      (select max(year) from {{ this }})

{% endif %}
)

select * from cte