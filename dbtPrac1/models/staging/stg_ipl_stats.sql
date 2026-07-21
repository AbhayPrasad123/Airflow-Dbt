{{ config(materialized='view') }}

select
    year,
    playername,
    matches_battled,
    runs_scored,
    highest_score,
    batting_avg,
    strikerate,
    century,
    half_century,
    load_timestamp
from {{ source('default','ipl_raw') }}
where year is not null