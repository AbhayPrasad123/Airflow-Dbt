{{ config(materialized='view') }}


select
playername,
year,
runs_scored,
matches_battled,
strikerate,
load_timestamp
from {{ source('default','ipl_raw2') }}
where year is not null