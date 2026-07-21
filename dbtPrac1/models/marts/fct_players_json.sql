{{ config(materialized='table') }}

select 
    playername,
    year,
    runs_scored,
    matches_battled,
    strikerate,
    load_timestamp
from {{ ref('stg_ipl_json') }}