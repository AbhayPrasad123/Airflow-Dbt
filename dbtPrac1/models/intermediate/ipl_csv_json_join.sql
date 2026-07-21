{{ config(materialized='view') }}

with csv_data as (

    select
        playername,
        year,
        runs_scored,
        century,
        batting_avg,
        matches_battled,
        strikerate,
        load_timestamp
    from {{ ref('stg_ipl_stats') }}

),

json_data as (

    select
        playername,
        year
    from {{ ref('stg_ipl_json') }}

)

select
    a.playername,
    a.year,
    a.runs_scored,
    a.century,
    a.batting_avg,
    a.matches_battled,
    a.strikerate,
    a.load_timestamp
from csv_data a
left join json_data b
on a.playername = b.playername
and a.year = b.year