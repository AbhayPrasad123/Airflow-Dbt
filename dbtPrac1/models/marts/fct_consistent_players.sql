{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('stg_ipl_stats') }}
    where year between 2008 and 2024

),

total_seasons as (

    select count(distinct year) as total_years
    from base

),

player_seasons as (
    select
        playername,
        count(distinct year) as seasons_played
    from base
    group by playername

),

players_all_seasons as (

    select p.playername
    from player_seasons p
    cross join total_seasons t
    where p.seasons_played = t.total_years

)

select b.*
from base b
join players_all_seasons p
on b.playername = p.playername