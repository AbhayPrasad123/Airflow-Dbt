{% snapshot player_year_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key=['playername','year'],
        strategy='check',
        check_cols=['runs_scored','strikerate']
    )
}}

select
    playername,
    year,
    runs_scored,
    matches_battled,
    strikerate,
    load_timestamp
from {{ ref('fct_join_player_stats') }}

{% endsnapshot %}