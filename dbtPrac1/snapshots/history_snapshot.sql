{% snapshot history_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',  
      check_cols=['org', 'age', 'year'],
    )
}}

-- query to fetch form table
SELECT * FROM workspace.default.history_abhay

{% endsnapshot %}