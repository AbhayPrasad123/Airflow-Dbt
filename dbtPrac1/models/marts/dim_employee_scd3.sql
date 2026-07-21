{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH new_data AS (

    SELECT *
    FROM {{ ref('stg_employee') }}

    {% if is_incremental() %}
        WHERE effective_date >
              (SELECT COALESCE(MAX(valid_from), DATE '1900-01-01')
               FROM {{ this }})
    {% endif %}
),

current_data AS (

    {% if is_incremental() %}

        SELECT
            id,
            name,
            org,
            age,
            valid_from AS effective_date
        FROM {{ this }}
        WHERE valid_to = DATE '9999-12-31'

    {% else %}

        SELECT
            NULL AS id,
            NULL AS name,
            NULL AS org,
            NULL AS age,
            NULL AS effective_date
        WHERE FALSE

    {% endif %}
),

combined AS (
    SELECT * FROM new_data
    UNION ALL
    SELECT * FROM current_data
)

{{ scd2_lag_lead('combined') }}