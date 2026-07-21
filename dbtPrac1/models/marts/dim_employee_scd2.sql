{{ config(
    materialized='incremental',
    unique_key='id'
) }}

WITH new_data AS (

    SELECT *
    FROM {{ ref('stg_employee') }}

    {% if is_incremental() %}
        WHERE effective_date >
              (SELECT COALESCE(MAX(valid_from),'1900-01-01') FROM {{ this }})
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
        WHERE is_current = true

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
),

hashed AS (

    SELECT
        id,
        name,
        org,
        age,
        effective_date,

        MD5(CONCAT_WS('|',
        COALESCE(name,''),
        COALESCE(org,''),
        COALESCE(age,0)
    )) AS row_hash

    FROM combined
),

lagged AS (

    SELECT *,
        LAG(row_hash) OVER (
            PARTITION BY id
            ORDER BY effective_date
        ) AS prev_hash
    FROM hashed
),

filtered AS (

    SELECT *
    FROM lagged
    WHERE prev_hash IS NULL
       OR row_hash <> prev_hash
),

final AS (

    SELECT
        id,
        name,
        org,
        age,
        effective_date AS valid_from,

        LEAD(effective_date) OVER (
            PARTITION BY id
            ORDER BY effective_date
        ) AS valid_to

    FROM filtered
)

SELECT
    id,
    name,
    org,
    age,
    valid_from,
    valid_to,
    CASE WHEN valid_to IS NULL THEN true ELSE false END AS is_current
FROM final



