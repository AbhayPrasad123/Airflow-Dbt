{% macro scd2_lag_lead(input_relation) %}

, hashed AS (

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

    FROM {{ input_relation }}

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

        COALESCE(
            LEAD(effective_date) OVER (
                PARTITION BY id
                ORDER BY effective_date
            ),
            DATE '9999-12-31'
        ) AS valid_to

    FROM filtered
)

SELECT *
FROM final

{% endmacro %}