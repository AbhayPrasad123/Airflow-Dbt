SELECT *
FROM {{ ref('stg_employee') }}
WHERE age <= 0