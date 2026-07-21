{{ config(materialized='view') }}

SELECT
    id,
    name,
    org,
    age,
    effective_date
FROM {{ source('default', 'employee_source') }}
WHERE year is not null