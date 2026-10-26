%sql

CREATE OR REPLACE TEMP VIEW employee_scd2_rebuilt AS
WITH combined AS (
    SELECT employee_id, employee_name, department, salary, city,
           start_date AS effective_date
    FROM workspace.default_gold.employee_scd2

    UNION ALL

    SELECT employee_id, employee_name, department, salary, city,
           TO_DATE(updated_at) AS effective_date
    FROM workspace.bronze.employee_source
),
deduplicated AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY employee_id, effective_date
                   ORDER BY effective_date DESC
               ) AS rn
        FROM combined
    )
    WHERE rn = 1
),
hashed AS (
    SELECT *,
           MD5(CONCAT_WS('|',
               COALESCE(employee_name, ''),
               COALESCE(department, ''),
               COALESCE(CAST(salary AS STRING), ''),
               COALESCE(city, '')
           )) AS row_hash
    FROM deduplicated
),
lagged AS (
    SELECT *,
           LAG(row_hash) OVER (
               PARTITION BY employee_id
               ORDER BY effective_date
           ) AS prev_hash
    FROM hashed
),
filtered AS (
    SELECT *
    FROM lagged
    WHERE prev_hash IS NULL OR row_hash <> prev_hash
),
versioned AS (
    SELECT employee_id, employee_name, department, salary, city,
           effective_date AS start_date,
           DATE_SUB(
               LEAD(effective_date) OVER (
                   PARTITION BY employee_id
                   ORDER BY effective_date
               ), 1
           ) AS end_date
    FROM filtered
)
SELECT employee_id, employee_name, department, salary, city, start_date,
       COALESCE(end_date, DATE('9999-12-31')) AS end_date,
       CASE WHEN end_date IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM versioned;

-- Check the result before writing it to the target
SELECT *
FROM employee_scd2_rebuilt
ORDER BY employee_id, start_date;
