%sql

MERGE INTO workspace.default_gold.employee_scd2 AS target
USING (
    -- New employee or changed version: insert this row
    SELECT
        CAST(NULL AS INT) AS merge_key,
        src.employee_id,
        src.employee_name,
        src.department,
        src.salary,
        src.city,
        TO_DATE(src.updated_at) AS start_date,
        DATE('9999-12-31') AS end_date,
        TRUE AS is_current
    FROM workspace.bronze.employee_source AS src
    LEFT JOIN workspace.default_gold.employee_scd2 AS tgt
        ON src.employee_id = tgt.employee_id
       AND tgt.is_current = TRUE
    WHERE tgt.employee_id IS NULL
       OR NOT (
            src.employee_name <=> tgt.employee_name
        AND src.department    <=> tgt.department
        AND src.salary        <=> tgt.salary
        AND src.city          <=> tgt.city
       )

    UNION ALL

    -- Changed employee: expire the old current version
    SELECT
        src.employee_id AS merge_key,
        tgt.employee_id,
        tgt.employee_name,
        tgt.department,
        tgt.salary,
        tgt.city,
        tgt.start_date,
        DATE_SUB(TO_DATE(src.updated_at), 1) AS end_date,
        FALSE AS is_current
    FROM workspace.bronze.employee_source AS src
    INNER JOIN workspace.default_gold.employee_scd2 AS tgt
        ON src.employee_id = tgt.employee_id
       AND tgt.is_current = TRUE
    WHERE NOT (
            src.employee_name <=> tgt.employee_name
        AND src.department    <=> tgt.department
        AND src.salary        <=> tgt.salary
        AND src.city          <=> tgt.city
       )
) AS staged
ON target.employee_id = staged.merge_key
AND target.is_current = TRUE

WHEN MATCHED THEN
    UPDATE SET
        target.end_date = staged.end_date,
        target.is_current = FALSE

WHEN NOT MATCHED THEN
    INSERT (
        employee_id, employee_name, department, salary, city,
        start_date, end_date, is_current
    )
    VALUES (
        staged.employee_id, staged.employee_name, staged.department,
        staged.salary, staged.city, staged.start_date, staged.end_date,
        staged.is_current
    );
