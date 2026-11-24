%sql

MERGE INTO workspace.default_gold.employee_scd2 AS target

USING workspace.bronze.employee_source AS source

ON target.employee_id = source.employee_id

WHEN MATCHED THEN
    UPDATE SET
        target.employee_name = source.employee_name,
        target.department    = source.department,
        target.salary        = source.salary,
        target.city          = source.city

WHEN NOT MATCHED THEN
    INSERT (
        employee_id,
        employee_name,
        department,
        salary,
        city
    )
    VALUES (
        source.employee_id,
        source.employee_name,
        source.department,
        source.salary,
        source.city
    );