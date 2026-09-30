MERGE INTO target_employee AS t

USING (
    
    SELECT
        id,
        name,
        org,
        age,
        operation,
        change_time

    FROM (
        
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY change_time DESC
            ) AS rn

        FROM employee_cdc

    ) x

    WHERE rn = 1

) AS s

ON t.id = s.id


WHEN MATCHED
     AND s.operation = 'U'

THEN UPDATE SET

    t.name = s.name,
    t.org = s.org,
    t.age = s.age


WHEN MATCHED
     AND s.operation = 'D'

THEN DELETE


WHEN NOT MATCHED
     AND s.operation = 'I'

THEN INSERT (
    id,
    name,
    org,
    age
)

VALUES (
    s.id,
    s.name,
    s.org,
    s.age
);