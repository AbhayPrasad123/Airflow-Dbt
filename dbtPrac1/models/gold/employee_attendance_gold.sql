{{ config(
    materialized='view'
) }}

SELECT
    employee_id,
    work_date,

    first_entry_time,
    last_exit_time,

    total_sessions,
    total_hours,

    incomplete_flag,
    invalid_hours_flag,

    CASE
        WHEN incomplete_flag = TRUE
             OR invalid_hours_flag = TRUE
        THEN 'ABSENT'

        WHEN total_hours >= 8
        THEN 'PRESENT'

        WHEN total_hours >= 4
        THEN 'HALF DAY'

        ELSE 'ABSENT'
    END AS attendance_status,

    CASE
        WHEN incomplete_flag = TRUE
        THEN 'Missing punch-out swipe'

        WHEN invalid_hours_flag = TRUE
        THEN 'Invalid working hours'

        WHEN total_sessions > 3
        THEN 'Too many sessions'

        WHEN total_hours < 4
        THEN 'Insufficient hours'

        ELSE 'Normal day'
    END AS remark,

    HOUR(first_entry_time) AS entry_hour,

    CASE
        WHEN HOUR(first_entry_time) > 10
        THEN 'LATE_LOGIN'
        ELSE 'ON_TIME'
    END AS login_behavior

FROM {{ ref('attendance_silver') }}