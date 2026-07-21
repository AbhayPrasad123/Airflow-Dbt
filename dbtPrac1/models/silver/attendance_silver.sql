{{ config(
    materialized='table'
) }}

WITH cleaned_logs AS (

    SELECT DISTINCT
        employee_id,
        TO_TIMESTAMP(event_time) AS event_ts,
        TO_DATE(event_time) AS work_date

    FROM {{ source('bronze', 'attendance_logs_raw') }}

),


non_holiday_logs AS (

    SELECT c.*
    FROM cleaned_logs c

    LEFT JOIN {{ source('bronze', 'holidays_raw') }} h
        ON c.work_date = TO_DATE(h.holiday_date)

    WHERE h.holiday_date IS NULL

),

ordered_swipes AS (

    SELECT
        employee_id,
        work_date,
        event_ts,

        ROW_NUMBER() OVER (
            PARTITION BY employee_id, work_date
            ORDER BY event_ts
        ) AS rn

    FROM non_holiday_logs

),

paired_sessions AS (

    SELECT
        in_swipe.employee_id,
        in_swipe.work_date,

        in_swipe.event_ts AS entry_time,
        out_swipe.event_ts AS exit_time,

        CASE
            WHEN out_swipe.event_ts IS NOT NULL
            THEN ROUND(
                (
                    UNIX_TIMESTAMP(out_swipe.event_ts)
                    - UNIX_TIMESTAMP(in_swipe.event_ts)
                ) / 3600.0,
                2
            )
            ELSE NULL
        END AS session_hours

    FROM ordered_swipes in_swipe

    LEFT JOIN ordered_swipes out_swipe
        ON in_swipe.employee_id = out_swipe.employee_id
       AND in_swipe.work_date = out_swipe.work_date
       AND out_swipe.rn = in_swipe.rn + 1

    WHERE MOD(in_swipe.rn, 2) = 1

),

daily_summary AS (

    SELECT
        employee_id,
        work_date,

        MIN(entry_time) AS first_entry_time,
        MAX(exit_time) AS last_exit_time,

        COUNT(*) AS total_sessions,

        ROUND(
            SUM(COALESCE(session_hours, 0)),
            2
        ) AS total_hours,

        CASE
            WHEN COUNT_IF(exit_time IS NULL) > 0
            THEN TRUE
            ELSE FALSE
        END AS incomplete_flag,

        CASE
            WHEN ROUND(
                SUM(COALESCE(session_hours, 0)),
                2
            ) > 16
            THEN TRUE
            ELSE FALSE
        END AS invalid_hours_flag

    FROM paired_sessions
    GROUP BY employee_id, work_date

)

SELECT *
FROM daily_summary