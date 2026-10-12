{{ config(
    materialized='table'
) }}

WITH dates AS (

    SELECT
        DATEADD(
            DAY,
            SEQ4(),
            TO_DATE('2020-01-01')
        ) AS CALENDAR_DATE

    FROM TABLE(
        GENERATOR(ROWCOUNT => 10000)
    )
)

SELECT

    TO_NUMBER(
        TO_CHAR(CALENDAR_DATE, 'YYYYMMDD')
    ) AS DATE_SK,

    CALENDAR_DATE,

    YEAR(CALENDAR_DATE) AS YEAR,

    QUARTER(CALENDAR_DATE) AS QUARTER,

    MONTH(CALENDAR_DATE) AS MONTH,

    TO_CHAR(
        CALENDAR_DATE,
        'MMMM'
    ) AS MONTH_NAME,

    DAY(CALENDAR_DATE) AS DAY,

    DAYOFWEEK(CALENDAR_DATE) AS DAY_OF_WEEK,

    TO_CHAR(
        CALENDAR_DATE,
        'DY'
    ) AS DAY_NAME

FROM dates