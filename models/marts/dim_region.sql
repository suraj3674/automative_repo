{{ config(
    materialized='table'
) }}

SELECT DISTINCT

    SHA2(
        UPPER(TRIM(REGION)),
        256
    ) AS REGION_SK,

    UPPER(TRIM(REGION)) AS REGION

FROM {{ ref('stg_dealers_valid') }}

WHERE REGION IS NOT NULL