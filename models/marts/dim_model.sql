{{ config(
    materialized='table'
) }}

WITH model_source AS (

    SELECT
        MODEL,
        MODEL_YEAR,
        UPDATED_AT

    FROM {{ ref('stg_vehicles_valid') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY MODEL, MODEL_YEAR
        ORDER BY UPDATED_AT DESC
    ) = 1
)

SELECT

    SHA2(
        CONCAT(
            COALESCE(MODEL, ''),
            '||',
            COALESCE(TO_VARCHAR(MODEL_YEAR), '')
        ),
        256
    ) AS MODEL_SK,

    MODEL,
    MODEL_YEAR

FROM model_source