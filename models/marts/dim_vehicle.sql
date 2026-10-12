{{ config(
    materialized='incremental',
    unique_key='VEHICLE_SK',
    incremental_strategy='merge'
) }}

WITH source_dedup AS (

    SELECT
        VIN,
        MODEL,
        MODEL_YEAR,
        FUEL_TYPE,
        PLANT_CODE,
        UPDATED_AT,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID

    FROM {{ ref('stg_vehicles_valid') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY VIN
        ORDER BY
            UPDATED_AT DESC,
            INGESTION_TIMESTAMP DESC,
            SOURCE_FILE_NAME DESC,
            RUN_ID DESC
    ) = 1
),

incoming AS (

    SELECT
        VIN,
        MODEL,
        MODEL_YEAR,
        FUEL_TYPE,
        PLANT_CODE,
        UPDATED_AT,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID,

        SHA2(
            CONCAT_WS(
                '||',
                COALESCE(MODEL, ''),
                COALESCE(TO_VARCHAR(MODEL_YEAR), ''),
                COALESCE(FUEL_TYPE, ''),
                COALESCE(PLANT_CODE, '')
            ),
            256
        ) AS ATTR_HASH

    FROM source_dedup
)

{% if is_incremental() %}

,

current_dim AS (

    SELECT *
    FROM {{ this }}
    WHERE CURRENT_FLAG = 'Y'
),

changes AS (

    SELECT
        i.*,

        d.VEHICLE_SK AS OLD_VEHICLE_SK,
        d.ATTR_HASH AS OLD_ATTR_HASH,
        d.MODEL AS OLD_MODEL,
        d.MODEL_YEAR AS OLD_MODEL_YEAR,
        d.FUEL_TYPE AS OLD_FUEL_TYPE,
        d.PLANT_CODE AS OLD_PLANT_CODE,
        d.EFFECTIVE_FROM AS OLD_EFFECTIVE_FROM,
        d.SOURCE_SYSTEM AS OLD_SOURCE_SYSTEM,
        d.SOURCE_FILE_NAME AS OLD_SOURCE_FILE_NAME,
        d.INGESTION_TIMESTAMP AS OLD_INGESTION_TIMESTAMP,
        d.RUN_ID AS OLD_RUN_ID,

        CASE
            WHEN d.VIN IS NULL
                THEN i.UPDATED_AT
            WHEN i.UPDATED_AT > d.EFFECTIVE_FROM
                THEN i.UPDATED_AT
            ELSE i.INGESTION_TIMESTAMP
        END AS NEW_VERSION_TS

    FROM incoming i

    LEFT JOIN current_dim d
        ON i.VIN = d.VIN

    WHERE d.VIN IS NULL
       OR d.ATTR_HASH <> i.ATTR_HASH
),

expired_rows AS (

    SELECT

        OLD_VEHICLE_SK AS VEHICLE_SK,

        VIN,
        OLD_MODEL AS MODEL,
        OLD_MODEL_YEAR AS MODEL_YEAR,
        OLD_FUEL_TYPE AS FUEL_TYPE,
        OLD_PLANT_CODE AS PLANT_CODE,
        OLD_ATTR_HASH AS ATTR_HASH,

        OLD_EFFECTIVE_FROM AS EFFECTIVE_FROM,
        NEW_VERSION_TS AS EFFECTIVE_TO,

        'N' AS CURRENT_FLAG,

        OLD_SOURCE_SYSTEM AS SOURCE_SYSTEM,
        OLD_SOURCE_FILE_NAME AS SOURCE_FILE_NAME,
        OLD_INGESTION_TIMESTAMP AS INGESTION_TIMESTAMP,
        OLD_RUN_ID AS RUN_ID,

        CURRENT_TIMESTAMP() AS DIM_LOAD_TIMESTAMP

    FROM changes
    WHERE OLD_VEHICLE_SK IS NOT NULL
),

new_rows AS (

    SELECT

        SHA2(
            CONCAT(
                VIN,
                '||',
                TO_VARCHAR(
                    NEW_VERSION_TS,
                    'YYYY-MM-DD HH24:MI:SS.FF9'
                )
            ),
            256
        ) AS VEHICLE_SK,

        VIN,
        MODEL,
        MODEL_YEAR,
        FUEL_TYPE,
        PLANT_CODE,
        ATTR_HASH,

        NEW_VERSION_TS AS EFFECTIVE_FROM,

        TO_TIMESTAMP_NTZ(
            '9999-12-31 23:59:59.999999999'
        ) AS EFFECTIVE_TO,

        'Y' AS CURRENT_FLAG,

        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID,

        CURRENT_TIMESTAMP() AS DIM_LOAD_TIMESTAMP

    FROM changes
)

SELECT * FROM expired_rows

UNION ALL

SELECT * FROM new_rows

{% else %}

SELECT

    SHA2(
        CONCAT(
            VIN,
            '||',
            TO_VARCHAR(
                UPDATED_AT,
                'YYYY-MM-DD HH24:MI:SS.FF9'
            )
        ),
        256
    ) AS VEHICLE_SK,

    VIN,
    MODEL,
    MODEL_YEAR,
    FUEL_TYPE,
    PLANT_CODE,
    ATTR_HASH,

    UPDATED_AT AS EFFECTIVE_FROM,

    TO_TIMESTAMP_NTZ(
        '9999-12-31 23:59:59.999999999'
    ) AS EFFECTIVE_TO,

    'Y' AS CURRENT_FLAG,

    SOURCE_SYSTEM,
    SOURCE_FILE_NAME,
    INGESTION_TIMESTAMP,
    RUN_ID,

    CURRENT_TIMESTAMP() AS DIM_LOAD_TIMESTAMP

FROM incoming

{% endif %}