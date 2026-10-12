{{ config(
    materialized='incremental',
    unique_key='DEALER_SK',
    incremental_strategy='merge'
) }}

WITH source_dedup AS (

    SELECT
        DEALER_ID,
        DEALER_NAME,
        CITY,
        STATE,
        REGION,
        UPDATED_AT,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID

    FROM {{ ref('stg_dealers_valid') }}

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DEALER_ID
        ORDER BY
            UPDATED_AT DESC,
            INGESTION_TIMESTAMP DESC,
            SOURCE_FILE_NAME DESC,
            RUN_ID DESC
    ) = 1
),

incoming AS (

    SELECT

        DEALER_ID,
        DEALER_NAME,
        CITY,
        STATE,
        REGION,
        UPDATED_AT,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID,

        SHA2(
            CONCAT_WS(
                '||',
                COALESCE(DEALER_NAME, ''),
                COALESCE(CITY, ''),
                COALESCE(STATE, ''),
                COALESCE(REGION, '')
            ),
            256
        ) AS ATTR_HASH

    FROM source_dedup
)

{% if is_incremental() %}

,

current_dim AS (

    SELECT
        DEALER_SK,
        DEALER_ID,
        DEALER_NAME,
        CITY,
        STATE,
        REGION,
        ATTR_HASH,
        EFFECTIVE_FROM,
        EFFECTIVE_TO,
        CURRENT_FLAG,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID

    FROM {{ this }}

    WHERE CURRENT_FLAG = 'Y'
),

changes AS (

    SELECT

        i.DEALER_ID,
        i.DEALER_NAME,
        i.CITY,
        i.STATE,
        i.REGION,
        i.ATTR_HASH,
        i.UPDATED_AT,
        i.SOURCE_SYSTEM,
        i.SOURCE_FILE_NAME,
        i.INGESTION_TIMESTAMP,
        i.RUN_ID,

        d.DEALER_SK AS OLD_DEALER_SK,
        d.DEALER_NAME AS OLD_DEALER_NAME,
        d.CITY AS OLD_CITY,
        d.STATE AS OLD_STATE,
        d.REGION AS OLD_REGION,
        d.ATTR_HASH AS OLD_ATTR_HASH,
        d.EFFECTIVE_FROM AS OLD_EFFECTIVE_FROM,
        d.SOURCE_SYSTEM AS OLD_SOURCE_SYSTEM,
        d.SOURCE_FILE_NAME AS OLD_SOURCE_FILE_NAME,
        d.INGESTION_TIMESTAMP AS OLD_INGESTION_TIMESTAMP,
        d.RUN_ID AS OLD_RUN_ID,

        CASE
            WHEN d.DEALER_ID IS NULL
                THEN i.UPDATED_AT

            WHEN i.UPDATED_AT > d.EFFECTIVE_FROM
                THEN i.UPDATED_AT

            ELSE i.INGESTION_TIMESTAMP
        END AS NEW_VERSION_TS

    FROM incoming i

    LEFT JOIN current_dim d
        ON i.DEALER_ID = d.DEALER_ID

    WHERE d.DEALER_ID IS NULL
       OR d.ATTR_HASH <> i.ATTR_HASH
),

expired_rows AS (

    SELECT

        OLD_DEALER_SK AS DEALER_SK,

        DEALER_ID,
        OLD_DEALER_NAME AS DEALER_NAME,
        OLD_CITY AS CITY,
        OLD_STATE AS STATE,
        OLD_REGION AS REGION,
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

    WHERE OLD_DEALER_SK IS NOT NULL
),

new_rows AS (

    SELECT

        SHA2(
            CONCAT(
                DEALER_ID,
                '||',
                TO_VARCHAR(
                    NEW_VERSION_TS,
                    'YYYY-MM-DD HH24:MI:SS.FF9'
                )
            ),
            256
        ) AS DEALER_SK,

        DEALER_ID,
        DEALER_NAME,
        CITY,
        STATE,
        REGION,
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

SELECT *
FROM expired_rows

UNION ALL

SELECT *
FROM new_rows

{% else %}

SELECT

    SHA2(
        CONCAT(
            DEALER_ID,
            '||',
            TO_VARCHAR(
                UPDATED_AT,
                'YYYY-MM-DD HH24:MI:SS.FF9'
            )
        ),
        256
    ) AS DEALER_SK,

    DEALER_ID,
    DEALER_NAME,
    CITY,
    STATE,
    REGION,
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