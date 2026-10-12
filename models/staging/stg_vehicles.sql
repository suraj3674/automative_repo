WITH technical_validation AS (

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

        CASE
            WHEN VIN IS NULL THEN 'FAILED'
            WHEN MODEL IS NULL THEN 'FAILED'
            WHEN MODEL_YEAR IS NULL THEN 'FAILED'
            WHEN UPDATED_AT IS NULL THEN 'FAILED'
            ELSE 'PASSED'
        END AS TECHNICAL_VALIDATION

    FROM {{ source('raw', 'VEHICLES') }}
),

standardized AS (

    SELECT
        NULLIF(TRIM(VIN), '') AS VIN,
        NULLIF(TRIM(MODEL), '') AS MODEL,
        MODEL_YEAR,
        UPPER(TRIM(FUEL_TYPE)) AS FUEL_TYPE,
        UPPER(TRIM(PLANT_CODE)) AS PLANT_CODE,
        UPDATED_AT,

        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID,

        TECHNICAL_VALIDATION

    FROM technical_validation
),

dq_validation AS (

    SELECT
        *,

        CASE
            WHEN VIN IS NULL THEN 'FAILED'
            ELSE 'PASSED'
        END AS VIN_VALIDATION,

        CASE
            WHEN MODEL IS NULL THEN 'FAILED'
            ELSE 'PASSED'
        END AS MODEL_VALIDATION,

        CASE
            WHEN MODEL_YEAR BETWEEN 1990 AND YEAR(CURRENT_DATE()) + 1
            THEN 'PASSED'
            ELSE 'FAILED'
        END AS MODEL_YEAR_VALIDATION,

        CASE
            WHEN FUEL_TYPE IN (
                'PETROL',
                'DIESEL',
                'ELECTRIC',
                'HYBRID'
            )
            THEN 'PASSED'
            ELSE 'FAILED'
        END AS FUEL_TYPE_VALIDATION,

        CASE
            WHEN PLANT_CODE IS NULL THEN 'FAILED'
            ELSE 'PASSED'
        END AS PLANT_CODE_VALIDATION

    FROM standardized
)

SELECT
    *,

    CASE
        WHEN TECHNICAL_VALIDATION = 'FAILED'
          OR VIN_VALIDATION = 'FAILED'
          OR MODEL_VALIDATION = 'FAILED'
          OR MODEL_YEAR_VALIDATION = 'FAILED'
          OR FUEL_TYPE_VALIDATION = 'FAILED'
          OR PLANT_CODE_VALIDATION = 'FAILED'
        THEN 'INVALID'
        ELSE 'VALID'
    END AS RECORD_STATUS,

    CASE
        WHEN TECHNICAL_VALIDATION = 'FAILED'
            THEN 'TECHNICAL_VALIDATION_FAILED'

        WHEN VIN_VALIDATION = 'FAILED'
            THEN 'VIN_MISSING'

        WHEN MODEL_VALIDATION = 'FAILED'
            THEN 'MODEL_MISSING'

        WHEN MODEL_YEAR_VALIDATION = 'FAILED'
            THEN 'INVALID_MODEL_YEAR'

        WHEN FUEL_TYPE_VALIDATION = 'FAILED'
            THEN 'INVALID_FUEL_TYPE'

        WHEN PLANT_CODE_VALIDATION = 'FAILED'
            THEN 'PLANT_CODE_MISSING'

        ELSE NULL
    END AS ERROR_REASON,

    CASE
        WHEN TECHNICAL_VALIDATION = 'FAILED'
            THEN 'TECHNICAL_FIELD_CHECK'

        WHEN VIN_VALIDATION = 'FAILED'
            THEN 'VIN_MANDATORY_CHECK'

        WHEN MODEL_VALIDATION = 'FAILED'
            THEN 'MODEL_MANDATORY_CHECK'

        WHEN MODEL_YEAR_VALIDATION = 'FAILED'
            THEN 'MODEL_YEAR_RANGE_CHECK'

        WHEN FUEL_TYPE_VALIDATION = 'FAILED'
            THEN 'FUEL_TYPE_DOMAIN_CHECK'

        WHEN PLANT_CODE_VALIDATION = 'FAILED'
            THEN 'PLANT_CODE_MANDATORY_CHECK'

        ELSE NULL
    END AS VALIDATION_RULE

FROM dq_validation


-- STAGING
--  ├─ Technical/schema checks
--  ├─ Standardization / cleansing
--  ├─ Basic DQ/business-rule checks
--  ├─ Identify VALID records
--  └─ Identify INVALID records → Quarantine