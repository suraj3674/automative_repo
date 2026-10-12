WITH sales_ranked AS (

    SELECT
        s.*,

        ROW_NUMBER() OVER (
            PARTITION BY SALE_ID
            ORDER BY
                UPDATED_AT DESC,
                INGESTION_TIMESTAMP DESC,
                SOURCE_FILE_NAME DESC,
                RUN_ID DESC
        ) AS RECORD_RANK

    FROM {{ ref('stg_vehicle_sales_valid') }} s
),

sales AS (

    SELECT
        SALE_ID,
        VIN,
        DEALER_ID,
        SALE_DATE,
        SALE_AMOUNT,
        CURRENCY,
        SALES_CHANNEL,
        STATUS,
        UPDATED_AT,
        SOURCE_SYSTEM,
        SOURCE_FILE_NAME,
        INGESTION_TIMESTAMP,
        RUN_ID

    FROM sales_ranked
    WHERE RECORD_RANK = 1
),

vehicles AS (

    SELECT *
    FROM {{ ref('stg_vehicles_valid') }}

),

dealers AS (

    SELECT *
    FROM {{ ref('stg_dealers_valid') }}

),

enriched AS (

    SELECT

        s.SALE_ID,
        s.VIN,
        s.DEALER_ID,
        s.SALE_DATE,
        s.SALE_AMOUNT,
        s.CURRENCY,
        s.SALES_CHANNEL,
        s.STATUS,
        s.UPDATED_AT,

        v.MODEL,
        v.MODEL_YEAR,
        v.FUEL_TYPE,
        v.PLANT_CODE,

        d.DEALER_NAME,
        d.CITY,
        d.STATE,
        d.REGION,

        s.SOURCE_SYSTEM,
        s.SOURCE_FILE_NAME,
        s.INGESTION_TIMESTAMP,
        s.RUN_ID,

        CASE
            WHEN v.VIN IS NULL THEN 'FAILED'
            ELSE 'PASSED'
        END AS VEHICLE_REFERENTIAL_CHECK,

        CASE
            WHEN d.DEALER_ID IS NULL THEN 'FAILED'
            ELSE 'PASSED'
        END AS DEALER_REFERENTIAL_CHECK

    FROM sales s

    LEFT JOIN vehicles v
        ON s.VIN = v.VIN

    LEFT JOIN dealers d
        ON s.DEALER_ID = d.DEALER_ID

)

SELECT

    *,

    CASE
        WHEN VEHICLE_REFERENTIAL_CHECK = 'FAILED'
          OR DEALER_REFERENTIAL_CHECK = 'FAILED'
        THEN 'FAILED'
        ELSE 'PASSED'
    END AS REFERENTIAL_CHECK,

    CASE
        WHEN SALE_AMOUNT >= 50000 THEN 'HIGH_VALUE'
        WHEN SALE_AMOUNT >= 20000 THEN 'MEDIUM_VALUE'
        ELSE 'STANDARD'
    END AS SALE_CATEGORY,

    YEAR(SALE_DATE) AS SALE_YEAR,

    MONTH(SALE_DATE) AS SALE_MONTH

FROM enriched