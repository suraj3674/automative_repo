{{ config(
    materialized='incremental',
    unique_key='SALE_ID',
    incremental_strategy='merge'
) }}

WITH sales AS (

    SELECT
        s.*
    FROM {{ ref('int_sales_enriched') }} s

    {% if is_incremental() %}

    CROSS JOIN (

        SELECT
            COALESCE(
                DATEADD(
                    DAY,
                    -2,
                    MAX(UPDATED_AT)
                ),
                TO_TIMESTAMP_NTZ('1900-01-01')
            ) AS LOWER_BOUND

        FROM {{ this }}

    ) wm

    WHERE s.UPDATED_AT >= wm.LOWER_BOUND

    {% endif %}
),

final AS (

    SELECT
        s.SALE_ID,

        dt.DATE_SK,

        COALESCE(
            v.VEHICLE_SK,
            SHA2('UNKNOWN_VEHICLE', 256)
        ) AS VEHICLE_SK,

        d.DEALER_SK,
        m.MODEL_SK,
        r.REGION_SK,

        s.SALE_DATE,
        s.SALE_AMOUNT,
        s.CURRENCY,
        s.SALES_CHANNEL,
        s.STATUS,
        s.SALE_CATEGORY,
        s.UPDATED_AT,
        s.SOURCE_SYSTEM,
        s.SOURCE_FILE_NAME,
        s.INGESTION_TIMESTAMP,
        s.RUN_ID,

        CURRENT_TIMESTAMP() AS FACT_LOAD_TIMESTAMP

    FROM sales s

    LEFT JOIN {{ ref('dim_vehicle') }} v
        ON s.VIN = v.VIN
        AND s.SALE_DATE >= TO_DATE(v.EFFECTIVE_FROM)
        AND s.SALE_DATE < TO_DATE(v.EFFECTIVE_TO)

    LEFT JOIN {{ ref('dim_dealer') }} d
        ON s.DEALER_ID = d.DEALER_ID
        AND s.SALE_DATE >= TO_DATE(d.EFFECTIVE_FROM)
        AND s.SALE_DATE < TO_DATE(d.EFFECTIVE_TO)

    LEFT JOIN {{ ref('dim_model') }} m
        ON s.MODEL = m.MODEL
        AND s.MODEL_YEAR = m.MODEL_YEAR

    LEFT JOIN {{ ref('dim_region') }} r
        ON s.REGION = r.REGION

    LEFT JOIN {{ ref('dim_date') }} dt
        ON s.SALE_DATE = dt.CALENDAR_DATE
)

SELECT *
FROM final