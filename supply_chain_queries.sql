CREATE DATABASE SUPPLY_CHAIN_DB;

CREATE SCHEMA SUPPLY_CHAIN_DB.RAW;
CREATE SCHEMA SUPPLY_CHAIN_DB.STAGING;
CREATE SCHEMA SUPPLY_CHAIN_DB.CURATED;


CREATE WAREHOUSE SUPPLY_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;


USE DATABASE SUPPLY_CHAIN_DB;
USE SCHEMA RAW;
USE WAREHOUSE SUPPLY_WH;

SHOW SCHEMAS IN DATABASE SUPPLY_CHAIN_DB
SELECT 'DIM_SKU'     AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.DIM_SKU
UNION ALL
SELECT 'DIM_DATE'    AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.DIM_DATE
UNION ALL
SELECT 'FACT_DEMAND' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW.FACT_DEMAND;

-- ── STAGING LAYER: DIM_SKU ────────────────────────────────────────
CREATE OR REPLACE TABLE SUPPLY_CHAIN_DB.STAGING.DIM_SKU AS
SELECT
    SKU_ID,
    UPPER(TRIM(SKU_NAME))                          AS SKU_NAME,
    UPPER(TRIM(CATEGORY))                          AS CATEGORY,
    UPPER(TRIM(SUPPLIER))                          AS SUPPLIER,
    UPPER(TRIM(ABC_CLASS))                         AS ABC_CLASS,
    UPPER(TRIM(XYZ_CLASS))                         AS XYZ_CLASS,
    CONCAT(ABC_CLASS, XYZ_CLASS)                   AS COMBINED_CLASS,
    ROUND(UNIT_COST_USD, 2)                        AS UNIT_COST_USD,
    LEAD_TIME_DAYS,
    SAFETY_STOCK_DAYS,
    IS_ACTIVE,
    CREATED_DATE,
    
    -- Classify SKU value tier
    CASE 
        WHEN UNIT_COST_USD >= 500  THEN 'HIGH VALUE'
        WHEN UNIT_COST_USD >= 100  THEN 'MID VALUE'
        ELSE                            'LOW VALUE'
    END                                            AS VALUE_TIER,
    
    -- Flag slow movers
    CASE 
        WHEN ABC_CLASS = 'C' AND XYZ_CLASS = 'Z' THEN 'YES'
        ELSE 'NO'
    END                                            AS IS_SLOW_MOVER,
    
    -- Rationalization candidate flag
    CASE
        WHEN ABC_CLASS = 'C' AND XYZ_CLASS = 'Z' AND IS_ACTIVE = TRUE THEN 'REVIEW'
        WHEN IS_ACTIVE = FALSE                                          THEN 'INACTIVE'
        ELSE                                                                 'KEEP'
    END                                            AS RATIONALIZATION_FLAG,
    
    CURRENT_TIMESTAMP()                            AS STAGING_LOAD_TS

FROM SUPPLY_CHAIN_DB.RAW.DIM_SKU
WHERE SKU_ID IS NOT NULL
  AND UNIT_COST_USD > 0;

-- Verify
SELECT 
    COUNT(*)                                        AS TOTAL_SKUS,
    COUNT(CASE WHEN IS_SLOW_MOVER = 'YES'  THEN 1 END) AS SLOW_MOVERS,
    COUNT(CASE WHEN RATIONALIZATION_FLAG = 'REVIEW' THEN 1 END) AS REVIEW_CANDIDATES,
    COUNT(CASE WHEN IS_ACTIVE = TRUE       THEN 1 END) AS ACTIVE_SKUS
FROM SUPPLY_CHAIN_DB.STAGING.DIM_SKU;

-- ── STAGING LAYER: DIM_DATE ───────────────────────────────────────
CREATE OR REPLACE TABLE SUPPLY_CHAIN_DB.STAGING.DIM_DATE AS
SELECT
    DATE_KEY,
    YEAR,
    QUARTER,
    MONTH,
    MONTH_NAME,
    WEEK,
    DAY_OF_WEEK,
    DAY_NAME,
    IS_WEEKEND,
    IS_MONTH_END,
    IS_QUARTER_END,
    FISCAL_YEAR,
    FISCAL_QUARTER,
    
    -- Add useful labels
    CONCAT('Q', QUARTER, '-', YEAR)                AS QUARTER_LABEL,
    CONCAT(MONTH_NAME, ' ', YEAR)                  AS MONTH_YEAR_LABEL,
    
    -- Season classification
    CASE
        WHEN MONTH IN (12, 1, 2)  THEN 'WINTER'
        WHEN MONTH IN (3, 4, 5)   THEN 'SPRING'
        WHEN MONTH IN (6, 7, 8)   THEN 'SUMMER'
        ELSE                           'AUTUMN'
    END                                            AS SEASON,
    
    -- Half year
    CASE
        WHEN MONTH <= 6 THEN 'H1'
        ELSE                 'H2'
    END                                            AS HALF_YEAR,
    
    CURRENT_TIMESTAMP()                            AS STAGING_LOAD_TS

FROM SUPPLY_CHAIN_DB.RAW.DIM_DATE;

-- Verify
SELECT COUNT(*) AS TOTAL_DATES FROM SUPPLY_CHAIN_DB.STAGING.DIM_DATE;
-- ── STAGING LAYER: FACT_DEMAND ────────────────────────────────────
CREATE OR REPLACE TABLE SUPPLY_CHAIN_DB.STAGING.FACT_DEMAND AS
SELECT
    FD.DEMAND_DATE,
    FD.SKU_ID,
    FD.REGION,
    FD.ACTUAL_DEMAND,
    FD.FORECAST_DEMAND,
    FD.VARIANCE,
    FD.UNIT_COST_USD,
    FD.DEMAND_VALUE_USD,
    FD.ABC_CLASS,
    FD.XYZ_CLASS,
    FD.SUPPLIER,
    FD.CATEGORY,
    
    -- Add date parts for easy filtering
    YEAR(FD.DEMAND_DATE)                           AS DEMAND_YEAR,
    MONTH(FD.DEMAND_DATE)                          AS DEMAND_MONTH,
    QUARTER(FD.DEMAND_DATE)                        AS DEMAND_QUARTER,
    
    -- Forecast accuracy metrics per row
    ABS(FD.VARIANCE)                               AS ABS_VARIANCE,
    
    CASE
        WHEN FD.ACTUAL_DEMAND = 0 THEN NULL
        ELSE ROUND(ABS(FD.VARIANCE) / FD.ACTUAL_DEMAND * 100, 2)
    END                                            AS MAPE_PCT,
    
    -- Forecast bias (over or under)
    CASE
        WHEN FD.VARIANCE > 0  THEN 'UNDER FORECAST'
        WHEN FD.VARIANCE < 0  THEN 'OVER FORECAST'
        ELSE                       'PERFECT'
    END                                            AS FORECAST_BIAS,
    
    -- Demand tier
    CASE
        WHEN FD.ACTUAL_DEMAND = 0        THEN 'ZERO DEMAND'
        WHEN FD.ACTUAL_DEMAND <= 10      THEN 'LOW'
        WHEN FD.ACTUAL_DEMAND <= 100     THEN 'MEDIUM'
        ELSE                                  'HIGH'
    END                                            AS DEMAND_TIER,
    
    CURRENT_TIMESTAMP()                            AS STAGING_LOAD_TS

FROM SUPPLY_CHAIN_DB.RAW.FACT_DEMAND FD
WHERE FD.SKU_ID   IS NOT NULL
  AND FD.DEMAND_DATE IS NOT NULL
  AND FD.ACTUAL_DEMAND >= 0;

-- Verify
SELECT
    COUNT(*)                                        AS TOTAL_ROWS,
    COUNT(DISTINCT SKU_ID)                          AS UNIQUE_SKUS,
    COUNT(DISTINCT REGION)                          AS UNIQUE_REGIONS,
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    MIN(DEMAND_DATE)                                AS EARLIEST_DATE,
    MAX(DEMAND_DATE)                                AS LATEST_DATE
FROM SUPPLY_CHAIN_DB.STAGING.FACT_DEMAND;

-- ── CURATED LAYER: DIM_SKU ────────────────────────────────────────
CREATE OR REPLACE TABLE SUPPLY_CHAIN_DB.CURATED.DIM_SKU AS
SELECT
    SKU_ID,
    SKU_NAME,
    CATEGORY,
    SUPPLIER,
    ABC_CLASS,
    XYZ_CLASS,
    COMBINED_CLASS,
    UNIT_COST_USD,
    LEAD_TIME_DAYS,
    SAFETY_STOCK_DAYS,
    IS_ACTIVE,
    CREATED_DATE,
    VALUE_TIER,
    IS_SLOW_MOVER,
    RATIONALIZATION_FLAG,
    
    -- Add reorder point (avg demand proxy)
    ROUND(UNIT_COST_USD * LEAD_TIME_DAYS, 2)       AS REORDER_VALUE_USD,
    
    -- Priority score for reporting
    CASE
        WHEN ABC_CLASS = 'A' AND XYZ_CLASS = 'X' THEN 1
        WHEN ABC_CLASS = 'A' AND XYZ_CLASS = 'Y' THEN 2
        WHEN ABC_CLASS = 'A' AND XYZ_CLASS = 'Z' THEN 3
        WHEN ABC_CLASS = 'B' AND XYZ_CLASS = 'X' THEN 4
        WHEN ABC_CLASS = 'B' AND XYZ_CLASS = 'Y' THEN 5
        WHEN ABC_CLASS = 'B' AND XYZ_CLASS = 'Z' THEN 6
        WHEN ABC_CLASS = 'C' AND XYZ_CLASS = 'X' THEN 7
        WHEN ABC_CLASS = 'C' AND XYZ_CLASS = 'Y' THEN 8
        ELSE                                           9
    END                                            AS PRIORITY_SCORE,

    CURRENT_TIMESTAMP()                            AS CURATED_LOAD_TS

FROM SUPPLY_CHAIN_DB.STAGING.DIM_SKU;

-- Verify
SELECT
    COMBINED_CLASS,
    COUNT(*)                                        AS SKU_COUNT,
    ROUND(AVG(UNIT_COST_USD), 2)                    AS AVG_UNIT_COST,
    ROUND(SUM(UNIT_COST_USD), 0)                    AS TOTAL_COST_USD
FROM SUPPLY_CHAIN_DB.CURATED.DIM_SKU
GROUP BY COMBINED_CLASS
ORDER BY COMBINED_CLASS;

-- ── CURATED LAYER: DIM_DATE ───────────────────────────────────────
CREATE OR REPLACE TABLE SUPPLY_CHAIN_DB.CURATED.DIM_DATE AS
SELECT
    DATE_KEY,
    YEAR,
    QUARTER,
    MONTH,
    MONTH_NAME,
    WEEK,
    DAY_OF_WEEK,
    DAY_NAME,
    IS_WEEKEND,
    IS_MONTH_END,
    IS_QUARTER_END,
    FISCAL_YEAR,
    FISCAL_QUARTER,
    QUARTER_LABEL,
    MONTH_YEAR_LABEL,
    SEASON,
    HALF_YEAR,

    -- Rolling period flags
    CASE
        WHEN DATE_KEY >= DATEADD('month', -3, CURRENT_DATE()) THEN 'LAST 3 MONTHS'
        WHEN DATE_KEY >= DATEADD('month', -6, CURRENT_DATE()) THEN 'LAST 6 MONTHS'
        WHEN DATE_KEY >= DATEADD('month', -12,CURRENT_DATE()) THEN 'LAST 12 MONTHS'
        ELSE                                                        'OLDER'
    END                                            AS ROLLING_PERIOD,

    CURRENT_TIMESTAMP()                            AS CURATED_LOAD_TS

FROM SUPPLY_CHAIN_DB.STAGING.DIM_DATE;

-- Verify
SELECT
    YEAR,
    COUNT(*)                                        AS DAYS_IN_YEAR
FROM SUPPLY_CHAIN_DB.CURATED.DIM_DATE
GROUP BY YEAR
ORDER BY YEAR;

-- ── CURATED LAYER: FACT_DEMAND ────────────────────────────────────
CREATE OR REPLACE TABLE SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND AS
SELECT
    FD.DEMAND_DATE,
    FD.SKU_ID,
    FD.REGION,
    FD.ACTUAL_DEMAND,
    FD.FORECAST_DEMAND,
    FD.VARIANCE,
    FD.ABS_VARIANCE,
    FD.MAPE_PCT,
    FD.FORECAST_BIAS,
    FD.DEMAND_TIER,
    FD.UNIT_COST_USD,
    FD.DEMAND_VALUE_USD,
    FD.ABC_CLASS,
    FD.XYZ_CLASS,
    FD.SUPPLIER,
    FD.CATEGORY,
    FD.DEMAND_YEAR,
    FD.DEMAND_MONTH,
    FD.DEMAND_QUARTER,

    -- Join enrichments from DIM_SKU
    SK.COMBINED_CLASS,
    SK.VALUE_TIER,
    SK.LEAD_TIME_DAYS,
    SK.SAFETY_STOCK_DAYS,
    SK.RATIONALIZATION_FLAG,
    SK.PRIORITY_SCORE,

    -- Join enrichments from DIM_DATE
    DD.QUARTER_LABEL,
    DD.MONTH_YEAR_LABEL,
    DD.SEASON,
    DD.HALF_YEAR,
    DD.ROLLING_PERIOD,

    -- Inventory value metrics
    ROUND(FD.ACTUAL_DEMAND * SK.SAFETY_STOCK_DAYS 
          * FD.UNIT_COST_USD / 30, 2)              AS SAFETY_STOCK_VALUE_USD,

    ROUND(FD.ACTUAL_DEMAND * SK.LEAD_TIME_DAYS
          / 30 * FD.UNIT_COST_USD, 2)              AS PIPELINE_INVENTORY_USD,

    CURRENT_TIMESTAMP()                            AS CURATED_LOAD_TS

FROM SUPPLY_CHAIN_DB.STAGING.FACT_DEMAND    FD
LEFT JOIN SUPPLY_CHAIN_DB.CURATED.DIM_SKU   SK ON FD.SKU_ID = SK.SKU_ID
LEFT JOIN SUPPLY_CHAIN_DB.CURATED.DIM_DATE  DD ON FD.DEMAND_DATE = DD.DATE_KEY;

-- Verify
SELECT
    COUNT(*)                                        AS TOTAL_ROWS,
    COUNT(DISTINCT SKU_ID)                          AS UNIQUE_SKUS,
    COUNT(DISTINCT REGION)                          AS UNIQUE_REGIONS,
    ROUND(SUM(DEMAND_VALUE_USD)/1000000, 2)         AS TOTAL_DEMAND_VALUE_MILLIONS,
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    ROUND(SUM(SAFETY_STOCK_VALUE_USD)/1000000, 2)   AS TOTAL_SAFETY_STOCK_MILLIONS
FROM SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND;

-- ── MATERIALIZED VIEW: MONTHLY KPI SUMMARY ────────────────────────

CREATE OR REPLACE DYNAMIC TABLE SUPPLY_CHAIN_DB.CURATED.MONTHLY_KPI_SUMMARY
    TARGET_LAG = '1 day'
    WAREHOUSE  = SUPPLY_WH
AS
SELECT
    DEMAND_YEAR,
    DEMAND_QUARTER,
    DEMAND_MONTH,
    MONTH_YEAR_LABEL,
    QUARTER_LABEL,
    REGION,
    ABC_CLASS,
    XYZ_CLASS,
    COMBINED_CLASS,
    CATEGORY,
    SUPPLIER,

    -- Volume KPIs
    SUM(ACTUAL_DEMAND)                              AS TOTAL_ACTUAL_DEMAND,
    SUM(FORECAST_DEMAND)                            AS TOTAL_FORECAST_DEMAND,
    SUM(ABS_VARIANCE)                               AS TOTAL_ABS_VARIANCE,

    -- Value KPIs
    ROUND(SUM(DEMAND_VALUE_USD), 2)                 AS TOTAL_DEMAND_VALUE_USD,
    ROUND(SUM(SAFETY_STOCK_VALUE_USD), 2)           AS TOTAL_SAFETY_STOCK_USD,
    ROUND(SUM(PIPELINE_INVENTORY_USD), 2)           AS TOTAL_PIPELINE_INV_USD,

    -- Accuracy KPIs
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    COUNT(CASE WHEN MAPE_PCT <= 15 THEN 1 END)      AS ACCURATE_FORECASTS,
    COUNT(*)                                        AS TOTAL_FORECASTS,
    ROUND(COUNT(CASE WHEN MAPE_PCT <= 15 THEN 1 END)
          / COUNT(*) * 100, 2)                      AS FORECAST_ACCURACY_RATE,

    -- Demand health
    COUNT(CASE WHEN ACTUAL_DEMAND = 0 THEN 1 END)   AS ZERO_DEMAND_COUNT,
    COUNT(DISTINCT SKU_ID)                          AS ACTIVE_SKU_COUNT

FROM SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND
GROUP BY 1,2,3,4,5,6,7,8,9,10,11;

-- Verify
SELECT COUNT(*) AS SUMMARY_ROWS FROM SUPPLY_CHAIN_DB.CURATED.MONTHLY_KPI_SUMMARY;

-- Which SKU class has best forecast accuracy?
SELECT
    ABC_CLASS,
    COUNT(DISTINCT SKU_ID)                          AS SKU_COUNT,
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    ROUND(MIN(MAPE_PCT), 2)                         AS BEST_MAPE,
    ROUND(MAX(MAPE_PCT), 2)                         AS WORST_MAPE,
    COUNT(CASE WHEN MAPE_PCT <= 15 THEN 1 END)      AS ACCURATE_COUNT,
    ROUND(COUNT(CASE WHEN MAPE_PCT <= 15 THEN 1 END)
          / COUNT(*) * 100, 2)                      AS ACCURACY_RATE_PCT
FROM SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND
WHERE MAPE_PCT IS NOT NULL
GROUP BY ABC_CLASS
ORDER BY ABC_CLASS;

-- Your most critical service parts
SELECT
    SKU_ID,
    CATEGORY,
    SUPPLIER,
    COMBINED_CLASS,
    VALUE_TIER,
    ROUND(SUM(DEMAND_VALUE_USD), 0)                 AS TOTAL_DEMAND_VALUE,
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    SUM(ACTUAL_DEMAND)                              AS TOTAL_UNITS_DEMANDED
FROM SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND
GROUP BY 1,2,3,4,5
ORDER BY TOTAL_DEMAND_VALUE DESC
LIMIT 10;

-- How is each region performing?
SELECT
    REGION,
    COUNT(DISTINCT SKU_ID)                          AS SKUS_ACTIVE,
    ROUND(SUM(DEMAND_VALUE_USD)/1000000, 2)         AS DEMAND_VALUE_MILLIONS,
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    ROUND(SUM(SAFETY_STOCK_VALUE_USD)/1000000, 2)   AS SAFETY_STOCK_MILLIONS,
    COUNT(CASE WHEN ACTUAL_DEMAND = 0 THEN 1 END)   AS ZERO_DEMAND_RECORDS,
    COUNT(CASE WHEN FORECAST_BIAS = 'UNDER FORECAST'
               THEN 1 END)                          AS UNDER_FORECAST_COUNT,
    COUNT(CASE WHEN FORECAST_BIAS = 'OVER FORECAST'
               THEN 1 END)                          AS OVER_FORECAST_COUNT
FROM SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND
GROUP BY REGION
ORDER BY DEMAND_VALUE_MILLIONS DESC;

-- Full 3-year demand trend
SELECT
    MONTH_YEAR_LABEL,
    DEMAND_YEAR,
    DEMAND_MONTH,
    ROUND(SUM(DEMAND_VALUE_USD)/1000000, 2)         AS DEMAND_VALUE_MILLIONS,
    SUM(ACTUAL_DEMAND)                              AS TOTAL_UNITS,
    SUM(FORECAST_DEMAND)                            AS TOTAL_FORECAST,
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    COUNT(DISTINCT SKU_ID)                          AS ACTIVE_SKUS
FROM SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND
GROUP BY 1,2,3
ORDER BY DEMAND_YEAR, DEMAND_MONTH;

-- Your cost saving opportunity analysis
SELECT
    RATIONALIZATION_FLAG,
    COMBINED_CLASS,
    COUNT(DISTINCT SKU_ID)                          AS SKU_COUNT,
    ROUND(SUM(DEMAND_VALUE_USD)/1000000, 2)         AS DEMAND_VALUE_MILLIONS,
    ROUND(SUM(SAFETY_STOCK_VALUE_USD)/1000000, 2)   AS SAFETY_STOCK_MILLIONS,
    ROUND(AVG(MAPE_PCT), 2)                         AS AVG_MAPE_PCT,
    ROUND(SUM(SAFETY_STOCK_VALUE_USD) /
          SUM(SUM(SAFETY_STOCK_VALUE_USD)) 
          OVER() * 100, 2)                          AS PCT_OF_TOTAL_SAFETY_STOCK
FROM SUPPLY_CHAIN_DB.CURATED.FACT_DEMAND
WHERE RATIONALIZATION_FLAG IS NOT NULL
GROUP BY 1,2
ORDER BY RATIONALIZATION_FLAG, COMBINED_CLASS;



