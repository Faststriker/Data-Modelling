-- ============================================================
-- PROJECT-3
-- ENTERPRISE INCREMENTAL SALES DATA WAREHOUSE USING SNOWFLAKE
-- ============================================================


-- ============================================================
-- PHASE 1 : SNOWFLAKE ENVIRONMENT
-- ============================================================

-- 1. CREATE WAREHOUSE

CREATE WAREHOUSE IF NOT EXISTS ENTERPRISE_WH
WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;


-- 2. CREATE DATABASE

CREATE DATABASE IF NOT EXISTS ENTERPRISE_DB;


-- 3. CREATE SCHEMA

CREATE SCHEMA IF NOT EXISTS ENTERPRISE_DB.SALES_SCHEMA;


-- SET CONTEXT

USE WAREHOUSE ENTERPRISE_WH;

USE DATABASE ENTERPRISE_DB;

USE SCHEMA SALES_SCHEMA;


-- 4. CREATE CSV FILE FORMAT

CREATE FILE FORMAT IF NOT EXISTS SALES_CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null');


-- 5. CREATE INTERNAL STAGE

CREATE STAGE IF NOT EXISTS SALES_STAGE
FILE_FORMAT = SALES_CSV_FORMAT;


-- VERIFY ENVIRONMENT

SHOW WAREHOUSES;

SHOW DATABASES;

SHOW SCHEMAS;

SHOW FILE FORMATS;

SHOW STAGES;



-- ============================================================
-- PHASE 2 : DATA LOADING
-- ============================================================

-- Upload these files manually to SALES_STAGE:
--
-- customers.csv
-- products.csv
-- branches.csv
-- sales_history.csv
-- new_sales.csv
--
-- Then verify:

LIST @SALES_STAGE;



-- ============================================================
-- CREATE TABLES
-- ============================================================


-- CUSTOMERS

CREATE OR REPLACE TABLE CUSTOMERS
(
    CUSTOMER_ID NUMBER,
    CUSTOMER_NAME VARCHAR(100),
    CITY VARCHAR(100),
    MEMBERSHIP VARCHAR(50)
);


-- PRODUCTS

CREATE OR REPLACE TABLE PRODUCTS
(
    PRODUCT_ID NUMBER,
    PRODUCT_NAME VARCHAR(100),
    CATEGORY VARCHAR(100),
    PRICE NUMBER(12,2)
);


-- BRANCHES

CREATE OR REPLACE TABLE BRANCHES
(
    BRANCH_ID NUMBER,
    BRANCH_NAME VARCHAR(100),
    STATE VARCHAR(100)
);


-- SALES

CREATE OR REPLACE TABLE SALES
(
    SALE_ID NUMBER,
    CUSTOMER_ID NUMBER,
    PRODUCT_ID NUMBER,
    BRANCH_ID NUMBER,
    QUANTITY NUMBER,
    SALE_DATE DATE,
    TOTAL_AMOUNT NUMBER(12,2)
);



-- ============================================================
-- LOAD CUSTOMERS
-- ============================================================

COPY INTO CUSTOMERS
FROM @SALES_STAGE/customers.csv
FILE_FORMAT = SALES_CSV_FORMAT
ON_ERROR = 'CONTINUE';

SELECT *
FROM CUSTOMERS
ORDER BY CUSTOMER_ID;



-- ============================================================
-- LOAD PRODUCTS
-- ============================================================

COPY INTO PRODUCTS
FROM @SALES_STAGE/products.csv
FILE_FORMAT = SALES_CSV_FORMAT
ON_ERROR = 'CONTINUE';

SELECT *
FROM PRODUCTS
ORDER BY PRODUCT_ID;



-- ============================================================
-- LOAD BRANCHES
-- ============================================================

COPY INTO BRANCHES
FROM @SALES_STAGE/branches.csv
FILE_FORMAT = SALES_CSV_FORMAT
ON_ERROR = 'CONTINUE';

SELECT *
FROM BRANCHES
ORDER BY BRANCH_ID;



-- ============================================================
-- LOAD HISTORICAL SALES
-- ============================================================

COPY INTO SALES
FROM @SALES_STAGE/sales_history.csv
FILE_FORMAT = SALES_CSV_FORMAT
ON_ERROR = 'CONTINUE';

SELECT *
FROM SALES
ORDER BY SALE_ID;



-- ============================================================
-- VERIFY RECORD COUNTS
-- ============================================================

SELECT COUNT(*) AS CUSTOMER_COUNT
FROM CUSTOMERS;

SELECT COUNT(*) AS PRODUCT_COUNT
FROM PRODUCTS;

SELECT COUNT(*) AS BRANCH_COUNT
FROM BRANCHES;

SELECT COUNT(*) AS HISTORICAL_SALES_COUNT
FROM SALES;



-- ============================================================
-- PHASE 3 : INCREMENTAL LOADING
-- ============================================================


-- 10. CREATE STREAM ON SALES

CREATE OR REPLACE STREAM SALES_STREAM
ON TABLE SALES;


-- VERIFY STREAM

SHOW STREAMS;



-- ============================================================
-- 11. LOAD NEW SALES
-- ============================================================

COPY INTO SALES
FROM @SALES_STAGE/new_sales.csv
FILE_FORMAT = SALES_CSV_FORMAT
ON_ERROR = 'CONTINUE';


-- VERIFY SALES

SELECT *
FROM SALES
ORDER BY SALE_ID;



-- ============================================================
-- 12. DISPLAY NEW RECORDS USING STREAM
-- ============================================================

SELECT
    SALE_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    BRANCH_ID,
    QUANTITY,
    SALE_DATE,
    TOTAL_AMOUNT,
    METADATA$ACTION,
    METADATA$ISUPDATE
FROM SALES_STREAM
WHERE METADATA$ACTION = 'INSERT'
ORDER BY SALE_ID;



-- ============================================================
-- CREATE TEMPORARY TABLE FOR INCREMENTAL DATA
-- ============================================================

CREATE OR REPLACE TEMPORARY TABLE NEW_SALES_STAGE
AS
SELECT *
FROM SALES
WHERE 1 = 0;


-- LOAD NEW FILE INTO TEMPORARY TABLE

COPY INTO NEW_SALES_STAGE
FROM @SALES_STAGE/new_sales.csv
FILE_FORMAT = SALES_CSV_FORMAT
ON_ERROR = 'CONTINUE';


-- CHECK STAGING DATA

SELECT *
FROM NEW_SALES_STAGE
ORDER BY SALE_ID;



-- ============================================================
-- 13. MERGE NEW SALES INTO SALES
-- ============================================================

MERGE INTO SALES AS TARGET

USING NEW_SALES_STAGE AS SOURCE

ON TARGET.SALE_ID = SOURCE.SALE_ID

WHEN MATCHED THEN
    UPDATE SET
        TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID,
        TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
        TARGET.BRANCH_ID = SOURCE.BRANCH_ID,
        TARGET.QUANTITY = SOURCE.QUANTITY,
        TARGET.SALE_DATE = SOURCE.SALE_DATE,
        TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT

WHEN NOT MATCHED THEN
    INSERT
    (
        SALE_ID,
        CUSTOMER_ID,
        PRODUCT_ID,
        BRANCH_ID,
        QUANTITY,
        SALE_DATE,
        TOTAL_AMOUNT
    )

    VALUES
    (
        SOURCE.SALE_ID,
        SOURCE.CUSTOMER_ID,
        SOURCE.PRODUCT_ID,
        SOURCE.BRANCH_ID,
        SOURCE.QUANTITY,
        SOURCE.SALE_DATE,
        SOURCE.TOTAL_AMOUNT
    );



-- VERIFY FINAL SALES

SELECT *
FROM SALES
ORDER BY SALE_ID;



-- ============================================================
-- PHASE 4 : DATA VALIDATION
-- ============================================================


-- 14. DUPLICATE SALE IDs

SELECT
    SALE_ID,
    COUNT(*) AS RECORD_COUNT

FROM SALES

GROUP BY SALE_ID

HAVING COUNT(*) > 1

ORDER BY SALE_ID;



-- ============================================================
-- 15. MISSING CUSTOMER IDs
-- ============================================================

SELECT
    S.SALE_ID,
    S.CUSTOMER_ID

FROM SALES S

LEFT JOIN CUSTOMERS C
    ON S.CUSTOMER_ID = C.CUSTOMER_ID

WHERE C.CUSTOMER_ID IS NULL;



-- ============================================================
-- 16. INVALID PRODUCT IDs
-- ============================================================

SELECT
    S.SALE_ID,
    S.PRODUCT_ID

FROM SALES S

LEFT JOIN PRODUCTS P
    ON S.PRODUCT_ID = P.PRODUCT_ID

WHERE P.PRODUCT_ID IS NULL;



-- ============================================================
-- 17. TOTAL NEWLY INSERTED RECORDS
-- ============================================================

SELECT
    COUNT(*) AS NEW_RECORD_COUNT

FROM SALES

WHERE SALE_ID > 5;



-- ============================================================
-- STREAM COUNT
-- ============================================================

SELECT
    COUNT(*) AS STREAM_NEW_RECORD_COUNT

FROM SALES_STREAM

WHERE METADATA$ACTION = 'INSERT';



-- ============================================================
-- PHASE 5 : TIME TRAVEL
-- ============================================================


-- FIRST RECORD CURRENT TIME

SELECT CURRENT_TIMESTAMP() AS BEFORE_DELETE_TIME;



-- 18. DELETE ONE SALES RECORD

DELETE FROM SALES
WHERE SALE_ID = 10;



-- VERIFY DELETION

SELECT *
FROM SALES
WHERE SALE_ID = 10;



-- ============================================================
-- 19. RECOVER USING TIME TRAVEL
-- ============================================================
--
-- Replace the timestamp below with the timestamp captured
-- immediately BEFORE the DELETE operation.
--
-- Example:
-- '2026-08-14 11:30:00'
--


INSERT INTO SALES
SELECT *
FROM SALES
AT
(
    TIMESTAMP => '2026-08-14 11:30:00'::TIMESTAMP
)
WHERE SALE_ID = 10;



-- ============================================================
-- 20. VERIFY RECOVERY
-- ============================================================

SELECT *
FROM SALES
WHERE SALE_ID = 10;



-- ============================================================
-- PHASE 6 : ZERO COPY CLONE
-- ============================================================


-- 21. CREATE CLONE

CREATE OR REPLACE TABLE SALES_TEST
CLONE SALES;



-- VERIFY CLONE

SHOW TABLES;



-- ============================================================
-- 22. DISPLAY CLONED RECORDS
-- ============================================================

SELECT *
FROM SALES_TEST
ORDER BY SALE_ID;



-- ============================================================
-- 23. INSERT TEST RECORD INTO CLONE
-- ============================================================

INSERT INTO SALES_TEST
(
    SALE_ID,
    CUSTOMER_ID,
    PRODUCT_ID,
    BRANCH_ID,
    QUANTITY,
    SALE_DATE,
    TOTAL_AMOUNT
)

VALUES
(
    999,
    1,
    101,
    1,
    1,
    '2026-07-15',
    60000
);



-- VERIFY CLONE

SELECT *
FROM SALES_TEST
WHERE SALE_ID = 999;



-- ============================================================
-- 24. VERIFY ORIGINAL TABLE IS UNCHANGED
-- ============================================================

SELECT *
FROM SALES
WHERE SALE_ID = 999;



-- ============================================================
-- PHASE 7 : TASK AUTOMATION
-- ============================================================


-- CREATE INCREMENTAL STAGING TABLE

CREATE OR REPLACE TABLE SALES_INCREMENTAL_STAGE
(
    SALE_ID NUMBER,
    CUSTOMER_ID NUMBER,
    PRODUCT_ID NUMBER,
    BRANCH_ID NUMBER,
    QUANTITY NUMBER,
    SALE_DATE DATE,
    TOTAL_AMOUNT NUMBER(12,2)
);



-- CREATE STREAM ON INCREMENTAL STAGING TABLE

CREATE OR REPLACE STREAM SALES_INCREMENTAL_STREAM
ON TABLE SALES_INCREMENTAL_STAGE;



-- ============================================================
-- 25. CREATE TASK
-- ============================================================

CREATE OR REPLACE TASK SALES_INCREMENTAL_TASK

WAREHOUSE = ENTERPRISE_WH

SCHEDULE = 'USING CRON 0 2 * * * UTC'

WHEN SYSTEM$STREAM_HAS_DATA('SALES_INCREMENTAL_STREAM')

AS

MERGE INTO SALES AS TARGET

USING
(
    SELECT
        SALE_ID,
        CUSTOMER_ID,
        PRODUCT_ID,
        BRANCH_ID,
        QUANTITY,
        SALE_DATE,
        TOTAL_AMOUNT

    FROM SALES_INCREMENTAL_STREAM

    WHERE METADATA$ACTION = 'INSERT'

) AS SOURCE

ON TARGET.SALE_ID = SOURCE.SALE_ID

WHEN MATCHED THEN

    UPDATE SET
        TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID,
        TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
        TARGET.BRANCH_ID = SOURCE.BRANCH_ID,
        TARGET.QUANTITY = SOURCE.QUANTITY,
        TARGET.SALE_DATE = SOURCE.SALE_DATE,
        TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT

WHEN NOT MATCHED THEN

    INSERT
    (
        SALE_ID,
        CUSTOMER_ID,
        PRODUCT_ID,
        BRANCH_ID,
        QUANTITY,
        SALE_DATE,
        TOTAL_AMOUNT
    )

    VALUES
    (
        SOURCE.SALE_ID,
        SOURCE.CUSTOMER_ID,
        SOURCE.PRODUCT_ID,
        SOURCE.BRANCH_ID,
        SOURCE.QUANTITY,
        SOURCE.SALE_DATE,
        SOURCE.TOTAL_AMOUNT
    );



-- ============================================================
-- 26. RESUME TASK
-- ============================================================

ALTER TASK SALES_INCREMENTAL_TASK RESUME;



-- ============================================================
-- 27. VERIFY TASK
-- ============================================================

SHOW TASKS;



-- TASK HISTORY

SELECT *
FROM TABLE
(
    INFORMATION_SCHEMA.TASK_HISTORY
    (
        TASK_NAME => 'SALES_INCREMENTAL_TASK'
    )
)

ORDER BY SCHEDULED_TIME DESC;



-- ============================================================
-- PHASE 8 : BUSINESS ANALYTICS
-- ============================================================


-- ============================================================
-- 28. CUSTOMER REVENUE REPORT
-- ============================================================

SELECT
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM CUSTOMERS C

JOIN SALES S
    ON C.CUSTOMER_ID = S.CUSTOMER_ID

GROUP BY
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME

ORDER BY TOTAL_REVENUE DESC;



-- ============================================================
-- 29. BRANCH REVENUE REPORT
-- ============================================================

SELECT
    B.BRANCH_ID,
    B.BRANCH_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM BRANCHES B

JOIN SALES S
    ON B.BRANCH_ID = S.BRANCH_ID

GROUP BY
    B.BRANCH_ID,
    B.BRANCH_NAME

ORDER BY TOTAL_REVENUE DESC;



-- ============================================================
-- 30. PRODUCT REVENUE REPORT
-- ============================================================

SELECT
    P.PRODUCT_ID,
    P.PRODUCT_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM PRODUCTS P

JOIN SALES S
    ON P.PRODUCT_ID = S.PRODUCT_ID

GROUP BY
    P.PRODUCT_ID,
    P.PRODUCT_NAME

ORDER BY TOTAL_REVENUE DESC;



-- ============================================================
-- 31. MONTHLY REVENUE REPORT
-- ============================================================

SELECT
    DATE_TRUNC('MONTH', SALE_DATE) AS SALES_MONTH,
    SUM(TOTAL_AMOUNT) AS MONTHLY_REVENUE

FROM SALES

GROUP BY DATE_TRUNC('MONTH', SALE_DATE)

ORDER BY SALES_MONTH;



-- ============================================================
-- 32. HIGHEST REVENUE CUSTOMER
-- ============================================================

SELECT
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM CUSTOMERS C

JOIN SALES S
    ON C.CUSTOMER_ID = S.CUSTOMER_ID

GROUP BY
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME

ORDER BY TOTAL_REVENUE DESC

LIMIT 1;



-- ============================================================
-- 33. HIGHEST REVENUE BRANCH
-- ============================================================

SELECT
    B.BRANCH_ID,
    B.BRANCH_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM BRANCHES B

JOIN SALES S
    ON B.BRANCH_ID = S.BRANCH_ID

GROUP BY
    B.BRANCH_ID,
    B.BRANCH_NAME

ORDER BY TOTAL_REVENUE DESC

LIMIT 1;



-- ============================================================
-- 34. TOP FIVE PRODUCTS
-- ============================================================

SELECT
    P.PRODUCT_ID,
    P.PRODUCT_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM PRODUCTS P

JOIN SALES S
    ON P.PRODUCT_ID = S.PRODUCT_ID

GROUP BY
    P.PRODUCT_ID,
    P.PRODUCT_NAME

ORDER BY TOTAL_REVENUE DESC

LIMIT 5;



-- ============================================================
-- 35. CUSTOMER PURCHASE FREQUENCY
-- ============================================================

SELECT
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME,
    COUNT(S.SALE_ID) AS PURCHASE_FREQUENCY

FROM CUSTOMERS C

JOIN SALES S
    ON C.CUSTOMER_ID = S.CUSTOMER_ID

GROUP BY
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME

ORDER BY PURCHASE_FREQUENCY DESC;



-- ============================================================
-- 36. RUNNING REVENUE
-- ============================================================

SELECT
    SALE_ID,
    SALE_DATE,
    TOTAL_AMOUNT,

    SUM(TOTAL_AMOUNT) OVER
    (
        ORDER BY SALE_DATE, SALE_ID
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS RUNNING_REVENUE

FROM SALES

ORDER BY
    SALE_DATE,
    SALE_ID;



-- ============================================================
-- 37. CUSTOMER RANKING
-- ============================================================

WITH CUSTOMER_REVENUE AS
(
    SELECT
        C.CUSTOMER_ID,
        C.CUSTOMER_NAME,
        SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

    FROM CUSTOMERS C

    JOIN SALES S
        ON C.CUSTOMER_ID = S.CUSTOMER_ID

    GROUP BY
        C.CUSTOMER_ID,
        C.CUSTOMER_NAME
)

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    TOTAL_REVENUE,

    RANK() OVER
    (
        ORDER BY TOTAL_REVENUE DESC
    ) AS CUSTOMER_RANK

FROM CUSTOMER_REVENUE

ORDER BY CUSTOMER_RANK;



-- ============================================================
-- ADDITIONAL ANALYTICS
-- ============================================================


-- TOP FIVE CUSTOMERS

SELECT
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM CUSTOMERS C

JOIN SALES S
    ON C.CUSTOMER_ID = S.CUSTOMER_ID

GROUP BY
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME

ORDER BY TOTAL_REVENUE DESC

LIMIT 5;



-- ============================================================
-- PHASE 9 : VIEWS
-- ============================================================


-- ============================================================
-- 38. CUSTOMER_REVENUE VIEW
-- ============================================================

CREATE OR REPLACE VIEW CUSTOMER_REVENUE AS

SELECT
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM CUSTOMERS C

JOIN SALES S
    ON C.CUSTOMER_ID = S.CUSTOMER_ID

GROUP BY
    C.CUSTOMER_ID,
    C.CUSTOMER_NAME;



-- QUERY VIEW

SELECT *
FROM CUSTOMER_REVENUE
ORDER BY TOTAL_REVENUE DESC;



-- ============================================================
-- 39. BRANCH_REVENUE MATERIALIZED VIEW
-- ============================================================

CREATE OR REPLACE MATERIALIZED VIEW BRANCH_REVENUE AS

SELECT
    B.BRANCH_ID,
    B.BRANCH_NAME,
    SUM(S.TOTAL_AMOUNT) AS TOTAL_REVENUE

FROM BRANCHES B

JOIN SALES S
    ON B.BRANCH_ID = S.BRANCH_ID

GROUP BY
    B.BRANCH_ID,
    B.BRANCH_NAME;



-- ============================================================
-- 40. DISPLAY MATERIALIZED VIEW
-- ============================================================

SELECT *
FROM BRANCH_REVENUE
ORDER BY TOTAL_REVENUE DESC;



-- ============================================================
-- FINAL VERIFICATION
-- ============================================================

SELECT 'CUSTOMERS' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT
FROM CUSTOMERS

UNION ALL

SELECT 'PRODUCTS', COUNT(*)
FROM PRODUCTS

UNION ALL

SELECT 'BRANCHES', COUNT(*)
FROM BRANCHES

UNION ALL

SELECT 'SALES', COUNT(*)
FROM SALES

UNION ALL

SELECT 'SALES_TEST', COUNT(*)
FROM SALES_TEST;



-- ============================================================
-- SHOW ALL PROJECT OBJECTS
-- ============================================================

SHOW TABLES;

SHOW VIEWS;

SHOW MATERIALIZED VIEWS;

SHOW STREAMS;

SHOW TASKS;

SHOW STAGES;

SHOW FILE FORMATS;