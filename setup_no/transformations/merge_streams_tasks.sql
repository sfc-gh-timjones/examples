/***************************************************************************************************
 MERGE DEMO: The "Hard Way" (Without Dynamic Tables)
 
 This script replicates the EXACT same pipeline as the Dynamic Tables demo,
 but uses MERGE + Streams + Tasks instead.
 
 Dynamic Tables version:  4 CREATE DYNAMIC TABLE statements (~40 lines of transformation SQL)
 This version:            4 tables + 6 streams + 5 tasks + 4 stored procedures (~300 lines)
 
 Same data. Same transformations. Same result. Cumulative aggregates are built twice
 (CUMULATIVE_PURCHASES vs CUMULATIVE_PURCHASES_MERGE) so you can compare DELETE+INSERT vs MERGE.

 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │                         COMPLEXITY COMPARISON                                  │
 ├──────────────────────────────┬────────────────────┬────────────────────────────┤
 │ Aspect                       │ Dynamic Tables     │ MERGE Pipeline             │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Objects to manage            │ 4 DTs              │ 5 target tables (incl.   │
 │                              │                    │ side-by-side cumulatives)+ │
 │                              │                    │ 6 streams + 5 tasks +      │
 │                              │                    │ 4 procedures ≈ 20 objects  │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Lines of DDL/DML             │ ~40                │ ~300+                      │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Dependency management        │ Automatic          │ Manual task DAG            │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Change detection             │ Built-in           │ Must create streams        │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Incremental logic            │ Automatic          │ Must write MERGE logic     │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Initial backfill             │ ON_CREATE          │ Manual INSERT...SELECT     │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Schema evolution             │ ALTER DT           │ ALTER every downstream     │
 │                              │                    │ table + procedure + MERGE  │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Monitoring                   │ REFRESH_HISTORY    │ Must build custom logging  │
 ├──────────────────────────────┼────────────────────┼────────────────────────────┤
 │ Error handling               │ Built-in retry     │ Must handle per procedure  │
 └──────────────────────────────┴────────────────────┴────────────────────────────┘
****************************************************************************************************/

/******************************************************************************************
 SECTION 0: Pre-Demo Cleanup
*******************************************************************************************/
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.DT_DEMO_MERGE_ALT;
USE DATABASE DEMO;
USE SCHEMA DEMO.DT_DEMO_MERGE_ALT;
USE WAREHOUSE WH_XS;

-- Drop tasks first (must suspend root first, then children)
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.ROOT_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_TASK SUSPEND;

DROP TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_TASK;
DROP TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_TASK;
DROP TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_TASK;
DROP TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_TASK;
DROP TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.ROOT_TASK;

DROP PROCEDURE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_SP();
DROP PROCEDURE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_SP();
DROP PROCEDURE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_SP();
DROP PROCEDURE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_SP();

DROP STREAM IF EXISTS DEMO.DT_DEMO_MERGE_ALT.STR_CUSTOMERS;
DROP STREAM IF EXISTS DEMO.DT_DEMO_MERGE_ALT.STR_ORDERS;
DROP STREAM IF EXISTS DEMO.DT_DEMO_MERGE_ALT.STR_PRODUCT_INVENTORY;
DROP STREAM IF EXISTS DEMO.DT_DEMO_MERGE_ALT.STR_CUSTOMER_ORDERS;
DROP STREAM IF EXISTS DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_CUMULATIVE;
DROP STREAM IF EXISTS DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_ALERT;

DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.PRODUCT_INVENTORY_ALERT;
DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE;
DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES;
DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT;
DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS;



/******************************************************************************************
 SECTION 1: Shared Base Tables (in DT_DEMO)
 
 Both pipelines read from the SAME base tables. When you insert new data,
 it flows through the Dynamic Tables automatically AND through the
 MERGE + Streams + Tasks pipeline — side by side.
*******************************************************************************************/
CREATE SCHEMA IF NOT EXISTS DEMO.DT_DEMO;

CREATE TRANSIENT TABLE IF NOT EXISTS DEMO.DT_DEMO.CUSTOMERS AS
SELECT * FROM TABLE(DEMO.DT_DEMO.generate_customer_data(1000)) ORDER BY 1;

CREATE TRANSIENT TABLE IF NOT EXISTS DEMO.DT_DEMO.PRODUCT_INVENTORY AS
SELECT * FROM TABLE(DEMO.DT_DEMO.generate_product_inventory_data(30)) ORDER BY 1;

CREATE TRANSIENT TABLE IF NOT EXISTS DEMO.DT_DEMO.ORDERS AS
SELECT * FROM TABLE(DEMO.DT_DEMO.generate_sales_data(5000));


/******************************************************************************************
 SECTION 2: Target Tables 
 
 With Dynamic Tables, you just write the SELECT and the table is created for you.
 Here we have to manually define every column and type.
*******************************************************************************************/

-- Target 1: CUSTOMER_ORDERS (equivalent to DT customer_orders)
CREATE OR REPLACE TABLE DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS (
    ORDER_ID        NUMBER(38,0),
    CUSTOMER_ID     NUMBER(10,0),
    CUSTOMER_NAME   VARCHAR(100),
    REGION          VARCHAR(50),
    PRODUCT_ID      NUMBER(38,0),
    QUANTITY        NUMBER(38,0),
    ORDER_TOTAL     NUMBER(10,2),
    PURCHASE_DATE   DATE
);

-- Target 2: SALES_REPORT (equivalent to DT sales_report)
CREATE OR REPLACE TABLE DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT (
    CUSTOMER_ID              NUMBER(10,0),
    CUSTOMER_NAME            VARCHAR(100),
    ORDER_ID                 NUMBER(38,0),
    PRODUCT_ID               NUMBER(38,0),
    PRODUCT_NAME             VARCHAR(100),
    ORDER_TOTAL              NUMBER(10,2),
    QUANTITY                 NUMBER(38,0),
    UNITSALESPRICE           NUMBER(16,8),
    PURCHASE_DATE            DATE,
    DAYS_SINCE_LAST_PURCHASE NUMBER(9,0),
    CUSTOMER_SK              VARCHAR
);

-- Target 3a: CUMULATIVE_PURCHASES — DELETE + INSERT for touched customers
--             (same as before; kept as the default name for compatibility)
CREATE OR REPLACE TABLE DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES (
    CUSTOMER_ID      NUMBER(10,0),
    CUSTOMER_NAME    VARCHAR(100),
    TOTAL_SALES      NUMBER(22,2),
    TOTAL_ORDERS     NUMBER(18,0),
    DISTINCT_PRODUCTS NUMBER(18,0)
);

-- Target 3b: Same grain + metrics, maintained with MERGE (+ NOT MATCHED BY SOURCE DELETE)
--             Built in the same proc so the stream is consumed once; compare with 3a in demos.
CREATE OR REPLACE TABLE DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE (
    CUSTOMER_ID       NUMBER(10,0),
    CUSTOMER_NAME     VARCHAR(100),
    TOTAL_SALES       NUMBER(22,2),
    TOTAL_ORDERS      NUMBER(18,0),
    DISTINCT_PRODUCTS NUMBER(18,0)
);

-- Target 4: PRODUCT_INVENTORY_ALERT (equivalent to DT product_inventory_alert)
CREATE OR REPLACE TABLE DEMO.DT_DEMO_MERGE_ALT.PRODUCT_INVENTORY_ALERT (
    PRODUCT_ID       NUMBER(38,0),
    PRODUCT_NAME     VARCHAR(100),
    LATEST_SALES_DATE DATE,
    BEGINING_STOCK   NUMBER(10,2),
    TOTALUNITSOLD    NUMBER(38,0),
    UNITSLEFT        NUMBER(38,2),
    PERCENT_UNITLEFT NUMBER(38,2),
    ROWCREATIONTIME  TIMESTAMP_LTZ(9)
);


/******************************************************************************************
 SECTION 3: Streams (6 total)
 
 With Dynamic Tables, change detection is automatic. Here we must create
 and manage a stream for every source table, PLUS intermediate tables.
 
 We also need TWO streams on SALES_REPORT because a stream can only be 
 consumed once per DML transaction, and two downstream targets need it.
*******************************************************************************************/

-- Streams on shared base tables (in DT_DEMO)
CREATE OR REPLACE STREAM DEMO.DT_DEMO_MERGE_ALT.STR_CUSTOMERS
    ON TABLE DEMO.DT_DEMO.CUSTOMERS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM DEMO.DT_DEMO_MERGE_ALT.STR_ORDERS
    ON TABLE DEMO.DT_DEMO.ORDERS
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM DEMO.DT_DEMO_MERGE_ALT.STR_PRODUCT_INVENTORY
    ON TABLE DEMO.DT_DEMO.PRODUCT_INVENTORY;

-- Stream on intermediate target (CUSTOMER_ORDERS feeds SALES_REPORT)
CREATE OR REPLACE STREAM DEMO.DT_DEMO_MERGE_ALT.STR_CUSTOMER_ORDERS
    ON TABLE DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS
    APPEND_ONLY = TRUE;

-- TWO streams on SALES_REPORT (one per downstream consumer)
CREATE OR REPLACE STREAM DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_CUMULATIVE
    ON TABLE DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_ALERT
    ON TABLE DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
    APPEND_ONLY = TRUE;


/******************************************************************************************
 SECTION 4: Stored Procedures (4 total)
 
 Each procedure contains the MERGE logic that replicates one Dynamic Table.
 With DTs, you write a SELECT. Here you write a full MERGE with match keys,
 update/insert clauses, and handle recomputation of window functions.
*******************************************************************************************/

-- PROCEDURE 1: MERGE_CUSTOMER_ORDERS_SP
-- Equivalent to: CREATE DYNAMIC TABLE customer_orders ... 
--                SELECT ... FROM customers JOIN orders ...
CREATE OR REPLACE PROCEDURE DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_SP()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS AS tgt
    USING (
        SELECT
            s.SALES_DATA:order_id::NUMBER AS ORDER_ID,
            c.CUST_ID AS CUSTOMER_ID,
            c.CUSTOMER_NAME,
            c.REGION,
            s.SALES_DATA:purchase.product_id::NUMBER AS PRODUCT_ID,
            s.SALES_DATA:purchase.quantity::NUMBER AS QUANTITY,
            s.SALES_DATA:purchase.order_total::NUMBER(10,2) AS ORDER_TOTAL,
            s.SALES_DATA:purchase.purchase_date::DATE AS PURCHASE_DATE
        FROM DEMO.DT_DEMO.CUSTOMERS AS c
        JOIN DEMO.DT_DEMO_MERGE_ALT.STR_ORDERS AS s
            ON c.CUST_ID = s.SALES_DATA:custid::NUMBER
    ) AS src
    ON tgt.ORDER_ID = src.ORDER_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.CUSTOMER_ID   = src.CUSTOMER_ID,
        tgt.CUSTOMER_NAME = src.CUSTOMER_NAME,
        tgt.REGION        = src.REGION,
        tgt.PRODUCT_ID    = src.PRODUCT_ID,
        tgt.QUANTITY       = src.QUANTITY,
        tgt.ORDER_TOTAL    = src.ORDER_TOTAL,
        tgt.PURCHASE_DATE  = src.PURCHASE_DATE
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, CUSTOMER_ID, CUSTOMER_NAME, REGION, PRODUCT_ID, QUANTITY, ORDER_TOTAL, PURCHASE_DATE
    ) VALUES (
        src.ORDER_ID, src.CUSTOMER_ID, src.CUSTOMER_NAME, src.REGION, src.PRODUCT_ID,
        src.QUANTITY, src.ORDER_TOTAL, src.PURCHASE_DATE
    );
    RETURN 'MERGE_CUSTOMER_ORDERS completed';
END;
$$;


-- PROCEDURE 2: MERGE_SALES_REPORT_SP
-- Equivalent to: CREATE DYNAMIC TABLE sales_report ...
--                SELECT ... FROM customer_orders JOIN product_inventory ...
-- NOTE: DAYS_SINCE_LAST_PURCHASE uses a window function (LAG), so we must
-- recompute it for ALL rows of affected customers, not just new rows.
CREATE OR REPLACE PROCEDURE DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_SP()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Identify affected customers from new CUSTOMER_ORDERS rows
    CREATE OR REPLACE TEMPORARY TABLE DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUSTOMERS AS
    SELECT DISTINCT CUSTOMER_ID
    FROM DEMO.DT_DEMO_MERGE_ALT.STR_CUSTOMER_ORDERS;

    -- Step 2: Delete existing rows for affected customers (window function must be recomputed)
    DELETE FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
    WHERE CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUSTOMERS);

    -- Step 3: Re-insert fully recomputed rows for affected customers
    INSERT INTO DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
    SELECT
        t1.CUSTOMER_ID,
        t1.CUSTOMER_NAME,
        t1.ORDER_ID,
        t1.PRODUCT_ID,
        p.PRODUCT_NAME,
        t1.ORDER_TOTAL,
        t1.QUANTITY,
        (t1.ORDER_TOTAL / t1.QUANTITY) AS UNITSALESPRICE,
        t1.PURCHASE_DATE,
        DATEDIFF(DAY,
            LAG(t1.PURCHASE_DATE) OVER (PARTITION BY t1.CUSTOMER_ID ORDER BY t1.PURCHASE_DATE ASC, t1.ORDER_ID ASC),
            t1.PURCHASE_DATE
        ) AS DAYS_SINCE_LAST_PURCHASE,
        t1.CUSTOMER_ID || '-' || t1.PRODUCT_ID || '-' || t1.PURCHASE_DATE AS CUSTOMER_SK
    FROM DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS AS t1
    INNER JOIN DEMO.DT_DEMO.PRODUCT_INVENTORY AS p
        ON t1.PRODUCT_ID = p.PRODUCT_ID
    WHERE t1.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUSTOMERS);

    DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUSTOMERS;
    RETURN 'MERGE_SALES_REPORT completed';
END;
$$;


-- PROCEDURE 3: MERGE_CUMULATIVE_PURCHASES_SP
-- Equivalent to: CREATE DYNAMIC TABLE cumulative_purchases ...
--                SELECT ... GROUP BY customer_id, customer_name
--
-- Side-by-side: (A) DELETE + rows for touched customers then INSERT fresh aggregates
--               (B) MERGE recomputed aggregates + delete target rows that no longer qualify
-- One read from STR_SALES_REPORT_CUMULATIVE (consumes the stream); both paths use the same id set.
CREATE OR REPLACE PROCEDURE DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_SP()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUST_AGG AS
    SELECT DISTINCT CUSTOMER_ID
    FROM DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_CUMULATIVE;

    -- (A) DELETE + INSERT into CUMULATIVE_PURCHASES
    DELETE FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
    WHERE CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUST_AGG);

    INSERT INTO DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
    SELECT
        a.CUSTOMER_ID,
        a.CUSTOMER_NAME,
        SUM(a.ORDER_TOTAL)           AS TOTAL_SALES,
        COUNT(a.CUSTOMER_SK)         AS TOTAL_ORDERS,
        COUNT(DISTINCT a.PRODUCT_ID) AS DISTINCT_PRODUCTS
    FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT AS a
    WHERE a.PURCHASE_DATE >= DATEADD('MM', -6, GETDATE())
      AND a.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUST_AGG)
    GROUP BY
        a.CUSTOMER_ID,
        a.CUSTOMER_NAME;

    -- (B) MERGE into CUMULATIVE_PURCHASES_MERGE (same source query as the INSERT above)
    MERGE INTO DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE AS tgt
    USING (
        SELECT
            a.CUSTOMER_ID,
            a.CUSTOMER_NAME,
            SUM(a.ORDER_TOTAL)           AS TOTAL_SALES,
            COUNT(a.CUSTOMER_SK)         AS TOTAL_ORDERS,
            COUNT(DISTINCT a.PRODUCT_ID) AS DISTINCT_PRODUCTS
        FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT AS a
        WHERE a.PURCHASE_DATE >= DATEADD('MM', -6, GETDATE())
          AND a.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUST_AGG)
        GROUP BY
            a.CUSTOMER_ID,
            a.CUSTOMER_NAME
    ) AS src
    ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.CUSTOMER_NAME      = src.CUSTOMER_NAME,
        tgt.TOTAL_SALES        = src.TOTAL_SALES,
        tgt.TOTAL_ORDERS       = src.TOTAL_ORDERS,
        tgt.DISTINCT_PRODUCTS  = src.DISTINCT_PRODUCTS
    WHEN NOT MATCHED THEN INSERT (
        CUSTOMER_ID, CUSTOMER_NAME, TOTAL_SALES, TOTAL_ORDERS, DISTINCT_PRODUCTS
    ) VALUES (
        src.CUSTOMER_ID, src.CUSTOMER_NAME, src.TOTAL_SALES, src.TOTAL_ORDERS, src.DISTINCT_PRODUCTS
    );

    -- Delete affected customers that no longer qualify (aged out of the 6-month window)
    DELETE FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE
    WHERE CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUST_AGG)
      AND CUSTOMER_ID NOT IN (
          SELECT CUSTOMER_ID FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
          WHERE PURCHASE_DATE >= DATEADD('MM', -6, GETDATE())
      );

    DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_CUST_AGG;
    RETURN 'MERGE_CUMULATIVE_PURCHASES completed (DELETE+INSERT + MERGE tables)';
END;
$$;


-- PROCEDURE 4: MERGE_PRODUCT_INVENTORY_ALERT_SP
-- Equivalent to: CREATE DYNAMIC TABLE product_inventory_alert ...
--                SELECT ... QUALIFY ROW_NUMBER() OVER (...) = 1
-- Window function + QUALIFY requires full recompute per affected product.
CREATE OR REPLACE PROCEDURE DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_SP()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Identify products with changed sales data
    CREATE OR REPLACE TEMPORARY TABLE DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_PRODUCTS AS
    SELECT DISTINCT PRODUCT_ID
    FROM DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_ALERT;

    -- Delete + re-insert for affected products
    DELETE FROM DEMO.DT_DEMO_MERGE_ALT.PRODUCT_INVENTORY_ALERT
    WHERE PRODUCT_ID IN (SELECT PRODUCT_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_PRODUCTS);

    INSERT INTO DEMO.DT_DEMO_MERGE_ALT.PRODUCT_INVENTORY_ALERT
    SELECT
        S.PRODUCT_ID,
        S.PRODUCT_NAME,
        S.PURCHASE_DATE AS LATEST_SALES_DATE,
        P.STOCK AS BEGINING_STOCK,
        SUM(S.QUANTITY) OVER (PARTITION BY S.PRODUCT_ID ORDER BY S.PURCHASE_DATE) AS TOTALUNITSOLD,
        (P.STOCK - TOTALUNITSOLD) AS UNITSLEFT,
        ROUND(((P.STOCK - TOTALUNITSOLD) / P.STOCK) * 100, 2) AS PERCENT_UNITLEFT,
        CURRENT_TIMESTAMP() AS ROWCREATIONTIME
    FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT AS S
    JOIN DEMO.DT_DEMO.PRODUCT_INVENTORY AS P
        ON S.PRODUCT_ID = P.PRODUCT_ID
    WHERE S.PRODUCT_ID IN (SELECT PRODUCT_ID FROM DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_PRODUCTS)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY S.PRODUCT_ID ORDER BY S.PURCHASE_DATE DESC) = 1;

    DROP TABLE IF EXISTS DEMO.DT_DEMO_MERGE_ALT._TMP_AFFECTED_PRODUCTS;
    RETURN 'MERGE_PRODUCT_INVENTORY_ALERT completed';
END;
$$;


/******************************************************************************************
 SECTION 5: Task DAG (5 tasks)
 
 With Dynamic Tables, dependencies are handled automatically via DOWNSTREAM lag.
 Here we must manually wire a task tree with AFTER clauses and stream checks.
 
 ROOT_TASK (1 min) 
     └──> MERGE_CUSTOMER_ORDERS_TASK
              └──> MERGE_SALES_REPORT_TASK
                       ├──> MERGE_CUMULATIVE_PURCHASES_TASK
                       └──> MERGE_PRODUCT_INVENTORY_ALERT_TASK
*******************************************************************************************/

-- Root task: scheduler entry point (does nothing itself)
CREATE OR REPLACE TASK DEMO.DT_DEMO_MERGE_ALT.ROOT_TASK
    WAREHOUSE = WH_XS
    SCHEDULE = '1 MINUTE'
AS
    SELECT 1;

-- Layer 1: Combine customers + orders
CREATE OR REPLACE TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_TASK
    WAREHOUSE = WH_XS
    AFTER DEMO.DT_DEMO_MERGE_ALT.ROOT_TASK
    WHEN SYSTEM$STREAM_HAS_DATA('DEMO.DT_DEMO_MERGE_ALT.STR_ORDERS')
AS
    CALL DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_SP();

-- Layer 2: Add product info + calculations
CREATE OR REPLACE TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_TASK
    WAREHOUSE = WH_XS
    AFTER DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_TASK
    WHEN SYSTEM$STREAM_HAS_DATA('DEMO.DT_DEMO_MERGE_ALT.STR_CUSTOMER_ORDERS')
AS
    CALL DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_SP();

-- Layer 3a: Aggregate by customer
CREATE OR REPLACE TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_TASK
    WAREHOUSE = WH_XS
    AFTER DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_TASK
    WHEN SYSTEM$STREAM_HAS_DATA('DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_CUMULATIVE')
AS
    CALL DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_SP();

-- Layer 3b: Product inventory alerts
CREATE OR REPLACE TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_TASK
    WAREHOUSE = WH_XS
    AFTER DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_TASK
    WHEN SYSTEM$STREAM_HAS_DATA('DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_ALERT')
AS
    CALL DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_SP();


/******************************************************************************************
 SECTION 6: Initial Data Backfill
 
 With Dynamic Tables, data is populated automatically via INITIALIZE = ON_CREATE.
 Here we must manually write INSERT...SELECT for each table in dependency order.
 
 We can't use the stored procedures here because streams were created AFTER the
 base table CTAS statements, so they have no data to consume. Another gotcha
 you don't have to think about with Dynamic Tables.
*******************************************************************************************/

-- Backfill 1: CUSTOMER_ORDERS (same transformation as the DT)
INSERT INTO DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS
SELECT
    s.SALES_DATA:order_id::NUMBER AS ORDER_ID,
    c.CUST_ID AS CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.REGION,
    s.SALES_DATA:purchase.product_id::NUMBER AS PRODUCT_ID,
    s.SALES_DATA:purchase.quantity::NUMBER AS QUANTITY,
    s.SALES_DATA:purchase.order_total::NUMBER(10,2) AS ORDER_TOTAL,
    s.SALES_DATA:purchase.purchase_date::DATE AS PURCHASE_DATE
FROM DEMO.DT_DEMO.CUSTOMERS AS c
JOIN DEMO.DT_DEMO.ORDERS AS s
    ON c.CUST_ID = s.SALES_DATA:custid::NUMBER;

-- Backfill 2: SALES_REPORT
INSERT INTO DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
SELECT
    t1.CUSTOMER_ID,
    t1.CUSTOMER_NAME,
    t1.ORDER_ID,
    t1.PRODUCT_ID,
    p.PRODUCT_NAME,
    t1.ORDER_TOTAL,
    t1.QUANTITY,
    (t1.ORDER_TOTAL / t1.QUANTITY) AS UNITSALESPRICE,
    t1.PURCHASE_DATE,
    DATEDIFF(DAY,
        LAG(t1.PURCHASE_DATE) OVER (PARTITION BY t1.CUSTOMER_ID ORDER BY t1.PURCHASE_DATE ASC, t1.ORDER_ID ASC),
        t1.PURCHASE_DATE
    ) AS DAYS_SINCE_LAST_PURCHASE,
    t1.CUSTOMER_ID || '-' || t1.PRODUCT_ID || '-' || t1.PURCHASE_DATE AS CUSTOMER_SK
FROM DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS AS t1
INNER JOIN DEMO.DT_DEMO.PRODUCT_INVENTORY AS p
    ON t1.PRODUCT_ID = p.PRODUCT_ID;

-- Backfill 3: CUMULATIVE_PURCHASES (both tables — same query)
INSERT INTO DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
SELECT
    a.CUSTOMER_ID,
    a.CUSTOMER_NAME,
    SUM(a.ORDER_TOTAL) AS TOTAL_SALES,
    COUNT(a.CUSTOMER_SK) AS TOTAL_ORDERS,
    COUNT(DISTINCT a.PRODUCT_ID) AS DISTINCT_PRODUCTS
FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT AS a
WHERE a.PURCHASE_DATE >= DATEADD('MM', -6, GETDATE())
GROUP BY a.CUSTOMER_ID, a.CUSTOMER_NAME;

INSERT INTO DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE
SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES;

-- Backfill 4: PRODUCT_INVENTORY_ALERT
INSERT INTO DEMO.DT_DEMO_MERGE_ALT.PRODUCT_INVENTORY_ALERT
SELECT
    S.PRODUCT_ID,
    S.PRODUCT_NAME,
    S.PURCHASE_DATE AS LATEST_SALES_DATE,
    P.STOCK AS BEGINING_STOCK,
    SUM(S.QUANTITY) OVER (PARTITION BY S.PRODUCT_ID ORDER BY S.PURCHASE_DATE) AS TOTALUNITSOLD,
    (P.STOCK - TOTALUNITSOLD) AS UNITSLEFT,
    ROUND(((P.STOCK - TOTALUNITSOLD) / P.STOCK) * 100, 2) AS PERCENT_UNITLEFT,
    CURRENT_TIMESTAMP() AS ROWCREATIONTIME
FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT AS S
JOIN DEMO.DT_DEMO.PRODUCT_INVENTORY AS P
    ON S.PRODUCT_ID = P.PRODUCT_ID
QUALIFY ROW_NUMBER() OVER (PARTITION BY S.PRODUCT_ID ORDER BY S.PURCHASE_DATE DESC) = 1;

-- Advance intermediate streams past the backfill data so they don't re-process it
-- (Yet another thing you don't need to worry about with Dynamic Tables)
CREATE OR REPLACE TEMPORARY TABLE DEMO.DT_DEMO_MERGE_ALT._TMP_ADVANCE1 AS
    SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.STR_CUSTOMER_ORDERS;
CREATE OR REPLACE TEMPORARY TABLE DEMO.DT_DEMO_MERGE_ALT._TMP_ADVANCE2 AS
    SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_CUMULATIVE;
CREATE OR REPLACE TEMPORARY TABLE DEMO.DT_DEMO_MERGE_ALT._TMP_ADVANCE3 AS
    SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.STR_SALES_REPORT_ALERT;

-- Verify initial data
SELECT 'CUSTOMER_ORDERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS
UNION ALL
SELECT 'SALES_REPORT', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
UNION ALL
SELECT 'CUMULATIVE_PURCHASES (DELETE+INSERT)', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
UNION ALL
SELECT 'CUMULATIVE_PURCHASES_MERGE (MERGE)', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE
UNION ALL
SELECT 'PRODUCT_INVENTORY_ALERT', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.PRODUCT_INVENTORY_ALERT;

-- Side-by-side: row counts should match; EXCEPT should return no rows when both paths agree
SELECT
    'DELETE+INSERT' AS AGG_PATTERN,
    COUNT(*)        AS ROW_CNT
FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
UNION ALL
SELECT 'MERGE', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE;

SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
MINUS
SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE;


/******************************************************************************************
 SECTION 7: Resume Tasks (start the pipeline)
 
 Tasks must be resumed in reverse dependency order (leaf tasks first, root last).
 With Dynamic Tables, you just CREATE and they start automatically.
*******************************************************************************************/
ALTER TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_TASK RESUME;
ALTER TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_TASK RESUME;
ALTER TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_TASK RESUME;
ALTER TASK DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_TASK RESUME;
ALTER TASK DEMO.DT_DEMO_MERGE_ALT.ROOT_TASK RESUME;


/******************************************************************************************
 SECTION 8: Demo - Add New Data (triggers the pipeline)
 
 Same function calls as the DT demo. Insert new sales data and watch it propagate.
*******************************************************************************************/
INSERT INTO DEMO.DT_DEMO.ORDERS
    SELECT * FROM TABLE(DEMO.DT_DEMO.generate_sales_data(2000));

-- Check raw base table (shared — feeds both DTs and MERGE pipeline)
SELECT COUNT(*) FROM DEMO.DT_DEMO.ORDERS;

-- After ~1 minute, check target tables for updated counts
SELECT 'CUSTOMER_ORDERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM DEMO.DT_DEMO_MERGE_ALT.CUSTOMER_ORDERS
UNION ALL
SELECT 'SALES_REPORT', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.SALES_REPORT
UNION ALL
SELECT 'CUMULATIVE_PURCHASES (DELETE+INSERT)', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
UNION ALL
SELECT 'CUMULATIVE_PURCHASES_MERGE (MERGE)', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE
UNION ALL
SELECT 'PRODUCT_INVENTORY_ALERT', COUNT(*) FROM DEMO.DT_DEMO_MERGE_ALT.PRODUCT_INVENTORY_ALERT;

-- Re-check parity after incremental load (expect 0 rows from each MINUS when tasks have run)
SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES
MINUS
SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE;
SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES_MERGE
MINUS
SELECT * FROM DEMO.DT_DEMO_MERGE_ALT.CUMULATIVE_PURCHASES;


/******************************************************************************************
 SECTION 9: Cleanup - Suspend tasks (leave objects for demo purposes)
*******************************************************************************************/
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.ROOT_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUSTOMER_ORDERS_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_SALES_REPORT_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_CUMULATIVE_PURCHASES_TASK SUSPEND;
ALTER TASK IF EXISTS DEMO.DT_DEMO_MERGE_ALT.MERGE_PRODUCT_INVENTORY_ALERT_TASK SUSPEND;

-- Full teardown (uncomment to drop everything)
-- DROP SCHEMA IF EXISTS DEMO.DT_DEMO_MERGE_ALT CASCADE;
