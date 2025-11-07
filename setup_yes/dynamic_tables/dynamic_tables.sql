/******************************************************************************************
 Pre-Demo Cleanup - before demo. Kept objects for demo purposes.
*******************************************************************************************/
--CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.DT_DEMO;
USE SCHEMA DEMO.DT_DEMO;

--USE WAREHOUSE WH_SMALL; 
USE WAREHOUSE WH_XS;

DROP DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.CUMULATIVE_PURCHASES;
DROP DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.CUSTOMER_SALES_DATA_HISTORY;
DROP DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.PRODUCT_INV_ALERT;
DROP DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.SALES_REPORT;

DROP TABLE IF EXISTS DEMO.DT_DEMO.CUST_INFO;
DROP TABLE IF EXISTS DEMO.DT_DEMO.PRODUCT_STOCK_INV;
DROP TABLE IF EXISTS DEMO.DT_DEMO.SALES_DATA;

-- Create tables and insert records 
create or replace transient table cust_info as select * from table(demo.dt_demo.gen_cust_info(1000)) order by 1;
create or replace transient table product_stock_inv as select * from table(demo.dt_demo.gen_prod_inv(30)) order by 1;
create or replace transient table sales_data as select * from table(demo.dt_demo.gen_cust_purchase(10000,10));

/******************************************************************************************
 DYNAMIC TABLES 
*******************************************************************************************/

-- customer information table, each customer has spending limits
select * from demo.dt_demo.cust_info order by cust_id;

-- product stock table, each product has stock level from fulfilment day

select * from demo.dt_demo.product_stock_inv;

-- sales data for products purchsaed online by various customers
select * from demo.dt_demo.sales_data;

/******************************************************************************************
 Combine customer and sales data. 
*******************************************************************************************/
CREATE OR REPLACE DYNAMIC TABLE demo.dt_demo.customer_sales_data_history
    LAG='DOWNSTREAM'
    WAREHOUSE= wh_xs
AS
SELECT  
    c.cust_id as customer_id,
    c.customer_name,
    s.purchase:"prodid"::number(5) as product_id,
    s.purchase:"purchase_amount"::number(10) as saleprice,
    s.purchase:"quantity"::number(5) as quantity,
    s.purchase:"purchase_date"::date as salesdate
FROM
    demo.dt_demo.cust_info c 
    INNER JOIN demo.dt_demo.sales_data s 
        ON c.cust_id = s.custid
;

-- quick check
select * from demo.dt_demo.customer_sales_data_history;
select count(*) from demo.dt_demo.customer_sales_data_history;


/******************************************************************************************
 Create a sales report by adding in product information.  
*******************************************************************************************/
CREATE OR REPLACE DYNAMIC TABLE demo.dt_demo.sales_report
    LAG = '1 MINUTE'
    WAREHOUSE=wh_xs
    REFRESH_MODE = INCREMENTAL
AS
    SELECT
        t1.customer_id,
        t1.customer_name, 
        t1.product_id,
        p.product_name,
        t1.saleprice,
        t1.quantity,
        (t1.saleprice/t1.quantity) as unitsalesprice,
        t1.salesdate,
        DATEDIFF(DAY,LAG(salesdate) OVER (PARTITION BY t1.customer_id ORDER BY salesdate ASC),t1.salesdate) AS days_since_last_purchase,
        customer_id || '-' || t1.product_id  || '-' || t1.salesdate AS CUSTOMER_SK,
    FROM 
        demo.dt_demo.customer_sales_data_history t1 
        INNER JOIN demo.dt_demo.product_stock_inv p 
            ON t1.product_id = p.product_id      
;

select * from demo.dt_demo.sales_report;

select count(*) from demo.dt_demo.sales_report;

/******************************************************************************************
 Go add 5000 new sales to the sales table.  
*******************************************************************************************/
-- Add new records
insert into demo.dt_demo.sales_data select * from table(demo.dt_demo.gen_cust_purchase(5000,2));

-- Check raw base table
select count(*) from demo.dt_demo.sales_data;

-- Check Dynamic Tables after a minute
select count(*) from demo.dt_demo.customer_sales_data_history;
select count(*) from demo.dt_demo.sales_report;
select * from demo.dt_demo.sales_report;


/******************************************************************************************
 Create a table that aggregates sales by customer + month.  
*******************************************************************************************/
CREATE OR REPLACE DYNAMIC TABLE demo.dt_demo.cumulative_purchases
    LAG = '1 MINUTE'
    WAREHOUSE=wh_xs
AS
    SELECT  
        TO_CHAR(salesdate, 'YYYY-MM') AS month_year,
        a.customer_id,
        customer_name,
        SUM(a.saleprice) AS total_monthly_sales,
        COUNT(CUSTOMER_SK) AS monthly_orders, 
        COUNT(DISTINCT PRODUCT_ID) AS distinct_products,
    FROM 
        demo.dt_demo.sales_report AS a
    GROUP BY  
        a.customer_id,
        a.customer_name,
        month_year
;

SELECT * 
FROM  
    demo.dt_demo.cumulative_purchases 
ORDER BY 
    CUSTOMER_ID, MONTH_YEAR
;

/******************************************************************************************
 Create a table to quickly check our product inventory. (create an alert off this table.)
*******************************************************************************************/
CREATE OR REPLACE DYNAMIC TABLE demo.dt_demo.PRODUCT_INV_ALERT
    LAG = '1 MINUTE'
    WAREHOUSE=wh_xs
    --REFRESH_MODE= AUTO
AS
    SELECT 
        S.PRODUCT_ID, 
        S.PRODUCT_NAME,SALESDATE AS LATEST_SALES_DATE,
        STOCK AS BEGINING_STOCK,
        SUM(S.QUANTITY) OVER (PARTITION BY S.PRODUCT_ID ORDER BY SALESDATE) TOTALUNITSOLD, 
        (STOCK - TOTALUNITSOLD) AS UNITSLEFT,
        ROUND(((STOCK-TOTALUNITSOLD)/STOCK) *100,2) PERCENT_UNITLEFT,
        CURRENT_TIMESTAMP() AS ROWCREATIONTIME
    FROM 
        demo.dt_demo.SALES_REPORT S 
        JOIN demo.dt_demo.PRODUCT_STOCK_INV AS p 
            ON S.PRODUCT_ID = p.product_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY S.PRODUCT_ID ORDER BY SALESDATE DESC) = 1
;

-- check products with low inventory and alert
select * from demo.dt_demo.product_inv_alert;

select * 
from demo.dt_demo.product_inv_alert 
--where percent_unitleft < 10
order by unitsleft;

-- Add new records
insert into sales_data select * from table(demo.dt_demo.gen_cust_purchase(5000,2));

/******************************************************************************************
 END
*******************************************************************************************/

/******************************************************************************************
 Cleanup - leave object for doing demos without running code. 
*******************************************************************************************/
ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.CUMULATIVE_PURCHASES SUSPEND;
ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.CUSTOMER_SALES_DATA_HISTORY SUSPEND;
ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.PROD_INV_ALERT SUSPEND;
ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.SALES_REPORT SUSPEND; 

-- ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.CUMULATIVE_PURCHASES RESUME;
-- ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.CUSTOMER_SALES_DATA_HISTORY RESUME;
-- ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.PROD_INV_ALERT RESUME;
-- ALTER DYNAMIC TABLE IF EXISTS DEMO.DT_DEMO.SALES_REPORT RESUME; 


/****************************************************************************************************************
 FUNCTIONS FOR GENERATING DATA BELOW. ALSO IN QUICKSTART
 https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/index.html?index=..%2F..index#1
******************************************************************************************************************/
/*
--FUNCTION 1
CREATE OR REPLACE FUNCTION gen_cust_info(num_records NUMBER)
RETURNS TABLE (
    cust_id NUMBER(10), 
    customer_name VARCHAR(100), 
    tier VARCHAR(20), 
    industry VARCHAR(50),
    region VARCHAR(50)
)
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
HANDLER='CustTab'
PACKAGES = ('Faker')
AS $$
from faker import Faker
import random

fake = Faker()
tier_options = ['Basic', 'Gold', 'Premium']
industry_options = ['Retail', 'Technology', 'Healthcare', 'Finance', 'Manufacturing']
region_options = ['North America', 'Europe', 'Asia', 'South America', 'Australia']

class CustTab:
    def process(self, num_records):
        customer_id = 1000  # Starting customer ID                 
        for _ in range(num_records):
            cust_id = customer_id + 1
            customer_name = fake.company()  # Generate a fake business name
            tier = random.choice(tier_options)  # Randomly assign a tier
            industry = random.choice(industry_options)  # Random industry
            region = random.choice(region_options)  # Random region
            customer_id += 1
            yield (cust_id, customer_name, tier, industry, region)

$$;



--FUNCTION 2
create or replace function gen_prod_inv(num_records number)
returns table (pid number(10), pname varchar(100), stock number(10,2), stockdate date)
language python
runtime_version=3.8
handler='ProdTab'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta
fake = Faker()

class ProdTab:
    # Generate multiple product records
    def process(self, num_records):
        product_id = 100 # Starting customer ID                 
        for _ in range(num_records):
            pid = product_id + 1
            pname = fake.catch_phrase()
            stock = round(random.uniform(500, 1000),0)
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (3 months from now)
            min_date = current_date - timedelta(days=90)
            
            # Generate a random date within the date range
            stockdate = fake.date_between_dates(min_date,current_date)

            product_id += 1
            yield (pid,pname,stock,stockdate)

$$;

CREATE OR REPLACE FUNCTION gen_prod_inv(num_records NUMBER)
RETURNS TABLE (product_id NUMBER(10), product_name VARCHAR(100), stock NUMBER(10,2), stockdate DATE)
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
HANDLER='ProdTab'
PACKAGES = ('Faker')
AS $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class ProdTab:
    # Generate multiple product records
    def process(self, num_records):
        product_id = 100  # Starting product ID
        for _ in range(num_records):
            product_id = product_id + 1
            # Generate a more product-like name by combining product descriptors
            product_name = f"{fake.word().capitalize()} {random.randint(1000, 9999)} {fake.word().capitalize()} {fake.word().capitalize()}"
            stock = round(random.uniform(1500, 2500), 0)
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (7 days from now)
            min_date = current_date - timedelta(days=7)
            
            # Generate a random date within the date range
            stockdate = fake.date_between_dates(min_date, current_date)

            yield (product_id, product_name, stock, stockdate)

$$;


--FUNCTION 3
create or replace function gen_cust_purchase(num_records number,ndays number)
returns table (custid number(10), purchase variant)
language python
runtime_version=3.8
handler='genCustPurchase'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    # Generate multiple customer purchase records
    def process(self, num_records,ndays):       
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            
            #print(c_id)
            customer_purchase = {
                'custid': c_id,
                'purchased': []
            }
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (days from now)
            min_date = current_date - timedelta(days=ndays)
            
            # Generate a random date within the date range
            pdate = fake.date_between_dates(min_date,current_date)
            
            purchase = {
                'prodid': fake.random_int(min=101, max=130),
                'quantity': fake.random_int(min=1, max=5),
                'purchase_amount': round(random.uniform(10, 1000),2),
                'purchase_date': pdate
            }
            customer_purchase['purchased'].append(purchase)
            
            #customer_purchases.append(customer_purchase)
            yield (c_id,purchase)

$$;
*/



