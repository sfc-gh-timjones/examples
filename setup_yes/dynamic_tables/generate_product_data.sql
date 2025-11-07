/****************************************************************************************************************
 FUNCTIONS FOR GENERATING DATA BELOW. ALSO IN QUICKSTART
 https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/index.html?index=..%2F..index#1
******************************************************************************************************************/

CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.DT_DEMO;
USE DATABASE DEMO;
USE SCHEMA DEMO.DT_DEMO;

--FUNCTION 2
create or replace function generate_product_inventory_data(num_records number)
returns table (
    product_id number(10), 
    product_name varchar(100), 
    stock number(10,2), 
    stockdate date
)
language python
runtime_version=3.12
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
