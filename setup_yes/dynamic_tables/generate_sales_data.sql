/****************************************************************************************************************
 FUNCTIONS FOR GENERATING DATA BELOW. ALSO IN QUICKSTART
 https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/index.html?index=..%2F..index#1
******************************************************************************************************************/

CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.DT_DEMO;
USE DATABASE DEMO;
USE SCHEMA DEMO.DT_DEMO;

--FUNCTION 3
create or replace function generate_sales_data(num_records number)
returns table (
    sales_data variant
)
language python
runtime_version=3.12
handler='genCustPurchase'
packages = ('Faker')
as $$
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

class genCustPurchase:
    # Generate multiple customer purchase records
    def process(self, num_records):       
        # Create base timestamp in milliseconds for unique order IDs
        base_timestamp = int(datetime.now().timestamp() * 1000)
        order_counter = 0
        for _ in range(num_records):
            c_id = fake.random_int(min=1001, max=1999)
            order_id = base_timestamp + order_counter
            
            # Get the current date
            current_date = datetime.now()
            
            # Calculate the maximum date (2 days back from now)
            min_date = current_date - timedelta(days=2)
            
            # Generate a random date within the date range
            pdate = fake.date_between_dates(min_date,current_date)
            
            # Create comprehensive sales data JSON
            sales_record = {
                'order_id': order_id,
                'custid': c_id,
                'purchase': {
                    'product_id': fake.random_int(min=101, max=130),
                    'quantity': fake.random_int(min=1, max=5),
                    'order_total': float(round(random.uniform(10, 1000), 2)),
                    'purchase_date': pdate.isoformat()  # Convert date to string for JSON
                }
            }
            
            yield (sales_record,)
            order_counter += 1

$$;
