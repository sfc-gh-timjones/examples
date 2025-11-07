/****************************************************************************************************************
 FUNCTIONS FOR GENERATING DATA BELOW. ALSO IN QUICKSTART
 https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/index.html?index=..%2F..index#1
******************************************************************************************************************/
use database demo;
use schema dt_demo;
--FUNCTION 3
create or replace function generate_sales_data(num_records number,ndays number)
returns table (custid number(10), purchase variant)
language python
runtime_version=3.9
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
