/****************************************************************************************************************
 FUNCTIONS FOR GENERATING DATA BELOW. ALSO IN QUICKSTART
 https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/index.html?index=..%2F..index#1
******************************************************************************************************************/

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