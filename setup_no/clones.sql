-- our Accountadmin role will be used for this demo. Really could be any role that has permissions to create a db + schema + table. 
USE ROLE accountadmin;

CREATE DATABASE IF NOT EXISTS DEMO_DB; 
USE DATABASE DEMO_DB; 
CREATE SCHEMA IF NOT EXISTS DEMO_SCHEMA;
USE SCHEMA DEMO_SCHEMA; 

-- Create the Customers table
CREATE OR REPLACE TABLE Customers (
    CustomerID INT AUTOINCREMENT PRIMARY KEY,
    Name STRING,
    Industry STRING,
    DurationAsCustomer INT -- Duration in years
);

-- Insert dummy data
INSERT INTO Customers (Name, Industry, DurationAsCustomer) VALUES
    ('Acme Corp', 'Technology', 5),
    ('Greenfield Logistics', 'Transportation', 3),
    ('Summit Financial', 'Finance', 7),
    ('Bright Horizons Marketing', 'Marketing', 2),
    ('Everest Publishing', 'Publishing', 4),
    ('Insight Consulting Group', 'Consulting', 6);

-- Verify the data
SELECT * FROM Customers;

-- Create a clone of the Customers table
CREATE OR REPLACE TABLE Customers_Dev CLONE Customers;

-- Verify the cloned table
SELECT * FROM Customers_Dev;

-- Update the cloned table to convert DurationAsCustomer from years to months
UPDATE Customers_Dev
SET DurationAsCustomer = DurationAsCustomer * 12;

-- Verify the update
SELECT * FROM Customers_Dev;

-- Now that we've made the appropriate updates, tested in our Dev clone, we can promote that up to production. We do this by swapping them. 
ALTER TABLE Customers_Dev SWAP WITH Customers;

-- let's confirm the production customers table has the updated column in place
SELECT * FROM Customers; 

-- We now see that the Customer_Dev table has the "DurationAsCustomer" in years instead of months. We can drop this table. 
SELECT * FROM Customers_Dev;

-- looks great, let's now drop the Development Table. We can always recreate another clone in the future for testing. 
DROP TABLE Customers_Dev;

-- END of demo script. Since this was a demo. Let's clean up the environment and drop the Demo database  to clean up our account. 
DROP DATABASE DEMO_DB;