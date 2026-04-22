/** OVERVIEW:

  Snowflake System Defined Role Definitions:
   1 - ORGADMIN: Role that manages operations at the organization level.
   2 - ACCOUNTADMIN: Role that encapsulates the SYSADMIN and SECURITYADMIN system-defined roles.
        It is the top-level role in the system and should be granted only to a limited/controlled number of users
        in your account.
   3 - SECURITYADMIN: Role that can manage any object grant globally, as well as create, monitor,
      and manage users and roles.
   4 - USERADMIN: Role that is dedicated to user and role management only.
   5 - SYSADMIN: Role that has privileges to create warehouses and databases in an account.
      If, as recommended, you create a role hierarchy that ultimately assigns all custom roles to the SYSADMIN role, this role also has
      the ability to grant privileges on warehouses, databases, and other objects to other roles.
   6 - PUBLIC: Pseudo-role that is automatically granted to every user and every role in your account. The PUBLIC role can own securable
      objects, just like any other role; however, the objects owned by the role are available to every other
      user and role in your account.

                            +---------------+
                            | ACCOUNTADMIN  |
                            +---------------+
                              ^    ^     ^
                              |    |     |
                +-------------+-+  |    ++-------------+
                | SECURITYADMIN |  |    |   SYSADMIN   |<------------+
                +---------------+  |    +--------------+             |
                        ^          |     ^        ^                  |
                        |          |     |        |                  |
                +-------+-------+  |     |  +-----+-------+  +-------+-----+
                |   USERADMIN   |  |     |  | CUSTOM ROLE |  | CUSTOM ROLE |
                +---------------+  |     |  +-------------+  +-------------+
                        ^          |     |      ^              ^      ^
                        |          |     |      |              |      |
                        |          |     |      |              |    +-+-----------+
                        |          |     |      |              |    | CUSTOM ROLE |
                        |          |     |      |              |    +-------------+
                        |          |     |      |              |           ^
                        |          |     |      |              |           |
                        +----------+-----+---+--+--------------+-----------+
                                             |
                                        +----+-----+
                                        |  PUBLIC  |
                                        +----------+
                                        
=============================================================
  SNOWFLAKE RBAC DEMO
  Demonstrates role-based access control with:
  - Role creation & hierarchy
  - Warehouse access control
  - Object creation privileges
  - Read-only grants & future grants
  - Privilege denial (INSERT fails for read-only role)

  Run this script sequentially in a Snowsight worksheet.

  https://docs.snowflake.com/en/user-guide/security-access-control-privileges#all-privileges-alphabetical
=============================================================
*/

-- =========================================================
-- STEP 1: ACCOUNTADMIN Creates Roles & Establishes Hierarchy
-- =========================================================
USE ROLE ACCOUNTADMIN;

USE SECONDARY ROLES NONE;

CREATE OR REPLACE ROLE ENGINEER_ROLE;
CREATE OR REPLACE ROLE INTERN_ROLE;

GRANT ROLE ENGINEER_ROLE TO ROLE ACCOUNTADMIN;
GRANT ROLE INTERN_ROLE TO ROLE ACCOUNTADMIN;

-- =========================================================
-- STEP 2: ACCOUNTADMIN Creates Warehouses
-- =========================================================
CREATE OR REPLACE WAREHOUSE ENG_DEV_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 30 --suspend after 30 second of inactivity.
    AUTO_RESUME = TRUE;

CREATE OR REPLACE WAREHOUSE INTERN_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 30 --suspend after 30 second of inactivity.
    AUTO_RESUME = TRUE;

CREATE OR REPLACE RESOURCE MONITOR INTERN_WH_MONITOR
    WITH CREDIT_QUOTA = 25
    FREQUENCY = WEEKLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE INTERN_WH SET RESOURCE_MONITOR = INTERN_WH_MONITOR;

/*=========================================================
STEP 3: Grant Warehouse Privileges


 Snowflake Warehouse Privilege Grants:

https://docs.snowflake.com/en/user-guide/security-access-control-privileges#virtual-warehouse-privileges
 
  1 - MODIFY: Enables altering any properties of a warehouse, including changing its size.
  2 - MONITOR: Enables viewing current and past queries executed on a warehouse as well as usage
       statistics on that warehouse.
  3 - OPERATE: Enables changing the state of a warehouse (stop, start, suspend, resume). In addition,
       enables viewing current and past queries executed on a warehouse and aborting any executing queries.
  4 - USAGE: Enables using a virtual warehouse and, as a result, executing queries on the warehouse.
       If the warehouse is configured to auto-resume when a SQL statement is submitted to it, the warehouse
       resumes automatically and executes the statement.
  5 - ALL: Grants all privileges, except OWNERSHIP, on the warehouse.


   ENGINEER_ROLE  -> USAGE, OPERATE, MODIFY, MONITOR on ENG_DEV_WH
   ENGINEER_ROLE  -> ALL PRIVILEGES on INTERN_WH
   INTERN_ROLE    -> USAGE only on INTERN_WH
===========================================================*/

GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE ENG_DEV_WH TO ROLE ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON WAREHOUSE INTERN_WH TO ROLE ENGINEER_ROLE;

GRANT USAGE ON WAREHOUSE INTERN_WH TO ROLE INTERN_ROLE;

-- =========================================================
-- STEP 4: Grant Object Creation Privileges to ENGINEER_ROLE
-- =========================================================
GRANT CREATE DATABASE ON ACCOUNT TO ROLE ENGINEER_ROLE;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE ENGINEER_ROLE;

-- =========================================================
-- STEP 5: ENGINEER_ROLE Creates a Database, Table, and View
-- =========================================================
USE ROLE ENGINEER_ROLE;
USE WAREHOUSE ENG_DEV_WH;

CREATE OR REPLACE DATABASE ENGINEERING_DB;
CREATE OR REPLACE SCHEMA ENGINEERING_DB.ENG_SCHEMA;

CREATE OR REPLACE TABLE ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS (
    ID INT,
    PROJECT_NAME VARCHAR,
    STATUS VARCHAR,
    BUDGET NUMBER(12,2),
    LEAD_NAME VARCHAR
);

INSERT INTO ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS VALUES
    (1, 'Data Pipeline',   'Active',   150000, 'Alice'),
    (2, 'ML Platform',     'Active',   280000, 'Bob'),
    (3, 'Dashboard v2',    'Planning', 75000,  'Carol'),
    (4, 'API Gateway',     'Complete', 120000, 'Dave'),
    (5, 'Security Audit',  'Active',   95000,  'Eve');

CREATE OR REPLACE VIEW ENGINEERING_DB.ENG_SCHEMA.ACTIVE_COMPANY_PROJECTS AS
    SELECT 
         ID, 
         PROJECT_NAME,
         BUDGET,
         LEAD_NAME
    FROM 
        ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS
    WHERE 
        STATUS = 'Active';

-- Verify the data
SELECT * FROM ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS;
SELECT * FROM ENGINEERING_DB.ENG_SCHEMA.ACTIVE_COMPANY_PROJECTS;

-- =========================================================
-- STEP 6: INTERN_ROLE Cannot See Any Objects
--   Expected: Both queries FAIL with insufficient privileges
-- =========================================================
USE ROLE INTERN_ROLE;

USE WAREHOUSE ENG_DEV_WH; --Fails

USE WAREHOUSE INTERN_WH;

-- EXPECTED FAILURE: Object does not exist or not authorized
SELECT * FROM ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS;

-- EXPECTED FAILURE: Object does not exist or not authorized
SELECT * FROM ENGINEERING_DB.ENG_SCHEMA.ACTIVE_COMPANY_PROJECTS;

/* =========================================================
-- STEP 7: ENGINEER_ROLE Grants Read-Only Access to INTERN_ROLE
--   Includes FUTURE GRANTS for any new tables/views
https://docs.snowflake.com/en/user-guide/security-access-control-privileges#database-privileges
-- =========================================================*/
USE ROLE ENGINEER_ROLE;

GRANT USAGE ON DATABASE ENGINEERING_DB TO ROLE INTERN_ROLE;

GRANT USAGE ON SCHEMA ENGINEERING_DB.ENG_SCHEMA TO ROLE INTERN_ROLE;

GRANT SELECT ON TABLE ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS TO ROLE INTERN_ROLE;

GRANT SELECT ON VIEW ENGINEERING_DB.ENG_SCHEMA.ACTIVE_COMPANY_PROJECTS TO ROLE INTERN_ROLE;

-- Future grants: INTERN_ROLE automatically gets SELECT on any new tables/views in this schema
GRANT SELECT ON FUTURE TABLES IN SCHEMA ENGINEERING_DB.ENG_SCHEMA TO ROLE INTERN_ROLE;

GRANT SELECT ON FUTURE VIEWS IN SCHEMA ENGINEERING_DB.ENG_SCHEMA TO ROLE INTERN_ROLE;

-- =========================================================
-- STEP 8: INTERN_ROLE Can Now Read Data, But Cannot Write
-- =========================================================
USE ROLE INTERN_ROLE;
USE WAREHOUSE INTERN_WH;

-- SUCCESS: Intern can now read
SELECT * FROM ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS;
SELECT * FROM ENGINEERING_DB.ENG_SCHEMA.ACTIVE_COMPANY_PROJECTS;

-- EXPECTED FAILURE: Intern does NOT have INSERT privilege
INSERT INTO ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS VALUES (6, 'Intern Project A', 'Active', 0, 'Intern');

--Now use Engineering role to insert the data. 
USE ROLE ENGINEER_ROLE;

INSERT INTO ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS VALUES (6, 'Intern Project A', 'Active', 0, 'Intern');

USE ROLE INTERN_ROLE;
SELECT * FROM ENGINEERING_DB.ENG_SCHEMA.COMPANY_PROJECTS;

/*
=============================================================
  CLEANUP (optional)
=============================================================
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS ENGINEERING_DB;
DROP WAREHOUSE IF EXISTS ENG_DEV_WH;
DROP WAREHOUSE IF EXISTS INTERN_WH;
DROP ROLE IF EXISTS ENGINEER_ROLE;
DROP ROLE IF EXISTS INTERN_ROLE;
*/
