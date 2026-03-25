
/***********************************************************************
DEMO: Ingesting data into Snowflake via Cloud Bucket (S3) and setting up
Snowpipe or Tasks for automated data ingestion. 

Flow: Basic COPY INTO -> Snowpipe -> Infer Schema + Schema Evolution -> Query Stage Directly
************************************************************************/

/***********************************************************************
 SET CONTEXT, CREATE STAGE, CREATE FILE FORMAT 
************************************************************************/
USE ROLE ACCOUNTADMIN; 

CREATE DATABASE IF NOT EXISTS ETL_TESTING;
CREATE SCHEMA IF NOT EXISTS ETL_TESTING.CLOUDBUCKET;

USE DATABASE ETL_TESTING;
USE SCHEMA CLOUDBUCKET; 

CREATE OR REPLACE STAGE s3_stage
  STORAGE_INTEGRATION = capstone_timjones_storageintegration
  URL = 's3://capstone-timjones/';

CREATE OR REPLACE FILE FORMAT my_parquet_file_format
TYPE = 'PARQUET';

list @s3_stage/etl_testing/copy_into_snowpipe/;

/***********************************************************************
OPTION 1: BASIC COPY INTO
************************************************************************/
/***********************************************************************
  Create table, load data with COPY INTO, and query.
  Uses INCLUDE_METADATA to capture file-level audit info automatically.
************************************************************************/

CREATE OR REPLACE TABLE titanic_1 (
    data VARIANT
    ,src_file VARCHAR DEFAULT NULL
    ,src_row_num INT DEFAULT NULL
    ,src_last_modified TIMESTAMP_LTZ DEFAULT NULL
);

COPY INTO titanic_1
    FROM @s3_stage/etl_testing/copy_into_snowpipe/
    PATTERN = '.*\.parquet$'
    FILE_FORMAT = (TYPE = 'PARQUET')
    INCLUDE_METADATA = (
        src_file = METADATA$FILENAME
        ,src_row_num = METADATA$FILE_ROW_NUMBER
        ,src_last_modified = METADATA$FILE_LAST_MODIFIED
    );

SELECT COUNT(*) AS rows_loaded FROM titanic_1;

SELECT *
FROM titanic_1;

SELECT 
     Data:Age::numeric(38,2) AS Age
    ,Data:Cabin::varchar AS Cabin
    ,Data:Embarked::varchar AS Embarked
    ,Data:Fare::numeric(38,2) AS Fare
    ,Data:Name::varchar AS Name
    ,Data:Parch::int AS Parch
    ,Data:PassengerId::int AS PassengerId
    ,Data:Pclass::int AS Pclass
    ,Data:Sex::varchar AS Sex
    ,Data:SibSp::int AS SibSp
    ,Data:Survived::int AS Survived
    ,Data:Ticket::varchar AS Ticket
    ,src_file
    ,src_row_num
    ,src_last_modified
    ,Data AS Raw_Data 
FROM titanic_1;


/***********************************************************************
OPTION 2: LOAD VIA SNOWPIPE 
************************************************************************/
/***********************************************************************
  Create table, pipe, and validate. Snowpipe auto-ingests new files
  as they land in S3 via SQS event notifications. 
************************************************************************/

CREATE OR REPLACE TABLE titanic_2 (
    data VARIANT
);

CREATE OR REPLACE PIPE pipe_demo
AUTO_INGEST = TRUE
  AS
    COPY INTO titanic_2
    FROM @s3_stage/etl_testing/copy_into_snowpipe/
    PATTERN = '.*\.parquet$'
    FILE_FORMAT = (TYPE = 'PARQUET');

SHOW PIPES;
SELECT "name", "notification_channel" AS sqs_queue
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT SYSTEM$PIPE_STATUS('pipe_demo');

/***********************************************************************
  Upload file to S3 bucket. Check that it loaded. Query semi-structured data. 
************************************************************************/

SELECT COUNT(*) AS rows_loaded FROM titanic_2;

SELECT *
FROM titanic_2;

SELECT 
     Data:Age::numeric(38,2) AS Age
    ,Data:Cabin::varchar AS Cabin
    ,Data:Embarked::varchar AS Embarked
    ,Data:Fare::numeric(38,2) AS Fare
    ,Data:Name::varchar AS Name
    ,Data:Parch::int AS Parch
    ,Data:PassengerId::int AS PassengerId
    ,Data:Pclass::int AS Pclass
    ,Data:Sex::varchar AS Sex
    ,Data:SibSp::int AS SibSp
    ,Data:Survived::int AS Survived
    ,Data:Ticket::varchar AS Ticket
    ,Data AS Raw_Data 
FROM titanic_2;

/***********************************************************************
  Materialize flattened data with a Dynamic Table. 
************************************************************************/

CREATE OR REPLACE DYNAMIC TABLE my_table
  TARGET_LAG = '1 minute'
  WAREHOUSE = wh_xs
  REFRESH_MODE = auto
  INITIALIZE = on_create
  COMMENT = 'This is my dynamic table for materializing new data from my table.'
  AS
    SELECT 
         Data:Age::numeric(38,2) AS Age
        ,Data:Cabin::varchar AS Cabin
        ,Data:Embarked::varchar AS Embarked
        ,Data:Fare::numeric(38,2) AS Fare
        ,Data:Name::varchar AS Name
        ,Data:Parch::int AS Parch
        ,Data:PassengerId::int AS PassengerId
        ,Data:Pclass::int AS Pclass
        ,Data:Sex::varchar AS Sex
        ,Data:SibSp::int AS SibSp
        ,Data:Survived::int AS Survived
        ,Data:Ticket::varchar AS Ticket
        ,Data AS Raw_Data 
    FROM titanic_2
  ;

SELECT * 
FROM my_table; 

/***********************************************************************
OPTION 3: INFER SCHEMA, SCHEMA EVOLUTION & TASK-BASED LOADING
  https://docs.snowflake.com/en/user-guide/data-load-schema-evolution

  Use INFER_SCHEMA to auto-create a table from file metadata, then
  enable SCHEMA EVOLUTION so the table adapts as source files change.
  Requires:
    1. ENABLE_SCHEMA_EVOLUTION = TRUE on the table
    2. MATCH_BY_COLUMN_NAME on the COPY INTO
    3. EVOLVE SCHEMA or OWNERSHIP privilege on the table

  Upload all 3 parquet files to S3 first:
    - titanic.parquet            (original - 12 columns)
    - titanic_no_cabin.parquet   (Cabin column removed - 11 columns)
    - titanic_with_city.parquet  (Embarked_City column added - 13 columns)
************************************************************************/

/***********************************************************************
  Step 1: Infer schema and create table from titanic.parquet.
************************************************************************/

SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@s3_stage/etl_testing/copy_into_snowpipe/'
      , FILE_FORMAT=>'my_parquet_file_format'
      , FILES => ( 'titanic.parquet' )
      )
    );

CREATE OR REPLACE TABLE titanic_3
  ENABLE_SCHEMA_EVOLUTION = TRUE
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
      LOCATION=>'@s3_stage/etl_testing/copy_into_snowpipe/'
      , FILE_FORMAT=>'my_parquet_file_format'
      , FILES => ( 'titanic.parquet' )
        )
      ));

DESCRIBE TABLE titanic_3;

COPY INTO titanic_3 FROM @s3_stage/etl_testing/copy_into_snowpipe/
  FILES = ( 'titanic.parquet' )
  FILE_FORMAT = (FORMAT_NAME= 'my_parquet_file_format')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT COUNT(*) AS rows_loaded FROM titanic_3;

SELECT *
FROM titanic_3;

/***********************************************************************
  Step 2: Load a file with a missing column (no Cabin).
  The NOT NULL constraint on Cabin is dropped automatically.
************************************************************************/

COPY INTO titanic_3 FROM @s3_stage/etl_testing/copy_into_snowpipe/
  FILES = ( 'titanic_no_cabin.parquet' )
  FILE_FORMAT = (FORMAT_NAME= 'my_parquet_file_format')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT COUNT(*) AS rows_loaded FROM titanic_3;

DESCRIBE TABLE titanic_3;

/***********************************************************************
  Step 3: Load a file with a new column (Embarked_City).
  The column is automatically added to the table.
************************************************************************/

COPY INTO titanic_3 FROM @s3_stage/etl_testing/copy_into_snowpipe/
  FILES = ( 'titanic_with_city.parquet' )
  FILE_FORMAT = (FORMAT_NAME= 'my_parquet_file_format')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT COUNT(*) AS rows_loaded FROM titanic_3;

DESCRIBE TABLE titanic_3;

SELECT * FROM titanic_3 WHERE Embarked_City IS NOT NULL LIMIT 10;

SELECT * FROM titanic_3 WHERE Cabin IS NULL LIMIT 10;

/***********************************************************************
  Step 4: Automate ongoing loads with a Task.
************************************************************************/

CREATE OR REPLACE TASK load_my_data 
    WAREHOUSE = wh_xs
    SCHEDULE = '30 minutes'
    --SCHEDULE = 'USING CRON 15 8 * * * America/Denver' --Daily at 8:15am
    AS
        COPY INTO titanic_3 
        FROM @s3_stage/etl_testing/copy_into_snowpipe/
        PATTERN = '.*\.parquet$'
        FILE_FORMAT = (FORMAT_NAME= 'my_parquet_file_format')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE;

EXECUTE TASK load_my_data;

SELECT *
FROM titanic_3;

/***********************************************************************
OPTION 4: Query stage directly & COPY with transformation.
  Transform, filter, and enrich data inline during the COPY — no
  intermediate staging table needed. Also shows METADATA$ columns
  for file-level audit info.
************************************************************************/

/***********************************************************************
  Step 1: Query the stage directly with CTAS + METADATA$ columns.
************************************************************************/

CREATE OR REPLACE TABLE titanic_4 AS (
SELECT *
FROM 
(
    SELECT 
         METADATA$FILENAME AS FILE_NAME
        ,METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUM
        ,METADATA$FILE_CONTENT_KEY
        ,METADATA$FILE_LAST_MODIFIED AS FILE_LAST_MODIFIED_UTC
        ,t.$1:Age::numeric(38,2) AS Age
        ,t.$1:Cabin::varchar AS Cabin
        ,t.$1:Embarked::varchar AS Embarked
        ,t.$1:Fare::numeric(38,2) AS Fare
        ,t.$1:Name::varchar AS Name
        ,t.$1:Parch::int AS Parch
        ,t.$1:PassengerId::int AS PassengerId
        ,t.$1:Pclass::int AS Pclass
        ,t.$1:Sex::varchar AS Sex
        ,t.$1:SibSp::int AS SibSp
        ,t.$1:Survived::int AS Survived
        ,t.$1:Ticket::varchar AS Ticket
        ,t.$1 AS raw_data
    FROM @s3_stage/etl_testing/copy_into_snowpipe/ 
    (
    PATTERN => '.*\.parquet$',
    file_format => 'my_parquet_file_format'
    ) AS t
)
);

SELECT COUNT(*) AS rows_loaded FROM titanic_4;

SELECT *
FROM titanic_4;

/***********************************************************************
  Step 2: COPY with transformation.
  Use a SELECT subquery to reshape, cast, filter, and derive columns
  inline during the COPY — no need for a separate staging table.
************************************************************************/

CREATE OR REPLACE TABLE titanic_4_transformed (
     PassengerId     INT
    ,Name            VARCHAR
    ,Sex             VARCHAR
    ,Age             NUMERIC(38,2)
    ,Fare            NUMERIC(38,2)
    ,Pclass          INT
    ,Survived        INT
    ,Age_Group       VARCHAR
    ,Fare_Category   VARCHAR
    ,FILE_NAME       VARCHAR
    ,FILE_ROW_NUM    INT
);

COPY INTO titanic_4_transformed
FROM (
    SELECT 
         t.$1:PassengerId::int
        ,t.$1:Name::varchar
        ,t.$1:Sex::varchar
        ,t.$1:Age::numeric(38,2)
        ,t.$1:Fare::numeric(38,2)
        ,t.$1:Pclass::int
        ,t.$1:Survived::int
        ,CASE 
            WHEN t.$1:Age::numeric(38,2) < 18 THEN 'Child'
            WHEN t.$1:Age::numeric(38,2) < 65 THEN 'Adult'
            ELSE 'Senior'
         END
        ,CASE 
            WHEN t.$1:Fare::numeric(38,2) < 10 THEN 'Low'
            WHEN t.$1:Fare::numeric(38,2) < 50 THEN 'Medium'
            ELSE 'High'
         END
        ,METADATA$FILENAME
        ,METADATA$FILE_ROW_NUMBER
    FROM @s3_stage/etl_testing/copy_into_snowpipe/ AS t
)
PATTERN = '.*\.parquet$'
FILE_FORMAT = (FORMAT_NAME= 'my_parquet_file_format');

SELECT COUNT(*) AS rows_loaded FROM titanic_4_transformed;

SELECT *
FROM titanic_4_transformed
LIMIT 20;


/***********************************************************************
  Cleanup
************************************************************************/
DROP STAGE IF EXISTS s3_stage;
DROP FILE FORMAT IF EXISTS my_parquet_file_format;
DROP TABLE IF EXISTS titanic_1; 
DROP TABLE IF EXISTS titanic_2;
DROP PIPE IF EXISTS pipe_demo;
DROP DYNAMIC TABLE IF EXISTS my_table;
DROP TABLE IF EXISTS titanic_3;
DROP TASK IF EXISTS load_my_data; 
DROP TABLE IF EXISTS titanic_4;
DROP TABLE IF EXISTS titanic_4_transformed;
