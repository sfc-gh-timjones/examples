

/***********************************************************************
DEMO: Loading data into Snowflake-managed Iceberg tables using COPY INTO
with different LOAD_MODE options, Tasks, Snowpipe, and Schema Evolution.

Flow: ADD_FILES_REFERENCE (+ Task)
      -> ADD_FILES_COPY (INFER_SCHEMA, Parquet)
      -> FULL_INGEST (Parquet -> VARIANT + Snowpipe + Dynamic Table + Schema Evolution)
      -> Query Stage Directly
      -> FULL_INGEST (CSV)

  Feature                  | FULL_INGEST       | ADD_FILES_COPY    | ADD_FILES_REFERENCE
  -------------------------+-------------------+-------------------+--------------------
  File formats             | All (CSV,JSON,..) | Parquet only      | Parquet only
  Rewrites data            | Yes               | No (binary copy)  | No (zero-copy)
  Extra storage cost       | Yes               | Yes (copied files) | No
  MATCH_BY_COLUMN_NAME     | CASE_INSENSITIVE  | CASE_SENSITIVE    | CASE_SENSITIVE
  Transformations          | Supported         | Not supported     | Not supported
  Files under base loc     | Not required      | Not required      | Required
  Snowpipe support         | Yes               | Yes               | No

External Volume: S3SNOWFLAKEICEBERG
  Storage Base URL: s3://capstone-timjones/snowflake-iceberg/

Stage paths:
  ADD_FILES_REFERENCE : @iceberg_stage/snowflake-iceberg/iceberg_demo/titanic_files/
    initial files for demo start: titanic.parquet
  FULL_INGEST / ADD_FILES_COPY : @iceberg_stage/titanic_data_files/
    initial files for demo start: titanic.parquet, titanic_no_cabin.parquet, titanic_with_city.parquet, titanic.csv
************************************************************************/

/***********************************************************************
 SET CONTEXT, CREATE STAGE, CREATE FILE FORMATS
************************************************************************/
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS ETL_TESTING;
CREATE SCHEMA IF NOT EXISTS ETL_TESTING.ICEBERG_DEMO;

USE DATABASE ETL_TESTING;
USE SCHEMA ICEBERG_DEMO;

CREATE OR REPLACE STAGE iceberg_stage
  STORAGE_INTEGRATION = capstone_timjones_storageintegration
  URL = 's3://capstone-timjones/';

CREATE OR REPLACE FILE FORMAT iceberg_csv_format
  TYPE = CSV
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE FILE FORMAT iceberg_parquet_format
  TYPE = PARQUET
  USE_VECTORIZED_SCANNER = TRUE;

LIST @iceberg_stage/snowflake-iceberg/iceberg_demo/titanic_files/;
LIST @iceberg_stage/titanic_data_files/;

/***********************************************************************
OPTION 1: ADD_FILES_REFERENCE + Task
  Zero-copy. Snowflake registers the original Parquet files in the
  Iceberg metadata without copying or rewriting them. No extra storage.

  KEY CONSTRAINT: Source files must already be under the table's
  BASE_LOCATION. BASE_LOCATION = 'iceberg_demo/titanic_files/' maps to
  s3://capstone-timjones/snowflake-iceberg/iceberg_demo/titanic_files/
  which matches the stage path — add files here during the demo.

  NOTE: Snowpipe does NOT support ADD_FILES_REFERENCE. Instead, we use
  a Snowflake Task that polls every 10 seconds for new Parquet files.
  COPY INTO load history ensures each file is registered exactly once.

  This section demonstrates:
    Step 1 - Infer schema, create table, initial load
    Step 2 - Task for continuous polling (10 second interval)
************************************************************************/

/***********************************************************************
  Step 1: Infer schema, create table, and load via ADD_FILES_REFERENCE.
************************************************************************/

--show infer schema 
SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@iceberg_stage/snowflake-iceberg/iceberg_demo/titanic_files/'
      ,FILE_FORMAT => 'iceberg_parquet_format'
      ,FILES => ('titanic.parquet')
      ,KIND => 'ICEBERG'
    )
  );


--create the table. 
CREATE OR REPLACE ICEBERG TABLE titanic_iceberg_1
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION => '@iceberg_stage/snowflake-iceberg/iceberg_demo/titanic_files/'
          ,FILE_FORMAT => 'iceberg_parquet_format'
          ,FILES => ('titanic.parquet')
          ,KIND => 'ICEBERG'
        )
      )
  )
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'S3SNOWFLAKEICEBERG'
  BASE_LOCATION = 'iceberg_demo/titanic_files/'
  ICEBERG_VERSION = 3;

DESCRIBE TABLE titanic_iceberg_1;
SELECT * FROM titanic_iceberg_1;

COPY INTO titanic_iceberg_1
  FROM @iceberg_stage/snowflake-iceberg/iceberg_demo/titanic_files/
  FILES = ('titanic.parquet')
  FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
  LOAD_MODE = ADD_FILES_REFERENCE
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

SELECT * FROM titanic_iceberg_1;

/***********************************************************************
  Step 2: Task — polls every 10 seconds (for the demo) for new Parquet files and
  loads them via ADD_FILES_REFERENCE. COPY INTO load history ensures
  each file is registered exactly once (no duplicates).
************************************************************************/

CREATE OR REPLACE TASK iceberg_reference_task
  SCHEDULE = '10 SECOND'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  AS
    COPY INTO titanic_iceberg_1
    FROM @iceberg_stage/snowflake-iceberg/iceberg_demo/titanic_files/
    PATTERN = '.*\.parquet$'
    FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
    LOAD_MODE = ADD_FILES_REFERENCE
    MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

ALTER TASK iceberg_reference_task RESUME;

SHOW TASKS;

--ADD FILES
--add titanic2.parquet, check, then add titanic3.parquet. Show ingestion twice.

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
  TASK_NAME => 'ICEBERG_REFERENCE_TASK'
));

SELECT * FROM titanic_iceberg_1;


/***********************************************************************
OPTION 2: ADD_FILES_COPY (Parquet)
  Binary-copies Iceberg-compatible Parquet files into the table's base
  location without scanning or rewriting the data. Requires Parquet.
  Schema is inferred directly from the source Parquet file.

  Step 1 - INFER_SCHEMA on Parquet with KIND => 'ICEBERG'
  Step 2 - Create Iceberg table using INFER_SCHEMA template
  Step 3 - COPY INTO with ADD_FILES_COPY
************************************************************************/

--show infer schema again.
SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@iceberg_stage/titanic_data_files/'
      ,FILE_FORMAT => 'iceberg_parquet_format'
      ,FILES => ('titanic.parquet')
      ,KIND => 'ICEBERG'
    )
  );

--create table. 
CREATE OR REPLACE ICEBERG TABLE titanic_iceberg_2
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION => '@iceberg_stage/titanic_data_files/'
          ,FILE_FORMAT => 'iceberg_parquet_format'
          ,FILES => ('titanic.parquet')
          ,KIND => 'ICEBERG'
        )
      )
  )
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'S3SNOWFLAKEICEBERG'
  BASE_LOCATION = 'titanic_iceberg_2/'
  ICEBERG_VERSION = 3;

DESCRIBE TABLE titanic_iceberg_2;

--manual copy into for simplicity.
COPY INTO titanic_iceberg_2
  FROM @iceberg_stage/titanic_data_files/
  FILES = ('titanic.parquet')
  FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
  LOAD_MODE = ADD_FILES_COPY
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

SELECT * FROM titanic_iceberg_2;

--NOTE: ALL THESE ICEBERG TABLES CAN BE QUERIES BY EXTERNAL ENGINES. READ/WRITE.
--https://docs.snowflake.com/en/user-guide/tables-iceberg-access-using-external-query-engine-snowflake-horizon

/***********************************************************************
OPTION 3: FULL_INGEST (Parquet -> VARIANT) + Snowpipe + Dynamic Table
  FULL_INGEST supports transformations — Snowflake auto-wraps each
  Parquet row into a single VARIANT column on ingest. A Dynamic Iceberg
  Table then flattens the raw VARIANT into typed columns.

  This section demonstrates:
    Step 1 - Create VARIANT table, load titanic.parquet manually
    Step 2 - Create Dynamic Table (1 min lag, flattens VARIANT)
    Step 3 - Create Snowpipe (FULL_INGEST) + configure SQS
    Step 4 - Drop titanic2 & titanic3 into S3; watch Dynamic Table auto-refresh
    Step 5 - Schema Evolution (FULL_INGEST detects column changes via Snowpipe)
************************************************************************/

/***********************************************************************
  Step 1: Create VARIANT landing table and load titanic.parquet via
  FULL_INGEST. Snowflake auto-wraps each Parquet row as VARIANT.
************************************************************************/

CREATE OR REPLACE ICEBERG TABLE titanic_iceberg_3 (
    data VARIANT
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'S3SNOWFLAKEICEBERG'
  BASE_LOCATION = 'titanic_iceberg_3/'
  ICEBERG_VERSION = 3;

COPY INTO titanic_iceberg_3
  FROM @iceberg_stage/titanic_data_files/
  FILES = ('titanic.parquet')
  FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
  LOAD_MODE = FULL_INGEST;

SELECT * FROM titanic_iceberg_3;

/***********************************************************************
  Step 2: Dynamic Table — flattens the VARIANT landing table into typed
  columns. Refreshes automatically every 1 minute.
************************************************************************/

CREATE OR REPLACE DYNAMIC ICEBERG TABLE titanic_iceberg_3_flat
  TARGET_LAG = '1 minute'
  WAREHOUSE = wh_xs
  REFRESH_MODE = AUTO
  INITIALIZE = ON_CREATE
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'S3SNOWFLAKEICEBERG'
  BASE_LOCATION = 'titanic_iceberg_3_flat/'
  ICEBERG_VERSION = 3
  AS
    SELECT
         data:PassengerId::INT    AS PassengerId
        ,data:Survived::INT       AS Survived
        ,data:Pclass::INT         AS Pclass
        ,data:Name::VARCHAR       AS Name
        ,data:Sex::VARCHAR        AS Sex
        ,data:Age::FLOAT          AS Age
        ,data:SibSp::INT          AS SibSp
        ,data:Parch::INT          AS Parch
        ,data:Ticket::VARCHAR     AS Ticket
        ,data:Fare::FLOAT         AS Fare
        ,data:Cabin::VARCHAR      AS Cabin
        ,data:Embarked::VARCHAR   AS Embarked
        ,data                     AS raw_data
    FROM titanic_iceberg_3;

SELECT COUNT(*) AS rows_materialized FROM titanic_iceberg_3_flat;
SELECT * FROM titanic_iceberg_3_flat;

/***********************************************************************
  Step 3: Create Snowpipe (FULL_INGEST). Copy the SQS ARN and configure
  S3 bucket event notifications, then drop files to trigger auto-ingest.
************************************************************************/

CREATE OR REPLACE PIPE iceberg_copy_pipe
  AUTO_INGEST = TRUE
  AS
    COPY INTO titanic_iceberg_3
    FROM @iceberg_stage/titanic_data_files/
    PATTERN = '.*\.parquet$'
    FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
    LOAD_MODE = FULL_INGEST;

SHOW PIPES;
SELECT "name", "notification_channel" AS sqs_queue
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

SELECT PARSE_JSON(SYSTEM$PIPE_STATUS('iceberg_copy_pipe'));
--Show Pipe history in UI. Db = ETL TESTING, Schema = Iceberg_DEMO

/***********************************************************************
  Step 4: Drop titanic2.parquet then titanic3.parquet into S3 one at a
  time. The pipe ingests each file; watch the Dynamic Table row count
  grow after each 1 minute refresh.
************************************************************************/

--ADD FILES
-- After titanic2/3.parquet lands. Can do them both at the same time or sequentially.
SELECT * FROM titanic_iceberg_3;
SELECT * FROM titanic_iceberg_3_flat;

/***********************************************************************
  Step 5: Schema Evolution — FULL_INGEST detects column changes when
  ENABLE_SCHEMA_EVOLUTION = TRUE and MATCH_BY_COLUMN_NAME = CASE_SENSITIVE
  are set. Two manual COPY INTO statements load each schema-evolved file.

  Files at @iceberg_stage/titanic_data_files/:
    - titanic_no_cabin.parquet   (Cabin column removed — 11 columns)
    - titanic_with_city.parquet  (Embarked_City column added — 13 columns)
************************************************************************/

CREATE OR REPLACE ICEBERG TABLE titanic_iceberg_3b
  ENABLE_SCHEMA_EVOLUTION = TRUE
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
      FROM TABLE(
        INFER_SCHEMA(
          LOCATION => '@iceberg_stage/titanic_data_files/'
          ,FILE_FORMAT => 'iceberg_parquet_format'
          ,FILES => ('titanic.parquet')
          ,KIND => 'ICEBERG'
        )
      )
  )
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'S3SNOWFLAKEICEBERG'
  BASE_LOCATION = 'titanic_iceberg_3b/'
  ICEBERG_VERSION = 3;

-- Ingest inital data. Manual for demo. 
COPY INTO titanic_iceberg_3b
  FROM @iceberg_stage/titanic_data_files/
  FILES = ('titanic.parquet')
  FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
  LOAD_MODE = FULL_INGEST
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

SELECT * FROM titanic_iceberg_3b ORDER BY "Cabin";


-- Step 5a: Load titanic_no_cabin.parquet — Cabin column removed.
-- Schema evolution drops the NOT NULL constraint on Cabin.
COPY INTO titanic_iceberg_3b
  FROM @iceberg_stage/titanic_data_files/
  FILES = ('titanic_no_cabin.parquet')
  FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
  LOAD_MODE = FULL_INGEST
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

--demonstrate nulls backfilled.
SELECT * FROM titanic_iceberg_3b ORDER BY "Cabin";

-- Step 5b: Load titanic_with_city.parquet — Embarked_City column added.
-- Schema evolution automatically adds the Embarked_City column.
COPY INTO titanic_iceberg_3b
  FROM @iceberg_stage/titanic_data_files/
  FILES = ('titanic_with_city.parquet')
  FILE_FORMAT = (FORMAT_NAME = 'iceberg_parquet_format')
  LOAD_MODE = FULL_INGEST
  MATCH_BY_COLUMN_NAME = CASE_SENSITIVE;

--demonstrate new column added.
SELECT * FROM titanic_iceberg_3b;
DESCRIBE TABLE titanic_iceberg_3b;
SELECT * FROM titanic_iceberg_3b WHERE "Embarked_City" IS NOT NULL;
SELECT * FROM titanic_iceberg_3b ORDER BY "Embarked_City";

/***********************************************************************
OPTION 4: Query stage directly
  Query Parquet files on the stage without loading them into a table.
  Uses METADATA$ columns for file-level audit info.
************************************************************************/

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
FROM @iceberg_stage/titanic_data_files/
(
    PATTERN => '.*titanic\.parquet$',
    FILE_FORMAT => 'iceberg_parquet_format'
) AS t;

/***********************************************************************
OPTION 5: FULL_INGEST (CSV)
  Snowflake scans the CSV, converts it to Iceberg Parquet, and writes
  the data to the table's base location. This is the only LOAD_MODE
  that supports non-Parquet formats (CSV, JSON, Avro, ORC).
************************************************************************/
SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@iceberg_stage/titanic_data_files/'
      ,FILE_FORMAT => 'iceberg_csv_format'
      ,FILES => ('titanic.csv')
    )
  );

CREATE OR REPLACE ICEBERG TABLE titanic_iceberg_5 (
     PassengerId  INTEGER
    ,Survived     INTEGER
    ,Pclass       INTEGER
    ,Name         STRING
    ,Sex          STRING
    ,Age          FLOAT
    ,SibSp        INTEGER
    ,Parch        INTEGER
    ,Ticket       STRING
    ,Fare         FLOAT
    ,Cabin        STRING
    ,Embarked     STRING
)
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'S3SNOWFLAKEICEBERG'
  BASE_LOCATION = 'titanic_iceberg_5/'
  ICEBERG_VERSION = 3;

--manually copy into. 
COPY INTO titanic_iceberg_5
  FROM @iceberg_stage/titanic_data_files/
  FILES = ('titanic.csv')
  FILE_FORMAT = (FORMAT_NAME = 'iceberg_csv_format')
  LOAD_MODE = FULL_INGEST
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * FROM titanic_iceberg_5;

DESCRIBE TABLE titanic_iceberg_5;

/***********************************************************************
  Cleanup
************************************************************************/
ALTER TASK iceberg_reference_task SUSPEND;
DROP TASK IF EXISTS iceberg_reference_task;
DROP PIPE IF EXISTS iceberg_copy_pipe;
DROP ICEBERG TABLE IF EXISTS titanic_iceberg_1;
DROP ICEBERG TABLE IF EXISTS titanic_iceberg_2;
DROP DYNAMIC TABLE IF EXISTS titanic_iceberg_3_flat;
DROP ICEBERG TABLE IF EXISTS titanic_iceberg_3;
DROP ICEBERG TABLE IF EXISTS titanic_iceberg_3b;
DROP ICEBERG TABLE IF EXISTS titanic_iceberg_5;
DROP STAGE IF EXISTS iceberg_stage;
DROP FILE FORMAT IF EXISTS iceberg_parquet_format;
DROP FILE FORMAT IF EXISTS iceberg_csv_format;
