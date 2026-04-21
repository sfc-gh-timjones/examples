/*=============================================================================
  SNOWFLAKE SEMI-STRUCTURED DATA DEMO — QUERY WALKTHROUGH
  Healthcare Provider JSON Traversal
  
  Prerequisites: Run semi_structured_setup.sql first.
  
  This script demonstrates how Snowflake queries JSON data stored in VARIANT
  columns — starting with simple flat JSON, then progressing to deeply nested
  structures using LATERAL FLATTEN.
=============================================================================*/

USE WAREHOUSE WH_XS;


-- ============================================================================
-- SECTION 1: LOAD DATA — Create Tables and COPY INTO
-- ============================================================================

CREATE OR REPLACE TABLE  DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW (raw_data VARIANT);

COPY INTO DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW
FROM @DEMO.SEMI_STRUCTURED_DEMO.JSON_STAGE/provider_nested.json
FILE_FORMAT = (TYPE = 'JSON');

SELECT *
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW;


-- ============================================================================
-- NESTED JSON — 3-Level Organization Document (1 row, 2,000 providers)
-- ============================================================================
-- The entire Metro Health System is stored as a single JSON document:
--
--   Organization (level 0)
--     └── locations[] (level 1)  — 10 facilities
--           └── departments[] (level 2)  — 5-8 per location
--                 └── providers[] (level 3)  — ~2,000 total
--
-- We use LATERAL FLATTEN to "unroll" each nested array into rows.
-- ============================================================================

-- 3a. Level 0 — Organization metadata (no FLATTEN needed)
--     Simple dot notation reaches top-level fields
SELECT
    raw_data:organization:org_id::STRING         AS org_id,
    raw_data:organization:name::STRING           AS org_name,
    raw_data:organization:type::STRING           AS org_type,
    raw_data:organization:founded_year::NUMBER    AS founded,
    raw_data:organization:ceo::STRING            AS ceo,
    raw_data:organization:total_employees::NUMBER AS total_employees,
    raw_data:organization:website::STRING         AS website,
    raw_data
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW;

-- 3b. Level 1 — Locations (single LATERAL FLATTEN)
--     FLATTEN explodes the locations[] array: 1 row per facility
SELECT
    loc.value:location_id::STRING    AS location_id,
    loc.value:facility_name::STRING  AS facility_name,
    loc.value:address::STRING        AS address,
    loc.value:city::STRING           AS city,
    loc.value:state::STRING          AS state,
    loc.value:zip::STRING            AS zip,
    loc.value:bed_count::NUMBER      AS bed_count,
    loc.value:trauma_level::STRING   AS trauma_level,
    raw_data:organization:locations  AS loc 
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW,
    LATERAL FLATTEN(input => raw_data:organization:locations) loc;

-- 3c. Level 2 — Departments (double LATERAL FLATTEN)
--     First FLATTEN on locations, then FLATTEN on departments within each
SELECT
    loc.value:facility_name::STRING  AS facility,
    dept.value:dept_id::STRING       AS dept_id,
    dept.value:name::STRING          AS department,
    dept.value:floor::NUMBER         AS floor,
    dept.value:phone::STRING         AS phone,
    dept.value:accepting_referrals::BOOLEAN AS accepting_referrals,
    ARRAY_SIZE(dept.value:providers) AS provider_count
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW,
    LATERAL FLATTEN(input => raw_data:organization:locations) loc,
    LATERAL FLATTEN(input => loc.value:departments) dept;

-- 3d. Level 3 — Providers (triple LATERAL FLATTEN)
--     Three FLATTEN calls to reach the innermost array
SELECT
    loc.value:facility_name::STRING           AS facility,
    dept.value:name::STRING                   AS department,
    prov.value:provider_id::NUMBER            AS provider_id,
    prov.value:name::STRING                   AS provider_name,
    prov.value:credential::STRING             AS credential,
    prov.value:role::STRING                   AS role,
    prov.value:patients_seen_monthly::NUMBER  AS monthly_patients,
    prov.value:avg_patient_rating::FLOAT      AS rating
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW,
    LATERAL FLATTEN(input => raw_data:organization:locations) loc,
    LATERAL FLATTEN(input => loc.value:departments) dept,
    LATERAL FLATTEN(input => dept.value:providers) prov;

-- 3e. Full denormalized view — all 3 levels flattened into relational columns
--     One query turns a nested JSON document into
--     a flat, joinable result set with data from every level
SELECT
    raw_data:organization:name::STRING        AS organization,
    loc.value:facility_name::STRING           AS facility,
    loc.value:city::STRING                    AS city,
    loc.value:state::STRING                   AS state,
    loc.value:bed_count::NUMBER               AS facility_beds,
    dept.value:name::STRING                   AS department,
    dept.value:floor::NUMBER                  AS floor,
    prov.value:provider_id::NUMBER            AS provider_id,
    prov.value:name::STRING                   AS provider_name,
    prov.value:credential::STRING             AS credential,
    prov.value:role::STRING                   AS role,
    prov.value:gender::STRING                 AS gender,
    prov.value:years_experience::NUMBER       AS years_exp,
    prov.value:patients_seen_monthly::NUMBER  AS monthly_patients,
    prov.value:accepting_new_patients::BOOLEAN AS accepting,
    prov.value:avg_patient_rating::FLOAT      AS rating
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW,
    LATERAL FLATTEN(input => raw_data:organization:locations) loc,
    LATERAL FLATTEN(input => loc.value:departments) dept,
    LATERAL FLATTEN(input => dept.value:providers) prov;

-- 3f. Aggregation on nested data — providers per department per facility
SELECT
    loc.value:facility_name::STRING           AS facility,
    dept.value:name::STRING                   AS department,
    COUNT(*)                                  AS provider_count,
    ROUND(AVG(prov.value:patients_seen_monthly::NUMBER), 0) AS avg_monthly_patients,
    ROUND(AVG(prov.value:avg_patient_rating::FLOAT), 2)     AS avg_rating
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW,
    LATERAL FLATTEN(input => raw_data:organization:locations) loc,
    LATERAL FLATTEN(input => loc.value:departments) dept,
    LATERAL FLATTEN(input => dept.value:providers) prov
GROUP BY 1, 2
ORDER BY facility, avg_rating DESC;

-- 3g. Aggregation by location — facility-level summary
SELECT
    loc.value:facility_name::STRING           AS facility,
    loc.value:city::STRING                    AS city,
    loc.value:state::STRING                   AS state,
    COUNT(*)                                  AS total_providers,
    SUM(CASE WHEN prov.value:role::STRING = 'Attending' THEN 1 ELSE 0 END) AS attendings,
    ROUND(AVG(prov.value:years_experience::NUMBER), 1) AS avg_years_exp,
    ROUND(AVG(prov.value:avg_patient_rating::FLOAT), 2) AS avg_rating
FROM DEMO.SEMI_STRUCTURED_DEMO.PROVIDER_NESTED_RAW,
    LATERAL FLATTEN(input => raw_data:organization:locations) loc,
    LATERAL FLATTEN(input => loc.value:departments) dept,
    LATERAL FLATTEN(input => dept.value:providers) prov
GROUP BY 1, 2, 3
ORDER BY total_providers DESC;

