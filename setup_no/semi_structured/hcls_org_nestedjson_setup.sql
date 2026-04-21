/*=============================================================================
  SNOWFLAKE SEMI-STRUCTURED DATA DEMO — SETUP SCRIPT
  Healthcare Provider JSON Traversal
  
  Run this script FIRST to create all objects and generate data.
  Then use semi_structured_demo_queries.sql to walk through the demo.
  
  Everything runs inside the Snowflake account — no local files needed.
=============================================================================*/

-- ============================================================================
-- SECTION 1: SETUP — Schema, Stage, File Format
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS WH_XS
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 30
    AUTO_RESUME = TRUE;

USE WAREHOUSE WH_XS;

CREATE DATABASE IF NOT EXISTS DEMO;
CREATE OR REPLACE SCHEMA DEMO.SEMI_STRUCTURED_DEMO;

CREATE OR REPLACE STAGE DEMO.SEMI_STRUCTURED_DEMO.JSON_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- ============================================================================
-- SECTION 2: STORED PROCEDURE — Generate Nested JSON Data
-- ============================================================================

-- Generate 3-level nested organization document
CREATE OR REPLACE PROCEDURE DEMO.SEMI_STRUCTURED_DEMO.GENERATE_NESTED_PROVIDER_JSON()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import json
import random
import io

def run(session):
    random.seed(99)

    first_names_m = ["James","Robert","John","Michael","David","William","Richard","Joseph","Thomas","Charles",
                     "Daniel","Matthew","Anthony","Mark","Steven","Andrew","Paul","Joshua","Kenneth","Kevin",
                     "Brian","George","Timothy","Ronald","Edward","Jason","Jeffrey","Ryan","Jacob","Gary"]
    first_names_f = ["Sarah","Maria","Jennifer","Linda","Patricia","Elizabeth","Barbara","Susan","Jessica","Karen",
                     "Lisa","Nancy","Betty","Margaret","Sandra","Ashley","Dorothy","Kimberly","Emily","Donna",
                     "Michelle","Carol","Amanda","Melissa","Deborah","Stephanie","Rebecca","Sharon","Laura","Cynthia"]
    last_names = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
                  "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
                  "Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson",
                  "Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
                  "Green","Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts",
                  "Chen","Patel","Kim","Shah","Singh","Park","Yamamoto","Tanaka","Nakamura","Watanabe"]

    departments_pool = [
        ("Cardiology", 3), ("Dermatology", 2), ("Emergency Medicine", 1),
        ("Endocrinology", 4), ("Family Medicine", 1), ("Gastroenterology", 3),
        ("General Surgery", 2), ("Geriatrics", 4), ("Internal Medicine", 2),
        ("Nephrology", 5), ("Neurology", 3), ("Obstetrics & Gynecology", 2),
        ("Oncology", 4), ("Ophthalmology", 3), ("Orthopedic Surgery", 2),
        ("Pediatrics", 1), ("Psychiatry", 4), ("Pulmonology", 3),
        ("Radiology", 5), ("Rheumatology", 4), ("Urology", 3),
        ("Anesthesiology", 2), ("Critical Care", 1), ("Sports Medicine", 2)
    ]

    locations_data = [
        {"facility_name": "Metro Health Downtown", "city": "Dallas", "state": "TX", "zip": "75201", "address": "100 Medical Pkwy", "bed_count": 450, "trauma_level": "Level I"},
        {"facility_name": "Metro Health Northside", "city": "Dallas", "state": "TX", "zip": "75230", "address": "8500 Hillcrest Rd", "bed_count": 320, "trauma_level": "Level II"},
        {"facility_name": "Metro Health Lakewood", "city": "Houston", "state": "TX", "zip": "77001", "address": "2200 Lakewood Blvd", "bed_count": 280, "trauma_level": "Level II"},
        {"facility_name": "Metro Health Austin Campus", "city": "Austin", "state": "TX", "zip": "78701", "address": "500 University Ave", "bed_count": 400, "trauma_level": "Level I"},
        {"facility_name": "Metro Health Phoenix Center", "city": "Phoenix", "state": "AZ", "zip": "85001", "address": "1200 Camelback Rd", "bed_count": 350, "trauma_level": "Level I"},
        {"facility_name": "Metro Health Denver", "city": "Denver", "state": "CO", "zip": "80201", "address": "700 Colorado Blvd", "bed_count": 300, "trauma_level": "Level II"},
        {"facility_name": "Metro Health Chicago", "city": "Chicago", "state": "IL", "zip": "60601", "address": "333 Michigan Ave", "bed_count": 500, "trauma_level": "Level I"},
        {"facility_name": "Metro Health Atlanta", "city": "Atlanta", "state": "GA", "zip": "30301", "address": "150 Peachtree St", "bed_count": 380, "trauma_level": "Level I"},
        {"facility_name": "Metro Health Miami", "city": "Miami", "state": "FL", "zip": "33101", "address": "900 Biscayne Blvd", "bed_count": 420, "trauma_level": "Level I"},
        {"facility_name": "Metro Health Seattle", "city": "Seattle", "state": "WA", "zip": "98101", "address": "600 Pike St", "bed_count": 290, "trauma_level": "Level II"}
    ]

    roles = ["Attending", "Resident", "Fellow", "Nurse Practitioner", "Physician Assistant"]
    credentials = ["MD", "DO", "MD, PhD", "MD, FACC", "DO, MBA", "MD, MPH", "NP", "PA-C"]

    provider_id = 5001
    total_providers = 0
    target = 2000

    locations = []
    providers_per_location = target // len(locations_data)
    remainder = target % len(locations_data)

    for loc_idx, loc in enumerate(locations_data):
        loc_provider_count = providers_per_location + (1 if loc_idx < remainder else 0)
        num_depts = random.randint(5, 8)
        selected_depts = random.sample(departments_pool, num_depts)

        providers_per_dept = loc_provider_count // num_depts
        dept_remainder = loc_provider_count % num_depts

        departments = []
        for dept_idx, (dept_name, default_floor) in enumerate(selected_depts):
            dept_prov_count = providers_per_dept + (1 if dept_idx < dept_remainder else 0)
            providers_list = []
            for _ in range(dept_prov_count):
                gender = random.choice(["M", "F"])
                fn = random.choice(first_names_m) if gender == "M" else random.choice(first_names_f)
                ln = random.choice(last_names)
                role = random.choice(roles)
                cred = random.choice(credentials)
                if role in ("Nurse Practitioner",):
                    cred = "NP"
                elif role in ("Physician Assistant",):
                    cred = "PA-C"

                providers_list.append({
                    "provider_id": provider_id,
                    "name": f"Dr. {fn} {ln}" if cred not in ("NP", "PA-C") else f"{fn} {ln}, {cred}",
                    "credential": cred,
                    "role": role,
                    "gender": gender,
                    "years_experience": random.randint(1, 30),
                    "patients_seen_monthly": random.randint(40, 200),
                    "accepting_new_patients": random.choice([True, True, True, False]),
                    "avg_patient_rating": round(random.uniform(3.0, 5.0), 1)
                })
                provider_id += 1
                total_providers += 1

            departments.append({
                "dept_id": f"DEPT-{loc_idx * 100 + dept_idx + 10}",
                "name": dept_name,
                "floor": default_floor,
                "phone": f"555-{random.randint(100,999)}-{random.randint(1000,9999)}",
                "accepting_referrals": random.choice([True, True, False]),
                "providers": providers_list
            })

        locations.append({
            "location_id": f"LOC-{100 + loc_idx}",
            "facility_name": loc["facility_name"],
            "address": loc["address"],
            "city": loc["city"],
            "state": loc["state"],
            "zip": loc["zip"],
            "bed_count": loc["bed_count"],
            "trauma_level": loc["trauma_level"],
            "departments": departments
        })

    org_doc = {
        "organization": {
            "org_id": "ORG-5001",
            "name": "Metro Health System",
            "type": "Integrated Health Network",
            "founded_year": 1985,
            "ceo": "Dr. Patricia Armstrong",
            "total_employees": 15200,
            "website": "https://www.metrohealth.org",
            "locations": locations
        }
    }

    json_bytes = json.dumps(org_doc, indent=2).encode("utf-8")
    buf = io.BytesIO(json_bytes)

    session.file.put_stream(
        buf,
        "@DEMO.SEMI_STRUCTURED_DEMO.JSON_STAGE/provider_nested.json",
        auto_compress=False,
        overwrite=True
    )

    return f"Successfully generated nested org doc with {total_providers} providers across {len(locations)} locations and uploaded to @JSON_STAGE/provider_nested.json"
$$;

-- ============================================================================
-- SECTION 3: GENERATE DATA — Run Stored Procedures
-- ============================================================================

CALL DEMO.SEMI_STRUCTURED_DEMO.GENERATE_NESTED_PROVIDER_JSON();

LIST @DEMO.SEMI_STRUCTURED_DEMO.JSON_STAGE;

