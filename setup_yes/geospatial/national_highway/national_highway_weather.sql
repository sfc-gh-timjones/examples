/*=====================================================================================================================================
Create a warehouse to run the queries. 

Create database/schema + set context for the session. 
=====================================================================================================================================*/ 
create or replace warehouse my_compute_warehouse
warehouse_size = small 
auto_suspend = 30
comment = 'Create a warehouse to execute below queries';

use warehouse my_compute_warehouse;

create database if not exists demo;
use database demo; 
create schema if not exists demo.geospatial; 
use schema geospatial;

/*=====================================================================================================================================
Create Snowflake Internal Stage as a landing spot for geojson files. 
=====================================================================================================================================*/
create stage if not exists demo.geospatial.my_internal_stage
directory = (enable = true)
comment = 'stage for my geojson files';

/*=====================================================================================================================================
IMPORTANT: 
Manually go and upload the geojson files to the stage we just created. In a production environment, this wouldn't be manually and the 
files would be automatically put in the Internal Stage or in the Cloud Bucket (S3, Blob, GCP Cloud Storage) that would be referenced by 
an External Stage. 

You can upload them to the stage by navigating to the stage (Catalog --> Database Explorer --> Navigate to stage location --> +Files in top right)
=====================================================================================================================================*/

/*=====================================================================================================================================
Create a destination table to ingest the geojson files. We're going to load the raw data, then transform. 
=====================================================================================================================================*/
create or replace table demo.geospatial.data_staging (
raw_data variant 
);

/*=====================================================================================================================================
Copy the data from the stage to the destination table. In a production environment, this would be done by executing a task to run the 
command, or alternatively by settin up Snowpipe to automatically ingest new files as they land into the destination table. 
=====================================================================================================================================*/
COPY INTO demo.geospatial.data_staging 
FROM @demo.geospatial.my_internal_stage/
FILE_FORMAT = (TYPE = 'JSON');

/*=====================================================================================================================================
Verify data was loaded and table has rows. NOTE: you cannot view data as it's >16mb since each row json is >16mb. 
(Snowsight can store 128mb in variant column, but only view 16mb from query results in Snowsight UI)
=====================================================================================================================================*/
select count(*) 
from demo.geospatial.data_staging;

/*=====================================================================================================================================
Query flatten data so it can be viewed in the results. 
=====================================================================================================================================*/
SELECT 
    b.value:geometry::variant as geometry_data,
    b.value:properties::variant as properties
FROM demo.geospatial.data_staging as a,
LATERAL Flatten(input => a.raw_data:features) as b
;

/*=====================================================================================================================================
Create a table to store the results of the flattened data, with properties of interest. 
=====================================================================================================================================*/
create or replace table demo.geospatial.national_highway_system as (
SELECT 
    b.value:properties.OBJECTID::int as object_id,
    b.value:properties.FILE_NAME::varchar as file_name,
    b.value:properties.MILES::numeric(38,2) as miles,
    b.value:properties.AADT::int as average_annual_daily_traffic,
    b.value:properties.SPEED_LIMI::int as speed_limit,
    b.value:properties.YEAR::int as year,
    b.value:properties.SIGN1::varchar as sign1,
    TO_GEOGRAPHY(b.value:geometry) as geography_linestring, --creates a GEOGRAPHY data type
    ST_ASGEOJSON(GEOGRAPHY_LINESTRING) as geojson, --creates a OBJECT data type
    b.value::variant as raw_json
FROM demo.geospatial.data_staging as a,
LATERAL Flatten(input => a.raw_data:features) b
);

/*=====================================================================================================================================
View results
=====================================================================================================================================*/
select * from demo.geospatial.national_highway_system;

/*=====================================================================================================================================
IMPORTANT: Go get marketplace weather data. Keep the database name as ACCUWEATHERS_HISTORICAL_WEATHER_DATA_SAMPLE. Grant it to PUBLIC 
role to avoid any data access issues. 

https://app.snowflake.com/marketplace/listing/GZSTZ745B6V/accuweather%C2%AE-data-suite-sample-of-accuweather-s-historical-weather-data?search=accuweather&originTab=provider&providerName=AccuWeather%2525C2%2525AE%20Data%20Suite%20&profileGlobalName=GZSTZ745B56
=====================================================================================================================================*/

/*=====================================================================================================================================
Explore shared weather data. 
=====================================================================================================================================*/
SELECT 
    CITY_NAME,
    LATITUDE,
    LONGITUDE,
    DATETIME,
    DATE_PART('HH',DATETIME) as Hour_of_Day,
    HUMIDITY_RELATIVE,
    MINUTES_OF_PRECIPITATION,
    RAIN_LWE,
    TEMPERATURE,
    WIND_GUST,
    WIND_SPEED,
    *
FROM ACCUWEATHERS_HISTORICAL_WEATHER_DATA_SAMPLE.HISTORICAL.TOP_CITY_HOURLY_IMPERIAL;

/*=====================================================================================================================================
We now want to take all our roadway linstrings, and only return those roadways where we have weather data within a 10 miles distance of 
the city's geopoint. In other words, we only want to analyze the roadways were we have weather data. This requires a geojoin. 
=====================================================================================================================================*/
create or replace table demo.geospatial.national_highway_system_subset AS (
with distinct_cities as ( --returns 50 rows
    SELECT DISTINCT CITY_NAME, LATITUDE, LONGITUDE
    FROM ACCUWEATHERS_HISTORICAL_WEATHER_DATA_SAMPLE.HISTORICAL.TOP_CITY_HOURLY_IMPERIAL
)
select 
    c.city_name,
    c.latitude,
    c.longitude,        
    h.*
from 
    distinct_cities as c 
    inner join demo.geospatial.national_highway_system as h  --(>22M checks)
    ON ST_DWITHIN( --Geojoin
        ST_POINT(c.LONGITUDE, c.LATITUDE),
        h.GEOGRAPHY_LINESTRING,
        16093.4)  -- 10 miles in meters
);

/*=====================================================================================================================================
View results. 

NOTE: This table is used in the Streamlit application for visualization. No further queries are needed to get the streamlit app running.
=====================================================================================================================================*/
select * from demo.geospatial.national_highway_system_subset;

/*=====================================================================================================================================
STOP HERE. EVERYTHING ABOVE HERE IS ONLY CODE REQUIRED FOR STREAMLIT APPLICATION. 

BELOW QUERIES ARE FOR AD HOC ANALYSIS. THESE ARE FOR DEMONSTRATION PURPOSES AND NOT TIED TO THE STREAMLIT APPLICATION.
=====================================================================================================================================*/
/*=====================================================================================================================================
Run a query to see how many intersections each city has based on available geojson linestrings in the dataset.
=====================================================================================================================================*/
alter warehouse my_compute_warehouse set warehouse_size = medium;

with distinct_cities as ( --returns 50 rows
    SELECT DISTINCT CITY_NAME, LATITUDE, LONGITUDE
    FROM ACCUWEATHERS_HISTORICAL_WEATHER_DATA_SAMPLE.HISTORICAL.TOP_CITY_HOURLY_IMPERIAL
)

select 
    c.city_name, 
    count(*) as intersection_cnt
from 
    demo.geospatial.national_highway_system as t1
inner join demo.geospatial.national_highway_system as t2
    ON st_intersects(t1.geography_linestring, t2.geography_linestring)
    and t1.object_id <> t2.object_id
inner join distinct_cities as c
    ON ST_DWITHIN( --Geojoin
    ST_POINT(c.LONGITUDE, c.LATITUDE),
    t1.GEOGRAPHY_LINESTRING,
    16093.4)  -- 10 miles in meters
group by 
    c.city_name
order by intersection_cnt desc;

/*=====================================================================================================================================
Were there any cities that had rain on the sample weather? How many roadways were impacted by rainfall? Break out rainfall by hour. 
=====================================================================================================================================*/
select 
    c.city_name,
    date_part('hour',c.datetime) as hour,
    avg(c.minutes_of_rain)::int as minutes_of_rain,
    cast(sum(c.precipitation_lwe) as numeric(38,2)) as total_precipitation,
    cast(avg(c.temperature) as numeric(38,2)) as avg_termperature,
    count(h.geography_linestring) as roadway_segments_impacted
from 
    ACCUWEATHERS_HISTORICAL_WEATHER_DATA_SAMPLE.HISTORICAL.TOP_CITY_HOURLY_IMPERIAL as c 
    inner join demo.geospatial.national_highway_system as h  
    ON ST_DWITHIN( --Geojoin
        ST_POINT(c.LONGITUDE, c.LATITUDE),
        h.GEOGRAPHY_LINESTRING,
        32186.9)  -- 20 miles in meters
where 
    c.precipitation_type = 1 --Rain 
group by 
    c.city_name,
    date_part('hour',c.datetime)
order by 
    hour 
        ;

/*=====================================================================================================================================
End
=====================================================================================================================================*/