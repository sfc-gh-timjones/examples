/***********************************************************************
  USE CORTEX LLM FUNCTIONS: 
    -- COMPLETE 
    -- CLASSIFY_TEXT 
    -- EXTRACT_ANSWER 
    -- PARSE_DOCUMENT 
    -- SENTIMENT
    -- SUMMARIZE 
    -- TRANSLATE 
    -- EMBED_TEXT_768
    -- EMBED_TEXT_1024
************************************************************************/
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO; 
USE SCHEMA DEMO; 
USE WAREHOUSE wh_small; 

/***********************************************************************
  COMPLETE 
************************************************************************/
SELECT SNOWFLAKE.CORTEX.AI_COMPLETE('llama3.1-405b', 'Who was the tallest US President?') AS response ;
/***********************************************************************
  TRANSLATE 
************************************************************************/
SELECT 
    EXTRACTED_TEXT AS GERMAN_TEXT,
    SNOWFLAKE.CORTEX.TRANSLATE(GERMAN_TEXT, 'de', 'en') AS ENGLISH_TRANSLATION
FROM DEMO.DEMO.WHISPER_GERMAN_AUDIO;

/***********************************************************************
  CLASSIFY_TEXT  
************************************************************************/
--EXAMPLE 1
CREATE OR REPLACE TEMPORARY TABLE text_classification_table AS
SELECT 'one day I will see the world' AS input 
UNION ALL
SELECT 'my lobster bisque is second to none';

SELECT *
FROM text_classification_table;

SELECT input,
       SNOWFLAKE.CORTEX.CLASSIFY_TEXT(input, ['travel', 'cooking', 'dancing'])['label']::varchar as classification
FROM text_classification_table;


--EXAMPLE 2
CREATE OR REPLACE TEMPORARY TABLE text_classification_table AS
SELECT 'France' AS input, ['North America', 'Europe', 'Asia'] AS classes
UNION ALL 
SELECT 'Singapore', ['North America', 'Europe', 'Asia'];

SELECT *
FROM text_classification_table;

SELECT input,
       classes,
       SNOWFLAKE.CORTEX.CLASSIFY_TEXT(input, classes)['label']::varchar as classification
FROM text_classification_table;

/***********************************************************************
  SENTIMENT, SUMMARIZE, CLASSIFY_TEXT, TRANSLATE   
************************************************************************/
SELECT id, transcript
FROM demo.demo.call_transcripts;


--RUN INSIDE CTE FIRST, THEN RUN ENTIRE QUERY. 
CREATE OR REPLACE TRANSIENT TABLE DEMO.DEMO.TRANSCRIPT_INFO AS
SELECT 
    ID, 
    TRANSCRIPT,
    AI_COMPLETE(
        model => 'claude-3-5-sonnet',
        prompt => CONCAT(
            'What is the first name of the call center rep from XYZ Corp from this transcript? ','<transcript>',TRANSCRIPT,'</transcript>'),
        response_format => {
            'type': 'json',
            'schema': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'}
                }
            }
        }):name::string as rep_name,
    SNOWFLAKE.CORTEX.SENTIMENT(TRANSCRIPT) AS sentiment_score,
    SNOWFLAKE.CORTEX.SUMMARIZE(TRANSCRIPT) AS summary,
    --SNOWFLAKE.CORTEX.TRANSLATE(TRANSCRIPT, 'en', 'es') AS spanish_translation,  
FROM 
    demo.demo.call_transcripts
;

SELECT *
FROM DEMO.DEMO.TRANSCRIPT_INFO;

SELECT 
    REP_NAME,
    AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE
FROM DEMO.DEMO.TRANSCRIPT_INFO
GROUP BY 
    REP_NAME
ORDER BY 
    AVG_SENTIMENT_SCORE;

SELECT AI_AGG(TRANSCRIPT, 'Summarize the top 3 most common complaints about our product. I am looking for themes.')
FROM DEMO.DEMO.TRANSCRIPT_INFO
WHERE SENTIMENT_SCORE < 0.5;

SELECT 
    *,
    SNOWFLAKE.CORTEX.AI_COMPLETE('mistral-large2', CONCAT('Give me one sentence on the most negative thing the customer said in this transcript: <transcript>', TRANSCRIPT, '</transcript> Give me one sentence in the customers exact words')) AS negative_comment  
FROM DEMO.DEMO.TRANSCRIPT_INFO 
WHERE
    sentiment_score < .3;

/***********************************************************************
  EMBED_TEXT_768
************************************************************************/
-- Create the products table
CREATE OR REPLACE TEMPORARY TABLE products (
    ProductID INT PRIMARY KEY,
    ProductName STRING,
    ProductDescription STRING
);

-- Insert sample data
INSERT INTO products (ProductID, ProductName, ProductDescription) VALUES
    (1001, 'Wireless Noise-Canceling Headphones', 'Premium over-ear Bluetooth headphones with active noise cancellation, 30-hour battery life, and immersive sound quality.'),
    (2002, 'Organic Coffee Beans - Dark Roast', 'Rich and bold 100% Arabica coffee beans sourced from Colombia, freshly roasted for a smooth and full-bodied flavor.'),
    (3003, 'Ergonomic Office Chair', 'Adjustable mesh office chair with lumbar support, breathable fabric, and a reclining backrest for all-day comfort.');

SELECT 
    ProductID,
    ProductName,
    ProductDescription,
    --SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', ProductDescription),
    VECTOR_COSINE_SIMILARITY(SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 'A natural morning brew from South America. Crafted for a rich, satisfying taste.'), SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', ProductDescription)) AS Similarity,
FROM products
QUALIFY ROW_NUMBER() OVER (ORDER BY Similarity DESC) = 1
;


/***********************************************************************
  Cleanup
************************************************************************/



/***********************************************************************
  Script to load call_transcripts table from stage.
  Not needed unless the call_transcripts table somehow gets deleted. 
************************************************************************/
/*
CREATE TABLE call_transcripts (
id int,
transcript text,
insert_time text 
);

CREATE OR REPLACE FILE FORMAT csv_file_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE;

-- Make sure the data is in the internal stage 
ls @test; 

COPY INTO call_transcripts
FROM @test/CALLS_TRANSCRIPT.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_file_format');


SELECT 
    ID, 
    TRANSCRIPT
FROM
    call_transcripts;
*/

/***********************************************************************
  End
************************************************************************/





