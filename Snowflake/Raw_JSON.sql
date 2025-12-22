// First step: Load Raw JSON

CREATE OR REPLACE stage JSONSTAGE
     url='s3://snowflake-assignments-mc/unstructureddata/';

list @jsonstage

desc @jsonstage
 
CREATE OR REPLACE file format JSONFORMAT
    TYPE = JSON;
    
    
CREATE OR REPLACE table JSON_RAW (
    raw_file variant);
    
COPY INTO JSON_RAW
    FROM @jsonstage
    file_format= JSONFORMAT


select * from json_raw

SELECT
  raw_file:first_name:: STRING first_name,
  raw_file:last_name:: STRING last_name,
  raw_file:Skills[0]::string Skills,
  raw_file:Skills[1]::string Skills_1,
   raw_file:Skills[2]::string Skills_2
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW 



SELECT
  raw_file:first_name::STRING AS first_name,
  raw_file:last_name::STRING AS last_name,
  f.value::STRING AS skill
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW,
LATERAL FLATTEN(raw_file:"Skills") f

// Query from table
SELECT * FROM json_raw
WHERE FIRST_NAME='Florina';


////////////////////////////


SELECT 
    RAW_FILE:spoken_languages as spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT 
     array_size(RAW_FILE:spoken_languages) as spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT 
     RAW_FILE:first_name::STRING as first_name,
     array_size(RAW_FILE:spoken_languages) as spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;



SELECT 
    RAW_FILE:spoken_languages[0] as First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT 
    RAW_FILE:first_name::STRING as first_name,
    RAW_FILE:spoken_languages[0] as First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT 
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[0].language::STRING as First_language,
    RAW_FILE:spoken_languages[0].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;




SELECT 
    RAW_FILE:id::int as id,
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[0].language::STRING as First_language,
    RAW_FILE:spoken_languages[0].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int as id,
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[1].language::STRING as First_language,
    RAW_FILE:spoken_languages[1].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::int as id,
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[2].language::STRING as First_language,
    RAW_FILE:spoken_languages[2].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY ID;




select
      RAW_FILE:first_name::STRING as First_name,
      f.value:language::STRING as First_language,
      f.value:level::STRING as Level_spoken
from OUR_FIRST_DB.PUBLIC.JSON_RAW,
LATERAL FLATTEN(input => RAW_FILE:spoken_languages)f
// table(flatten(RAW_FILE:spoken_languages))f;

select * from jsontest
