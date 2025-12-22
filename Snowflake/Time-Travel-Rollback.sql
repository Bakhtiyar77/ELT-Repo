// Setting up table

CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);
    


CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    type = csv
    field_delimiter = ','
    skip_header = 1;
    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    file_format = MANAGE_DB.file_formats.csv_file;
    


LIST @MANAGE_DB.external_stages.time_travel_stage;



COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');


SELECT * FROM OUR_FIRST_DB.public.test;

// Use-case: Update data (by mistake)

UPDATE OUR_FIRST_DB.public.test
SET FIRST_NAME = 'Joyen' ;



// // // Using time travel: Method 1 - 2 minutes back
SELECT * FROM OUR_FIRST_DB.public.test at (OFFSET => -60*3);





// // // Using time travel: Method 2 - before timestamp
SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2021-04-15 17:47:50.581'::timestamp);


-- Setting up table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);

COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');


SELECT * FROM OUR_FIRST_DB.public.test;


2021-04-17 08:16:24.259

-- Setting up UTC time for convenience


ALTER SESSION SET TIMEZONE ='UTC'
SELECT DATEADD(DAY, 1, CURRENT_TIMESTAMP);



UPDATE OUR_FIRST_DB.public.test
SET Job = 'Data Scientist';


SELECT * FROM OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2021-04-16 07:30:47.145'::timestamp);








// // // Using time travel: Method 3 - before Query ID

// Preparing table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Phone string,
  Job string);

COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');


SELECT * FROM OUR_FIRST_DB.public.test;


// Altering table (by mistake)
UPDATE OUR_FIRST_DB.public.test
SET EMAIL = null;



SELECT * FROM OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9ee5-0500-8473-0043-4d8300073062');

/*//////////////ASSIGNMENT//////////////////*/
-- Switch to role of accountadmin --
 
USE ROLE ACCOUNTADMIN;
USE DATABASE OUR_FIRST_DB;
USE WAREHOUSE COMPUTE_WH;
 
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.PART
AS
SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART";
 
SELECT * FROM OUR_FIRST_DB.PUBLIC.PART
ORDER BY P_MFGR DESC;

UPDATE OUR_FIRST_DB.PUBLIC.PART
SET P_MFGR='Manufacturer#CompanyX'
WHERE P_MFGR='Manufacturer#5';

SELECT * FROM OUR_FIRST_DB.PUBLIC.PART AT (OFFSET => -60*10)
SELECT * FROM OUR_FIRST_DB.PUBLIC.PART BEFORE(STATEMENT => '01bff317-0003-bb51-000a-0ba200011aca')







/*//////////////////////////////////////////*/








// Setting up table

CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Job string,
  Phone string);
    

COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.test;

// Use-case: Update data (by mistake)

UPDATE OUR_FIRST_DB.public.test
SET LAST_NAME = 'Tyson';


UPDATE OUR_FIRST_DB.public.test
SET JOB = 'Data Analyst';

SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9eea-0500-845a-0043-4d830007402a');



// // // Bad method

CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test as
SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9eea-0500-845a-0043-4d830007402a');


SELECT * FROM OUR_FIRST_DB.public.test;


CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test as
SELECT * FROM OUR_FIRST_DB.public.test before (statement => '019b9eea-0500-8473-0043-4d830007307a');

// // // Good method

CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test_backup as
SELECT COUNT(*) FROM OUR_FIRST_DB.public.test before (statement => '01bff388-0003-bb51-000a-0ba200011c5e');

TRUNCATE OUR_FIRST_DB.public.test;

INSERT INTO OUR_FIRST_DB.public.test
SELECT * FROM OUR_FIRST_DB.public.test_backup;

SELECT * FROM OUR_FIRST_DB.public.test;

/*******************************************ASSIGNMENT*******************************************************/

CREATE DATABASE TIMETRAVEL_EXERCISE;
CREATE SCHEMA TIMETRAVEL_EXERCISE.COMPANY_X;

CREATE TABLE CUSTOMER AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
LIMIT 500;


UNDROP SCHEMA TIMETRAVEL_EXERCISE.COMPANY_X;
/***********************************************************************************************************/

SHOW TABLES like '%CUSTOMERS%';

ALTER TABLE OUR_FIRST_DB.PUBLIC.CUSTOMERS
SET DATA_RETENTION_TIME_IN_DAYS = 1;

CREATE OR REPLACE TABLE OUR_FIRST_DB.public.ret_example (
    id int,
    first_name string,
    last_name string,
    email string,
    gender string,
    Job string,
    Phone string)
DATA_RETENTION_TIME_IN_DAYS = 3;

SHOW TABLES like '%EX%';

DROP TABLE OUR_FIRST_DB.public.ret_example;
UNDROP TABLE OUR_FIRST_DB.public.ret_example;


ALTER TABLE OUR_FIRST_DB.public.ret_example
SET DATA_RETENTION_TIME_IN_DAYS = 0;

