-- ---- VALIDATION_MODE ----
-- // Prepare database & table
-- CREATE OR REPLACE DATABASE COPY_DB;


-- CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
--     ORDER_ID VARCHAR(30),
--     AMOUNT VARCHAR(30),
--     PROFIT INT,
--     QUANTITY INT,
--     CATEGORY VARCHAR(30),
--     SUBCATEGORY VARCHAR(30));

-- // Prepare stage object
-- CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
--     url='s3://snowflakebucket-copyoption/size/';
  
-- LIST @COPY_DB.PUBLIC.aws_stage_copy;
  
    
--  //Load data using copy command
-- COPY INTO COPY_DB.PUBLIC.ORDERS
--     FROM @aws_stage_copy
--     file_format= (type = csv field_delimiter=',' skip_header=1)
--     pattern='.*Order.*'
--     VALIDATION_MODE = RETURN_ERRORS;
    
-- SELECT * FROM ORDERS;    
    
-- COPY INTO COPY_DB.PUBLIC.ORDERS
--     FROM @aws_stage_copy
--     file_format= (type = csv field_delimiter=',' skip_header=1)
--     pattern='.*Order.*'
--    VALIDATION_MODE = RETURN_5_ROWS ;



-- --- Use files with errors ---

-- create or replace stage copy_db.public.aws_stage_copy
--     url ='s3://snowflakebucket-copyoption/returnfailed/';
    
-- list @copy_db.public.aws_stage_copy;

-- -- show all errors --
-- copy into copy_db.public.orders
--     from @copy_db.public.aws_stage_copy
--     file_format = (type=csv field_delimiter=',' skip_header=1)
--     pattern='.*Order.*'
--     validation_mode=return_errors;

-- -- validate first n rows --
-- copy into copy_db.public.orders
--     from @copy_db.public.aws_stage_copy
--     file_format = (type=csv field_delimiter=',' skip_header=1)
--     pattern='.*error.*'
--     validation_mode=return_1_rows

--     CREATE TABLE Employees (
--   customer_id int,

--   first_name varchar(50),

--   last_name varchar(50),

--   email varchar(50),

--   age int,

--   department varchar(50))


-- CREATE STAGE copy_db.public.assign_employee
-- url = 's3://snowflake-assignments-mc/copyoptions/example1'

-- list @assign_employee

-- CREATE FILE FORMAT assign_fileformat

-- COPY INTO EMPLOYEES 
-- FROM @assign_employee
-- FILE_FORMAT =(FORMAT_NAME = assign_fileformat  FIELD_DELIMITER=',' SKIP_HEADER=1)
-- //ON_ERROR = 'CONTINUE'
-- VALIDATION_MODE = RETURN_ALL_ERRORS;

-- SELECT * FROM EMPLOYEES

   
---- Use files with errors ----
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3://snowflakebucket-copyoption/returnfailed/';

LIST @COPY_DB.PUBLIC.aws_stage_copy;    



COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;



COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_1_rows;
    



-------------- Working with error results -----------

---- 1) Saving rejected files after VALIDATION_MODE ---- 

CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));


COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;


// Storing rejected /failed results in a table
CREATE OR REPLACE TABLE rejected AS 
select rejected_record from table(result_scan('01bfc61d-0001-514b-000a-efa20003625e'));


select * from rejected
-- Adding additional records --
INSERT INTO rejected  
select rejected_record from table(result_scan(last_query_id()));



SELECT * FROM rejected;




---- 2) Saving rejected files without VALIDATION_MODE ---- 
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    ON_ERROR=CONTINUE;
  
select * from table(validate(orders, job_id => '_last'));

---- 3) Working with rejected records ---- 



SELECT REJECTED_RECORD FROM rejected;

CREATE OR REPLACE TABLE rejected_values as
SELECT 
SPLIT_PART(rejected_record,',',1) as ORDER_ID, 
SPLIT_PART(rejected_record,',',2) as AMOUNT, 
SPLIT_PART(rejected_record,',',3) as PROFIT, 
SPLIT_PART(rejected_record,',',4) as QUATNTITY, 
SPLIT_PART(rejected_record,',',5) as CATEGORY, 
SPLIT_PART(rejected_record,',',6) as SUBCATEGORY
FROM rejected; 


SELECT * FROM rejected_values;