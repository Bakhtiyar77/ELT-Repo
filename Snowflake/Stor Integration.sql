
// Create storage integration object

create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::062507528784:role/SnowflakeAccesRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://barrysnowflakes3bucket/CSV/', 's3://barrysnowflakes3bucket/JSON/')
   COMMENT = 'This an optional comment';
   
   
// See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int;

CREATE DATABASE OUR_FIRST_DB

// Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.movie_titles (
  show_id STRING,
  type STRING,
  title STRING,
  director STRING,
  cast STRING,
  country STRING,
  date_added STRING,
  release_year STRING,
  rating STRING,
  duration STRING,
  listed_in STRING,
  description STRING );
  
  
create database MANAGE_DB
CREATE SCHEMA external_stages

// Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.json_fileformat
    type = JSON
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;
    

 // Create stage object with integration object & file format object
CREATE OR REPLACE stage MANAGE_DB.external_stages.json_folder
    URL = 's3://barrysnowflakes3bucket/JSON/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.json_fileformat;



// Use Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.movie_titles
    FROM @MANAGE_DB.external_stages.csv_folder;
    
    
    
    
    
// Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE    
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' ;
    
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.movie_titles;