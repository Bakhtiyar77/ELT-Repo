--create table Employee for file format 
CREATE OR REPLACE TABLE Employee
(customer_id int,
first_name varchar(50),
last_name varchar(50),
email varchar(50),
age int,
department varchar(50))

--create stage and file format for copy options example 2
CREATE OR REPLACE STAGE stage_assign_6
url = 's3://snowflake-assignments-mc/copyoptions/example2'

--list stage to verify files
list @stage_assign_6

--create file format
CREATE FILE FORMAT fileformat_assign_6

--create copy into table with different copy options 
COPY INTO Employee
    from @stage_assign_6
    file_format = (format_name = fileformat_assign_6 FIELD_DELIMITER=',' SKIP_HEADER = 1)
    VALIDATION_MODE = RETURN_ERRORS;

COPY INTO Employee
    from @stage_assign_6
    file_format = (format_name = fileformat_assign_6 FIELD_DELIMITER=',' SKIP_HEADER = 1)
    TRUNCATECOLUMNS = TRUE 

select * from employee