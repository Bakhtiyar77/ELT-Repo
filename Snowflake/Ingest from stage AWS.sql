create or replace table Employee
(customer_id int,
first_name varchar(50),
last_name varchar(50),
email varchar(50),
age int,
department varchar(50))

CREATE OR REPLACE STAGE stage_assign_6
url = 's3://snowflake-assignments-mc/copyoptions/example2'

list @stage_assign_6

CREATE FILE FORMAT fileformat_assign_6

COPY INTO Employee
    from @stage_assign_6
    file_format = (format_name = fileformat_assign_6 FIELD_DELIMITER=',' SKIP_HEADER = 1)
    VALIDATION_MODE = RETURN_ERRORS;

COPY INTO Employee
    from @stage_assign_6
    file_format = (format_name = fileformat_assign_6 FIELD_DELIMITER=',' SKIP_HEADER = 1)
    TRUNCATECOLUMNS = TRUE 

select * from employee