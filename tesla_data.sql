--Create Database
CREATE DATABASE TEST_DB1;

--Create our first table
CREATE TABLE TESLA_DATA(
  Date date,
  Open_value double,
  High_value double,
  Low_value double,
  Close_value double,
  Adj_Close double,
  volume bigint
  );

select * from tesla_data;

drop table tesla_data;

undrop table tesla_data;

update tesla_data set open_value = 200 where date = '2022-08-01';

select * from tesla_data before (statement => '01a92e91-0604-35cd-0000-003e1c87c251');
--Create External S3 Stage

CREATE OR REPLACE STAGE BULK_COPY_TESLA_STAGE URL='s3://snow-flake-computing-pro/TSLA.csv'
CREDENTIALS = (AWS_KEY_ID = 'AKIA5HR3523CQQGLRZOU' AWS_SECRET_KEY ='otDB93qw+gGlQ3NWZnQU2rN2Nz9xQ/qmxeB300Np');

--List content of stage 

LIST @BULK_COPY_TESLA_STAGE;

--Copy data from stage into table 

COPY INTO TESLA_DATA
FROM @BULK_COPY_TESLA_STAGE
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

--Read data from table 

SELECT * FROM TESLA_DATA;

truncate tesla_data;

drop table tesla_data;

use role accountadmin;

GRANT CREATE INTEGRATION on account to role sysadmin;
GRANT USAGE on S3_INTEGRATION to ROLE SYSADMIN
--CreateStorage Integration 

CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION_SYSADMIN
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::909585012421:role/Snowflake_Access_Role'
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('s3://snow-flake-computing-pro/Input/')

desc integration S3_INTEGRATION_SYSADMIN;

CREATE OR REPLACE STAGE TESLA_DATA_STAGE_SYSADMIN
URL='s3://snow-flake-computing-pro/Input/'
STORAGE_INTEGRATION = S3_INTEGRATION_SYSADMIN;
FILE_FORMAT=CSV_FORMAT;

LIST @TESLA_DATA_STAGE_SYSADMIN;

select * from tesla_data order by date desc;

COPY INTO TEST_DB1.PUBLIC.TESLA_DATA 
FROM @TESLA_DATA_STAGE_SYSADMIN
FILE_FORMAT=CSV_FORMAT;
PATTERN='.*.csv';

USE TEST_DB1;
CREATE OR REPLACE PIPE TESLA_PIPE_TEST AUTO_INGEST=TRUE AS 
COPY INTO TEST_DB1.PUBLIC.TESLA_DATA 
FROM @TESLA_DATA_STAGE_SYSADMIN
FILE_FORMAT=CSV_FORMAT;

show pipes;