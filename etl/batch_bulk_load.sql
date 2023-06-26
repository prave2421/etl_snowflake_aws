grant usage on integration aws_sf_data to role sysadmin;

create database Bank_Commercial;
use Bank_Commercial;
create schema bk_curated_data;

--BANK_COMMERCIAL.BK_CURATED_DATA

use schema "Bank_Commercial"."bk_curated_data";


--aws integarion set up
CREATE STORAGE INTEGRATION aws_sf_data_v1
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::069400667568:role/aws-snowflake-access'
  STORAGE_ALLOWED_LOCATIONS = ('s3://acct-csv-data/');

--truncate table  BANK_COMMERCIAL.BK_CURATED_DATA.ACCOUNTS
-- Create an empty table  ---


--file format ceation

CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER =1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    TRIM_SPACE = FALSE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO';

    ----stage creation
create stage stg_acct_csv_dev
storage_integration = aws_sf_data
url = 's3://dev-ecommerce-data/accts/'
file_format = csv_load_format;

--drop stage stg_acct_csv_dev;


-- List the contents of stage  ---
list @stg_acct_csv_dev;

--bulk load manual
copy into accounts
from @stg_acct_csv_dev
file_format = csv_load_format
ON_ERROR = ABORT_STATEMENT;

select * from information_schema.load_history where table_name='ACCOUNTS' order by last_load_time desc limit 10;


select * from accounts

--task creation for scheduling

CREATE TASK mytask_acct_run_M
  WAREHOUSE = 'TEST_LARGE'
  SCHEDULE = '2 MINUTE'
AS
copy into accounts
from @stg_acct_csv_dev
file_format = csv_load_format
ON_ERROR = ABORT_STATEMENT;

SHOW TASKS;
--CHECK TASK HISTORY
--SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'mytask_acct_run_M';


--start and stop tasks
ALTER TASK mytask_acct_run_M RESUME;

ALTER TASK mytask_acct_run_M SUSPEND;




select count(*) from accounts



