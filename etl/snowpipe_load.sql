create stage stg_pipe_uni
storage_integration = aws_sf_data
url = 's3://dev-ecommerce-data/api_universitiesReport/'
file_format = csv_load_format;

create or replace TABLE BANK_COMMERCIAL.BK_CURATED_DATA.UNIVERSITY_ACCT (
	ACC_KEY NUMBER(38,0),
	country varchar(100),
	alpha_two_code varchar(100),
	name_details varchar(100),
	state varchar(100),
	domains varchar(100),
	web_page varchar(100)
);

create pipe mypipe_uni_api
  auto_ingest = true
  as
  copy into UNIVERSITY_ACCT
  from @stg_pipe_uni
  file_format = csv_load_format
  ON_ERROR = continue;


show pipes;

select count(*) from university_acct

select * from information_schema.load_history order by last_load_time desc limit 10000;

list @stg_pipe_uni;