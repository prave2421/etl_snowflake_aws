from snowflake.connector import connect
import pandas as pd



user_name ='praveen2022'
password ='Spider@2019'
account ='gbtnfuf-kv47012'
warehouse ='test_large'
database ='BANK_COMMERCIAL'
schema ='BK_CURATED_DATA'

sql_query = """select * from BANK_COMMERCIAL.BK_CURATED_DATA.UNIVERSITY_ACCT where STATE is NULL limit 10"""




def build_conn(user_name,password,account,warehouse,database,schema):
    conn = connect(
        user=user_name,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    return conn


def build_sql(sql_query):
    conn_str = build_conn(user_name,password,account,warehouse,database,schema)
    db_cursor_eb = conn_str.cursor()
    print(db_cursor_eb)
    db_cursor_eb.execute(sql_query)
    df = db_cursor_eb.fetch_pandas_all()
    return df

def main():
    user_name = 'praveen2022',
    password = 'Spider@2019',
    account = 'gbtnfuf-kv47012',
    warehouse = 'test_large',
    database = 'BANK_COMMERCIAL',
    schema = 'BK_CURATED_DATA'

    sql_query = """select * from BANK_COMMERCIAL.BK_CURATED_DATA.UNIVERSITY_ACCT where STATE is NULL limit 10"""
    print(12)
    df_n=build_sql(sql_query)
    values={"STATE":'Default'}
    df1=df_n.fillna(value=values)
    print(df1.to_string())

main()
