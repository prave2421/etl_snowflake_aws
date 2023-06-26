from snowflake.connector import connect
import pandas as pd
from IPython.display import display, HTML

conn=connect(
      user='praveen2022',
                password='Spider@2019',
                account='gbtnfuf-kv47012',
                warehouse='test_large',
                database='BANK_COMMERCIAL',
                schema='BK_CURATED_DATA'
                )
#create cursor
db_cursor_eb=conn.cursor()


sql_query = """select * from BANK_COMMERCIAL.BK_CURATED_DATA.UNIVERSITY_ACCT where STATE is NULL limit 10"""

#Create the cursor object with your SQL command

db_cursor_eb.execute(sql_query)

#Convert output to a dataframe

df = db_cursor_eb.fetch_pandas_all()
#print(df)
#print(pd.isna(df))
values={"STATE":'Default'}

df1=df.fillna(value=values)
print(df1.to_string())

db_cursor_eb.close()