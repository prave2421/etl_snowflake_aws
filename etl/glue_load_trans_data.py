import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark import SparkContext
from datetime import datetime

#Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


spark = SparkSession \
    .builder \
    .appName("GlueJobDemo") \
    .getOrCreate()


def main():
    # Log starting time
    dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Start time:", dt_start)
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "BANK_COMMERCIAL"
    snowflake_schema = "BK_CURATED_DATA"
    source_table_name = "LINEITEM"
    snowflake_options = {
        "sfUrl": "gbtnfuf-kv47012.snowflakecomputing.com",
        "sfUser": "praveen2022",
        "sfPassword": "Spider@2019",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "COMPUTE_WH"
    }
    args = getResolvedOptions(sys.argv, ["file", "bucket", "tgttable"])
    file_name = args['file']
    bucket_name = args['bucket']
    tgt_tbl = args['tgttable']
    input_file_path = "s3://{}/{}".format(bucket_name, file_name)
    '''
    df = spark.read \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable",snowflake_database+"."+snowflake_schema+"."+source_table_name) \
        .load()
    df1=df.groupBy("L_SHIPMODE").sum("L_QUANTITY");
    df1.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "lineitem_glue").mode("overwrite") \
        .save()
        '''
    trans_typs_file_path="s3://dev-glue-data-load/ACCOUNT_TRANSACT_TYPES.csv"
    typs_file_path="s3://dev-glue-data-load/ACCOUNT_TYPES.csv"


    df_trans_src = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(input_file_path)
    df_trans_types=spark.read.options(header='True', inferSchema='True', delimiter=',').csv(trans_typs_file_path)
    df_acct_typ=spark.read.options(header='True', inferSchema='True', delimiter=',').csv(typs_file_path)

    df_trans_src.printSchema()
    df_trans_types.printSchema()
    df_acct_typ.printSchema()
    df_trans_types.show()
    df_acct_typ.show()

    df_join1=df_trans_src.join(df_trans_types,['ACTRNTP_KEY'],'inner')
    df_join2=df_join1.join(df_acct_typ,['ACCTP_KEY'],'inner')




    df_join2.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "ACCOUNT_TRANSACTIONS").mode("append") \
        .save()

    # Group by decade: Count movies, get average rating
    data_frame_aggregated = df_join2.groupby("ACTRNTP_DESC").agg(
        f.count(f.col("ACCTRN_ACCOUNTING_DATE")).alias('ACCTRN_ACCOUNTING_DATE_COUNT'),
        f.max(f.col("ACCTRN_AMOUNT_CZK")).alias('ACCTRN_AMOUNT_CZK_MAX'))

    data_frame_aggregated.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "ACCOUNT_TRANSACTIONS_ROLLUP").mode("append") \
        .save()
    # Log end time
    dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Start time:", dt_end)


main()