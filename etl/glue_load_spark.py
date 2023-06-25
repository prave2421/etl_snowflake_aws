import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
    .builder \
    .appName("GlueJobDemo") \
    .getOrCreate()


def main():
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    snowflake_database = "ECOMMERCE_DB"
    snowflake_schema = "ECOMMERCE_DEV"
    source_table_name = "LINEITEM"
    snowflake_options = {
        "sfUrl": "gbtnfuf-kv47012.snowflakecomputing.com",
        "sfUser": "praveen2022",
        "sfPassword": "Spider@2019",
        "sfDatabase": snowflake_database,
        "sfSchema": snowflake_schema,
        "sfWarehouse": "COMPUTE_WH"
    }
    args = getResolvedOptions(sys.argv, ["file", "bucket"])
    file_name = args['file']
    bucket_name = args['bucket']
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

    df4 = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(input_file_path)
    df4.write.format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", "test_glue_t").mode("overwrite") \
        .save()


main()