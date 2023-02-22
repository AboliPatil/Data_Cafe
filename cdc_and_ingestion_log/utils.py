from pyspark.sql import *
import os

def get_spark_session(default, table_name="generic"):
    mysql_jar_path = default.get("mysql_jar_path")
    bigquery_jar = default.get("bigquery_jar")
    project_id = default["project_id"]
    spark = SparkSession \
        .builder \
        .appName(table_name) \
        .config('parentProject',project_id ) \
        .config("credentialsFile", default["pem_file"]) \
        .config("spark.driver.extraClassPath", mysql_jar_path) \
        .config("spark.jars", bigquery_jar) \
        .config("spark.driver.memory", "1g")\
        .getOrCreate()
    return spark


def get_config_path():

    return "resource/config.ini"

# def get_spark_session(default, table_name="generic"):
#     mysql_jar_path = default.get("mysql_jar_path")
#     bigquery_jar = default.get("bigquery_jar")
#     spark = SparkSession \
#         .builder \
#         .appName(table_name) \
#         .config('parentProject', default["project_id"]) \
#         .config("credentialsFile", default["pem_file"]) \
#         .config("spark.driver.extraClassPath", mysql_jar_path) \
#         .config("spark.jars", f"{bigquery_jar},C:\\Users\\kshitij183663\\spark\\spark-3.3.1-bin-hadoop2\\jars\\gcs-connector-latest-hadoop2.jar")\
#         .getOrCreate()
#     return spark