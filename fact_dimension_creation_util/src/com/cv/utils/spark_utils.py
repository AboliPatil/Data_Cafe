import json

from pyspark.sql import SparkSession


def get_spark_session(default, table_name="generic"):
    mysql_jar_path = default.get("mysql_jar_path")
    bigquery_jar = default.get("bigquery_jar")
    spark = SparkSession \
        .builder \
        .appName(table_name) \
        .config("spark.driver.extraClassPath", mysql_jar_path) \
        .config("spark.jars", bigquery_jar) \
        .getOrCreate()
    return spark


def dynamic_df_names(default, table_config):
    join_condition = table_config["join_condition"]
    if join_condition:
        json.loads(join_condition)
