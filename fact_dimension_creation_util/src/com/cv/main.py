import configparser
import json

from src.com.cv.transformation import transformation_processor as tp
from src.com.cv.utils import spark_utils, bigquery_utils
from src.com.cv.utils import utils
from src.com.cv.utils import gcs_utils


def create_temp_views(table_list, table_df_dict):
    for table in table_list:
        table_df_dict[table].createOrReplaceTempView(table)


def create_joined_df(spark, table_config):
    df = spark.sql(table_config["join_query"])
    return df


def select(df, table_config):
    if table_config.get("columns_to_select", None):
        df = df.selectExpr(table_config["columns_to_select"].split(","))
        return df
    return df


def get_config(table_config):
    required_input_tables_json = json.loads(table_config["required_input_tables_json"])
    table_default_path_dict = {key.get("table_name"): key.get("base_path") for key in required_input_tables_json}
    table_list = table_default_path_dict.keys()
    table_df_dict = {key.get("table_name"): None for key in required_input_tables_json}
    return table_default_path_dict, table_df_dict, table_list


def main(default, table_config):
    spark = spark_utils.get_spark_session(default)
    table_default_path_dict, table_df_dict, table_list = get_config(table_config)
    gcs_utils.read_gcs(spark, default, table_default_path_dict, table_df_dict)
    create_temp_views(table_list, table_df_dict)
    df = create_joined_df(spark, table_config)
    df = tp.transformation_processor(df, table_config)
    df = select(df, table_config)
    bigquery_utils.write_bigquery(spark, df, table_config, default)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(utils.get_config_path())
    default = config["DEFAULT"]
    tables_run_seq = default["tables_run_seq"].split(",")
    for table_seq in tables_run_seq:
        table_config = config[table_seq]
        main(default, table_config)
