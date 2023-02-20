import configparser
import json

from transformation import transformation_processor as tp
from aggregation import aggregation_processor as ap
from utils import spark_utils, bigquery_utils
from utils import utils
from utils import gcs_utils


def create_temp_views(table_list, table_df_dict):
    for table in table_list:
        table_df_dict[table].createOrReplaceTempView(table)
        print("The table name is "+ table)
        #table_df_dict[table].show(20,False)

def create_joined_df(spark, table_config):
    df = spark.sql(table_config["join_query"])
    return df

def execute_spark_sql(spark,df , table_config):
    query = table_config.get("query", None)
    print("Table config in executable *****=== ")
    print(table_config.get("destination_table"))
    if query:
        df.createOrReplaceTempView(table_config.get("destination_table"))
        print("execute spark sql === ")
        df.show()
        df = spark.sql(query)
    return df



def select(df, table_config):
    if table_config.get("columns_to_select", None):
        df = df.selectExpr(table_config["columns_to_select"].split(","))
        return df
    return df


def get_gcs_config(table_config):
    required_input_tables_json = json.loads(table_config["required_input_tables_json"])
    table_default_path_dict = {key.get("table_name"): key.get("base_path") for key in required_input_tables_json}
    table_list = table_default_path_dict.keys()
    table_df_dict = {key.get("table_name"): None for key in required_input_tables_json}
    return table_default_path_dict, table_df_dict, table_list


def get_bq_config(table_config):
    required_input_tables_json = json.loads(table_config["required_input_tables_json"])
    table_default_dataset_dict = {key.get("table_name"): key.get("dataset_name") for key in required_input_tables_json}
    table_default_project_id_dict = {key.get("table_name"): key.get("project_id") for key in required_input_tables_json}
    table_list = table_default_dataset_dict.keys()
    table_df_dict = {key.get("table_name"): None for key in required_input_tables_json}
    return table_default_dataset_dict, table_default_project_id_dict, table_df_dict, table_list


def postprocessor(df, table_config):
    tp.transformation_processor(df, table_config)

def main(default, table_config):
    spark = spark_utils.get_spark_session(default)
    if default.get("is_bigquery_read", None):
        table_default_dataset_dict, table_default_project_id_dict, table_df_dict, table_list = get_bq_config(
            table_config)
        bigquery_utils.read_bigquery(spark, default, table_config, table_default_dataset_dict,
                                     table_default_project_id_dict, table_df_dict, table_list)
    else:
        table_default_path_dict, table_df_dict, table_list = get_gcs_config(table_config)
        gcs_utils.read_gcs(spark, default, table_default_path_dict, table_df_dict)
    create_temp_views(table_list, table_df_dict)
    df = create_joined_df(spark, table_config)
    df = tp.transformation_processor(df, table_config, True)
    # df.show(20, False)
    df = ap.aggregation_processor(df, table_config)
    df = execute_spark_sql(spark, df, table_config)
    # df.show(20, False)
    df = select(df, table_config)
    df.show(20, False)
    bigquery_utils.write_bigquery(spark, df, table_config, default)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    #config.read(utils.get_config_path())
    config.read(
        r"C:\Users\Ganesh Dabhade\Desktop\data_cafe\git\Data_Cafe\fact_dimension_creation_util\resources\usecases\config.ini")
    default = config["DEFAULT"]
    tables_run_seq = default["tables_run_seq"].split(",")
    for table_seq in tables_run_seq:
        table_config = config[table_seq]
        main(default, table_config)
