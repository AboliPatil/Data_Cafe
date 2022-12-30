# from google.cloud import storage
# import os

def read_gcs(spark, default, table_default_path_dict, table_df_dict):
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", default["pem_file"])
    default_base_path = default["gcs_base_path"]
    for table_name, base_path in table_default_path_dict.items():
        if base_path:
            file_path = base_path + "/" + table_name.lower()
        else:
            base_path = default_base_path
            file_path = default_base_path + "/" + table_name.lower()
        df = spark.read.option("basePath", base_path).parquet(file_path)
        table_df_dict[table_name] = df


def write_gcs(spark, df, table_config, default):
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", default["pem_file"])
    df.write.option("header", True) \
        .partitionBy("year", "month", "day", "hour") \
        .mode("append").parquet(table_config['target_gcs_path'])

# def move_data(default, table_config):
#     stage_path = table_config["stage_path"]
#     history_path = table_config["history_path"]
#     bucket_name = table_config["bucket_name"]
#     storage_client = storage.Client()
#     source_bucket = storage_client.get_bucket(bucket_name)
#     source_blob = source_bucket.blob(blob_name)
#     destination_bucket = storage_client.get_bucket(new_bucket_name)
#
#     # copy to new destination
#     new_blob = source_bucket.copy_blob(
#         source_blob, destination_bucket, new_blob_name)
#     # delete in old destination
#     source_blob.delete()