# from google.cloud import storage
# import os

def read_gcs(spark, table_config, default):
    base_path = default["gcs_base_path"]
    file_path = base_path + "/" + table_config["relative_path"]
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", default["pem_file"])
    df = spark.read.option("basePath", default["gcs_base_path"]).parquet(file_path)
    return df


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