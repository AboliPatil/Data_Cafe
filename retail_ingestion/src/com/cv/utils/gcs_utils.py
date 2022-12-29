def write_gcs(spark, df, table_config, default):
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", default["pem_file"])
    df.write.option("header", True) \
        .partitionBy(table_config["partition_column_list"].split(",")) \
        .mode("append").parquet(table_config['target_gcs_path'])
