def write_bigquery(spark, df, table_config, default):
    temporary_gcs_bucket = default["temporary_gcs_bucket"]
    project_id = table_config.get("project_id", None) if table_config.get("project_id", None) else default["project_id"]
    dataset_name = table_config.get("dataset_name", None) if table_config.get("dataset_name", None) else default["dataset_name"]
    destination_table = table_config["destination_table"]

    spark.conf.set("temporaryGcsBucket", temporary_gcs_bucket)

    df.write.format("bigquery") \
        .option("parentProject", project_id) \
        .option("project", dataset_name) \
        .option("table", f"{project_id}.{dataset_name}.{destination_table}") \
        .mode("append") \
        .save()


def read_bigquery(spark, default, table_config, table_default_dataset_dict, table_default_project_id_dict,
                  table_df_dict, table_list):
    temporary_gcs_bucket = default["temporary_gcs_bucket"]
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", default["pem_file"])
    for table_name in table_list:
        dataset_name = table_default_dataset_dict[table_name]
        spark.conf.set("temporaryGcsBucket", temporary_gcs_bucket)
        df = spark.read.format('bigquery') \
            .option("project", "evident-ethos-365308") \
            .option("table", f"{dataset_name}.{table_name}") \
            .load()
        table_df_dict[table_name] = df
