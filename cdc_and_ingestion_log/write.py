def write_bigquery(spark, df, default):
    temporary_gcs_bucket = default["temporary_gcs_bucket"]
    project_id = default["project_id"]
    dataset_name = default["dataset_name"]
    destination_table = default["destination_table"]

    spark.conf.set("temporaryGcsBucket", temporary_gcs_bucket)

    df.write.format("bigquery") \
        .option("parentProject", project_id) \
        .option("project", dataset_name) \
        .option("table", f"{project_id}.{dataset_name}.{destination_table}") \
        .mode("append") \
        .save()
