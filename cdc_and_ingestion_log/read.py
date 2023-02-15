def read_local(spark, file_path):
    df = spark.read.csv(file_path, header=True)
    return df


def read_bigquery(spark, default):
    pem_file = default['pem_file']
    project_id = default['project_id']
    dataset_name = default['dataset_name']
    # print(dataset_name)
    # table_name = ['table_name']
    # print(table_name)
    table_name = 'Vendor_old'
    df = spark.read.format("bigquery") \
        .option("credentialsFile", pem_file) \
        .option("table", f"{project_id}.{dataset_name}.{table_name}") \
        .load()
    return df