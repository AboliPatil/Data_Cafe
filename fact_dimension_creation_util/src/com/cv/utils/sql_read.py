def read_from_sql(spark, default, table_name):
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost/" + default['db_name']) \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", default['username']) \
        .option("password", default['password']) \
        .load()
    return df
