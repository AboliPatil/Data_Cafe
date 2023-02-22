from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


# def get_log(job_id,folder_name, starttime, endtime, status, count,spark):
#     spark.conf.set("temporaryGcsBucket", "bigquery_tempfile")
#     record = [{"job_id": job_id,'table_name':folder_name, "start_time": starttime, "end_time":endtime, "status":status, "record_processed":count}]
#
#     df = spark.createDataFrame(record)
#     df.printSchema()
#     # df.show()
#     return df
#     # df.write.format('bigquery') \
#     #     .mode("append") \
#     #     .option("parentProject", "evident-ethos-365308") \
#     #     .option("project", "Ingestion_dataset") \
#     #     .option("table", f"evident-ethos-365308.Ingestion_dataset.Execution_log") \
#     #     .save()

def get_log(job_id,folder_name, starttime, endtime, status, count,spark):
    spark.conf.set("temporaryGcsBucket", "bigquery_tempfile")
    data2 = [(job_id, folder_name, starttime, endtime,status, count)]

    # record = [{"job_id": job_id,'table_name':folder_name, "start_time": starttime, "end_time":endtime, "status":status, "record_processed":count}]
    schema = StructType([
        StructField("job_id", IntegerType(), True),
        StructField("folder_name", StringType(), True),
        StructField("starttime", StringType(), True),
        StructField("endtime", StringType(), True),
        StructField("status", StringType(), True),
        StructField("count", IntegerType(), True)
    ])

    df = spark.createDataFrame(data=data2, schema=schema)
    df.printSchema()
    df.show()

# def get_log(job_id,folder_name, starttime, endtime, status, count,spark):
#
#
#     data2 = [("James", "", "Smith", "36636", "M", 3000),
#              ("Michael", "Rose", "", "40288", "M", 4000),
#              ("Robert", "", "Williams", "42114", "M", 4000),
#              ("Maria", "Anne", "Jones", "39192", "F", 4000),
#              ("Jen", "Mary", "Brown", "", "F", -1)
#              ]
#
#     schema = StructType([
#         StructField("firstname", StringType(), True),
#         StructField("middlename", StringType(), True),
#         StructField("lastname", StringType(), True),
#         StructField("id", StringType(), True),
#         StructField("gender", StringType(), True),
#         StructField("salary", IntegerType(), True)
#     ])
#
#     df = spark.createDataFrame(data=data2, schema=schema)
#     df.printSchema()
#     df.show(truncate=False)