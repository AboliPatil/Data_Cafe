import configparser
import random
from datetime import  datetime
from pyspark.sql.types import DateType
from read import read_local, read_bigquery
from write import write_bigquery
import utils
import ingestion_log
import findspark
# findspark.init()

config = configparser.ConfigParser()
config.read(utils.get_config_path())
default = config["DEFAULT"]
local = config["local"]
project_id = default['project_id']
print(project_id)

def main(default, local):
    findspark.init()
    table_name = default["table_name"]
    starttime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    job_id = random.randint(5, 1000)

    spark = utils.get_spark_session(default)
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    file_path = local["file_path"]
    df_new = read_local(spark,file_path)
    # cast the INGESTIONTIME to date
    df_new = df_new.withColumn("INGESIONTIME", df_new.INGESIONTIME.cast(DateType()))
    df_0ld = read_bigquery(spark, default)
    # df_0ld.show()

    #creating temp view of both df
    df_0ld.createOrReplaceTempView('df_0ld')
    df_new.createOrReplaceTempView('df_new')

    # df_0ld.printSchema()
    # df_0ld.count()
    # df_0ld.show()

    # extracting the max date from old data and convert it into str
    max_ingestion_time = spark.sql("select max(INGESIONTIME) as INGESIONTIME from df_0ld")
    # max_ingestion_time.printSchema()

    # max_ingestion_time.show()
    max_date = max_ingestion_time.collect()[0][0]
    # print(max_date)

    incr_df = spark.sql(f"select * from df_new where INGESIONTIME > cast('{max_date}' as timestamp)")
    change_data_count = incr_df.count()
    # incr_df.printSchema()
    print("inc df")
    incr_df.show()

    try:

        write_bigquery(spark, incr_df, default)
        status = "successful"
        endtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    except:

        status = "unsuccessful"
        endtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


    df = ingestion_log.get_log(job_id,table_name, starttime, endtime, status, change_data_count,spark)
    # df.show()



if __name__ == '__main__':

    main(default, local)

