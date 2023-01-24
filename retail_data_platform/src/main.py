import configparser
import sys

from src.com.cv.utils import bigquery_utils
from src.com.cv.utils import gcs_utils
from src.com.cv.utils import spark_utils
from src.com.cv.utils import utils


def main(default, table_config):
    spark = spark_utils.get_spark_session(default, table_name)
    df = gcs_utils.read_gcs(spark, table_config, default)
    bigquery_utils.write_bigquery(spark, df, table_config, default)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(utils.get_config_path())
    config.read("config.ini")
    default = config["DEFAULT"]
    # table_name = sys.argv[1]
    tables_run_seq = default["tables_run_seq"].split(",")
    for table_seq in tables_run_seq:
        table_config = config[table_seq]
        main(default, table_config)
