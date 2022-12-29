import configparser
import sys

from src.com.cv.utils import gcs_utils
from src.com.cv.utils import spark_utils
from src.com.cv.utils import sql_read
from src.com.cv.utils import utils


def main(default, table_config):
    spark = spark_utils.get_spark_session(default, table_name)
    df = sql_read.read_from_sql(spark, default, table_name)
    df = utils.create_partion_columns(df)
    gcs_utils.write_gcs(spark, df, table_config, default)


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(utils.get_config_path())
    default = config["DEFAULT"]
    table_name = sys.argv[1]
    table_config = config[table_name]
    main(default, table_config)
