import os
from datetime import datetime, timezone

from pyspark.sql import functions as f


def get_config_path():
    cwd = os.getcwd()
    nvd = os.path.abspath(os.path.join(cwd, os.pardir))
    config_path = os.path.abspath(os.path.join(nvd, "resources", "config.ini"))
    return config_path


def create_partion_columns(df):
    date = datetime.now(timezone.utc)
    df = df.withColumn("year", f.lit(date.year))
    df = df.withColumn("month", f.lit(date.month))
    df = df.withColumn("day", f.lit(date.day))
    df = df.withColumn("hour", f.lit(date.hour))
    return df
