from pyspark.sql import functions as f
from functools import reduce


def multiply(df, new_col_name, *column_name):
    sun_val = f.col(column_name[0])
    for i in range(1, len(list(column_name))):
        sun_val = sun_val * f.col(column_name[i])
    df = df.withColumn(new_col_name, sun_val)
    return df


def add(df, new_col_name, *column_name):
    sun_val = 0
    for i in range(len(list(column_name))):
        sun_val = sun_val + f.col(column_name[i])
    df = df.withColumn(new_col_name, sun_val)
    return df


def substract(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name, f.col(column_name[0]) - f.col(column_name[1]))
    return df
