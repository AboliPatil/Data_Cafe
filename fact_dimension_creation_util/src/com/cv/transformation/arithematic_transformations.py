from pyspark.sql import functions as f


def multiply(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name, f.col(column_name[0]) * f.col(column_name[1]))
    return df


def add(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name, f.col(column_name[0]) + f.col(column_name[1]))
    return df
