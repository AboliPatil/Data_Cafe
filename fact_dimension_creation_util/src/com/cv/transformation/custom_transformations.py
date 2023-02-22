from pyspark.sql import functions as f


def substract_percent(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name,
                       ((f.col(column_name[0]) - f.col(column_name[1])) / f.col(column_name[0])) * int(column_name[2]))
    return df
