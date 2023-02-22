from pyspark.sql import functions as f


def lit(df, new_col_name, const_name):
    df = df.withColumn(new_col_name, f.lit(const_name))
    return df
