from pyspark.sql import functions as f


def day_of_week(df, column_name, new_col_name):
    df = df.withColumn(new_col_name, f.dayofweek(f.to_date(f.col(column_name))))
    return df


def day_of_month(df, column_name, new_col_name):
    df = df.withColumn(new_col_name, f.dayofmonth(f.to_date(f.col(column_name))))
    return df


def day_of_year(df, column_name, new_col_name):
    df = df.withColumn(new_col_name, f.dayofyear(f.to_date(f.col(column_name))))
    return df


def month_number(df, column_name, new_col_name):
    df = df.withColumn(new_col_name, f.month(f.to_date(f.col(column_name))))
    return df


def month_name(df, column_name, new_col_name):
    df = df.withColumn(new_col_name, f.date_format(f.to_date(f.col(column_name), "MM"), "MMMM"))
    return df


def year_month(df, column_name, new_col_name):
    df = df.withColumn(new_col_name, f.date_format(f.to_date(f.col(column_name)), "yMM"))
    return df


def quarter(df, column_name, new_col_name):
    df = df.withColumn(new_col_name, f.quarter(f.to_date(f.col(column_name))))
    return df

