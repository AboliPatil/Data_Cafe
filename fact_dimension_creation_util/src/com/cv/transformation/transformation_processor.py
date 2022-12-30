from src.com.cv.transformation import calender_transformation as ct
from src.com.cv.transformation import arithematic_transformations as at
from src.com.cv.transformation import general_transformations as gt


def transformation_processor(df, table_config):
    if table_config.get("transformation", None):
        transformations = table_config["transformation"]
        transformation_list = transformations.split(",")
        for transformation_str in transformation_list:
            trans_list = transformation_str.split("::")
            column = trans_list[0].split(":")
            transformation = trans_list[1]
            new_col_name = trans_list[2] if trans_list[2] else column
            df = transform(df, transformation, new_col_name, *column)
        return df
    return df


def transform(df, transformation, new_col_name, *column_name):
    if transformation.lower() == "dayofweek":
        return ct.day_of_week(df, column_name[0], new_col_name)
    elif transformation.lower() == "dayofmonth":
        return ct.day_of_month(df, column_name[0], new_col_name)
    elif transformation.lower() == "dayofyear":
        return ct.day_of_year(df, column_name[0], new_col_name)
    elif transformation.lower() == "monthnumber":
        return ct.month_number(df, column_name[0], new_col_name)
    elif transformation.lower() == "monthname":
        return ct.month_name(df, column_name[0], new_col_name)
    elif transformation.lower() == "yearmonth":
        return ct.year_month(df, column_name[0], new_col_name)
    elif transformation.lower() == "quarter":
        return ct.quarter(df, column_name[0], new_col_name)
    elif transformation.lower() == "multiply":
        return at.multiply(df, new_col_name, *column_name)
    elif transformation.lower() == "lit":
        return gt.lit(df, new_col_name, column_name[0])
    elif transformation.lower() == "addition" or transformation.lower() == "add":
        return at.add(df, new_col_name, *column_name)
