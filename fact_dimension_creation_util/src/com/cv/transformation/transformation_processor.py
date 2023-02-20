from transformation import calender_transformation as ct
from transformation import arithematic_transformations as at
from transformation import general_transformations as gt
from transformation import custom_transformations as cust


def transformation(df, table_config, property):
    if table_config.get(property, None):
        transformations = table_config[property]
        transformation_list = transformations.split(",")
        for transformation_str in transformation_list:
            trans_list = transformation_str.split("::")
            column = trans_list[0].split(":")
            transformation = trans_list[1]
            print(transformation)
            new_col_name = trans_list[2] if trans_list[2] else column
            print(column)
            df = transform(df, transformation, new_col_name, table_config, *column)
        return df
    return df


def transformation_processor(df, table_config, is_preprocessor=True):
    if is_preprocessor:
        df = transformation(df, table_config, "transformation")
    elif not is_preprocessor:
        df = transformation(df, table_config, "postprocessor")
    return df


def transform(df, transformation, new_col_name, table_config, *column_name):
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
    elif transformation.lower() == "weekofyear":
        return ct.week_of_year(df, column_name[0], new_col_name)
    elif transformation.lower() == "substract" or transformation.lower() == "substraction" or transformation.lower() == "minus":
        return at.add(df, new_col_name, *column_name)
    elif transformation.lower() == "substractpercent":
        return cust.substract_percent(df, new_col_name, *column_name)
    elif transformation.lower() == "date":
        return ct.date(df, column_name[0], new_col_name)
    elif transformation.lower() == "return_same":
        return gt.return_same(df, new_col_name, column_name[0])
    elif transformation.lower() == "cummulativepermanentmark" or transformation.lower() == "cpm":
        return cust.CummulativePermanentMark(df, new_col_name,table_config)
    elif transformation.lower() == "cummulativetemporarymark" or transformation.lower() == "ctm":
        return cust.CummulativeTemporaryMark(df, new_col_name,table_config)
    elif transformation.lower() == "BeginningCumulativeMark" or transformation.lower() == "bcm":
        return cust.BeginningCumulativeMark(df, new_col_name,*column_name)
    elif transformation.lower() == "CalendarReportingPeriodID" or transformation.lower() == "cal_periodid":
        return cust.CalendarReportingPeriodID(df, new_col_name ,column_name[0])
    elif transformation.lower() == "CurrentUnitCount" or transformation.lower() == "current_cnt":
        return cust.CurrentUnitCount(df, new_col_name ,*column_name)
    elif transformation.lower() == "averageInventory" or transformation.lower() == "average":
        return cust.averageInventory(df, new_col_name, *column_name)
    elif transformation.lower() == "cogsale" or transformation.lower() == "cogsale%":
        return cust.cogSalepercent(df, new_col_name, *column_name)