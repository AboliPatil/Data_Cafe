from pyspark.sql import functions as f
from pyspark.sql.window import *
import datetime as dt


def substract_percent(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name,
                       ((f.col(column_name[0]) - f.col(column_name[1])) / f.col(column_name[0])) * int(column_name[2]))
    return df

def CummulativePermanentMark(df, new_col_name, table_config):
    mkdown_items = table_config.get("markdown_item_id")
    mkup_items = table_config.get("markup_item_id")
    mkdown_percentage = table_config.get("markdown_percentage")
    mkup_percentage = table_config.get("markup_percentage")
    mkdown_item_id = tuple(int(i) for i in mkdown_items.split(","))
    mkup_item_id = tuple(int(i) for i in mkup_items.split(","))
    mkdown_valu = int(mkdown_percentage) / 100
    mkup_valu = int(mkup_percentage) / 100
    if new_col_name == 'CummulativePermanentMarkdownAmount':
        df2 = df.withColumn(new_col_name, f.expr(
            f"CASE WHEN ItemID in {mkdown_item_id} THEN (UnitNetCostAmount * {mkdown_valu}) ELSE 0 END"))
        return df2
    elif new_col_name == 'CummulativePermanentMarkupAmount':
        df2 = df.withColumn(new_col_name, f.expr(
            f"CASE WHEN ItemID in {mkup_item_id} THEN (UnitNetCostAmount * {mkup_valu}) ELSE 0 END"))
        return df2


def CummulativeTemporaryMark(df, new_col_name, table_config):
    mkdown_start_date = table_config.get("mkdown_start_date")
    mkdown_offer_duration = table_config.get("mkdown_offer_duration")
    markdown_offer_percentage = table_config.get("markdown_offer_percentage")
    mkup_start_date = table_config.get("markup_start_date")
    mkup_offer_duration = table_config.get("markup_offer_duration")
    markup_offer_percentage = table_config.get("markup_offer_percentage")
    mkdown_start_date = dt.datetime.strptime(mkdown_start_date, "%Y-%m-%d")
    mkdown_end_date = mkdown_start_date + dt.timedelta(days=int(mkdown_offer_duration))
    mkup_start_date = dt.datetime.strptime(mkup_start_date, "%Y-%m-%d")
    mkup_end_date = mkup_start_date + dt.timedelta(days=int(mkup_offer_duration))
    current_date = dt.datetime.now()
    mkdown_offers = table_config.get("mkdown_offer")
    mkdown_offers = tuple(i for i in mkdown_offers.split(","))
    mkup_offers = table_config.get("mkup_offer")
    mkup_offers = tuple(i for i in mkup_offers.split(","))
    mkdown_valu = int(markdown_offer_percentage) / 100
    mkup_valu = int(markup_offer_percentage) / 100

    if new_col_name == 'CummulativeTemporaryMarkdownAmount' and mkdown_start_date < current_date < mkdown_end_date:
        df2 = df.withColumn(new_col_name, f.expr(
            f'CASE WHEN OrderEventTypeCode in {mkdown_offers} THEN (UnitNetCostAmount * {mkdown_valu}) ELSE 0 END'))
        return df2
    elif new_col_name == 'CummulativeTemporaryMarkupAmount' and mkup_start_date < current_date < mkup_end_date:
        df2 = df.withColumn(new_col_name, f.expr(
            f"CASE WHEN OrderEventTypeCode in {mkup_offers} THEN (UnitNetCostAmount * {mkup_valu}) ELSE 0 END"))
        return df2
    elif new_col_name == 'CummulativeTemporaryMarkdownAmount':
        df2 = df.withColumn(new_col_name, f.lit(0.0))
        return df2
    elif new_col_name == 'CummulativeTemporaryMarkupAmount':
        df2 = df.withColumn(new_col_name, f.lit(0.0))
        return df2

def BeginningCumulativeMark(df, new_col_name, *column_name):
    if new_col_name == 'BeginningCumulativeMarkdownAmount':
        df2 = df.withColumn(new_col_name, (f.col(column_name[0]) + f.col(column_name[1])) / f.col(column_name[2]))
        return df2
    elif new_col_name == 'BeginningCumulativeMarkdownPercent':
        df2 = df.withColumn(new_col_name,
                            ((f.col(column_name[0]) + f.col(column_name[1])) / f.col(column_name[2])) * 100)
        return df2


def CalendarReportingPeriodID(df, new_col_name, column):
    window = Window.orderBy(f.col(column))
    df = df.withColumn(new_col_name, f.dense_rank().over(window))
    return df

def CurrentUnitCount(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name,(f.col(column_name[0]) + f.col(column_name[1]) + f.col(column_name[2]))- f.col(column_name[3]))
    return df

def averageInventory(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name,(((f.col(column_name[0]) * f.col(column_name[1])) + f.col(column_name[2]))/2))
    return df

def cogSalepercent(df, new_col_name, *column_name):
    df = df.withColumn(new_col_name,(((f.col(column_name[0]) - f.col(column_name[1])) * f.col(column_name[2])) /f.col(column_name[2])))
    return df