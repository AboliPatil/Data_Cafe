import json
from pyspark.sql import functions as f

def aggregation_processor(df, table_config):
    if table_config.get("aggregation", None):
        aggregations = table_config["aggregation"]
        aggregation_json = json.loads(aggregations)
        groupby = aggregation_json["groupby_columns"].split(",")
        aggregation_dict = aggregation_json["aggregation_dict"]
        aggregation_list = aggregate_function_list_creator(aggregation_dict)
        df.show(20, False)
        df = df.groupBy(*groupby).agg(*aggregation_list)
        return df
    return df


def aggregate_function_list_creator(aggregation_dict):
    aggregation_list = []
    for new_column_name, aggregation in aggregation_dict.items():
        aggregations = aggregation.split(",")
        aggregation_type = aggregations[0]
        column_name = aggregations[1]
        if aggregation_type.lower() == "count":
            aggregation_list.append(f.count(f.col(column_name)).alias(new_column_name))
        if aggregation_type.lower() == "countdistinct":
            aggregation_list.append(f.countDistinct(f.col(column_name)).alias(new_column_name))
        elif aggregation_type.lower() == "min":
            aggregation_list.append(f.min(f.col(column_name)).alias(new_column_name))
        elif aggregation_type.lower() == "max":
            aggregation_list.append(f.max(f.col(column_name)).alias(new_column_name))
        elif aggregation_type.lower() == "avg" or aggregation_type.lower() == "average":
            aggregation_list.append(f.avg(f.col(column_name)).alias(new_column_name))
        elif aggregation_type.lower() == "sum":
            aggregation_list.append(f.sum(f.col(column_name)).alias(new_column_name))
        elif aggregation_type.lower() == "sum":
            aggregation_list.append(f.sumDistinct(f.col(column_name)).alias(new_column_name))
    print(aggregation_list)
    return aggregation_list