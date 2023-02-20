import mysql.connector as msql
import pandas as pd

from sqlalchemy import create_engine


def get_cursor(default_config):
    conn = msql.connect(host=default_config["host"], user=default_config["username"],
                        password=default_config["password"])  # give ur username, password
    return conn


def create_database_if_not_exists(conn, default_config):
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS " + default_config['db_name'])
    cursor.execute("Use " + default_config['db_name'])


def create_tables_if_not_exists(conn, default_config):
    execute = get_query_list(default_config['create_table_file'])
    cursor = conn.cursor()
    for query in execute:
        print(query)
        cursor.execute(query)


def drop_tables(conn, drop_config):
    execute = get_query_list(drop_config['drop_table_path'])
    cursor = conn.cursor()
    for query in execute:
        print(query)
        cursor.execute(query)


def get_query_list(path):
    with open(path) as f:
        execute = []
        prevline = ""
        for line in f:
            if line.endswith(";\n"):
                prevline = prevline + line.lstrip('\t').rstrip('\n')
                print(prevline)
                execute.append(prevline)
                prevline = ""
            elif not line.endswith(";\n"):
                prevline = prevline + line.lstrip('\t').rstrip('\n')
    return execute


def insert_data(default_config, insert_config, csv_files_list):
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                           .format(host=default_config["host"], db=default_config["db_name"],
                                   user=default_config["username"], pw=default_config["password"]))
    for file_name in csv_files_list:
        print(insert_config["csv_files_directory"] + "\\" + file_name)
        df = pd.read_csv(insert_config["csv_files_directory"] + "\\" + file_name)
        print(file_name.split(".")[0])
        df.to_sql(file_name.split(".")[0].lower(), engine, index=False, if_exists='append', chunksize=1000)
