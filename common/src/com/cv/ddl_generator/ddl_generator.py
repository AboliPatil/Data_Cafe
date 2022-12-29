import configparser
import json
import os


def read_json(file_path):
    f = open(file_path, "r")
    json_data = json.loads(f.read())
    return json_data


def get_insertion_files_list(json_dir_path):
    json_dir_list = os.listdir(json_dir_path)
    return json_dir_list


def get_primary_query(primary_key):
    primary_query = "PRIMARY KEY ("
    for key in primary_key:
        primary_query = primary_query + key + ","
    primary_query = primary_query[:-1] + ")"
    print(primary_query)
    return primary_query


def get_foreign_query(table_name, col):
    query = ""
    foreign_key_col = col.get("foreign_key", None)
    parent_table = col.get("table_name", None)
    if foreign_key_col is not None:
        query = "FOREIGN KEY(" + foreign_key_col + ") REFERENCES " + parent_table + "(" + foreign_key_col + ")"
    return query


def create_query(table_name, column_partial_query, foreign_key_query, primary_key_query):
    query = "CREATE TABLE IF NOT EXISTS " + table_name + "("
    for partial_query in column_partial_query:
        query = query + partial_query + ","
    if primary_key_query:
        query = query + primary_key_query + ","
    for foreign_key in foreign_key_query:
        query = query + foreign_key + ","
    return query


def create_table(table_name, columns_list, primary_key):
    column_partial_query = []
    foreign_key_query = []
    primary_key_query = ""
    if primary_key is not None:
        primary_key_query = get_primary_query(primary_key)
    for col in columns_list:
        col_partial = col.get("name") + " " + col.get("db_data_type")
        if col.get("constraint", "") != "":
            col_partial = col_partial + " " + col.get("constraint", "")
        column_partial_query.append(col_partial)
        foreign_key = get_foreign_query(table_name, col)
        if foreign_key != "":
            foreign_key_query.append(foreign_key)
    query = create_query(table_name, column_partial_query, foreign_key_query, primary_key_query)
    query = query[:-1] + ");"
    return query


def write_to_file(out_file_path, query_list):
    os.makedirs(os.path.dirname(out_file_path), exist_ok=True)
    file = open(out_file_path, "w")
    file.writelines(query + '\n' for query in query_list)
    file.close()


def get_config_path():
    cwd = os.getcwd()
    nvd = os.path.abspath(os.path.join(cwd, os.pardir, os.pardir, os.pardir, os.pardir))
    config_path = os.path.abspath(os.path.join(nvd, "resources/ddl_generator_config", "config.ini"))
    return config_path


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(get_config_path())
    default_config = config['DEFAULT']
    json_dir_path = default_config['json_dir_path']
    json_file_list = get_insertion_files_list(json_dir_path)
    query_list = []
    for file in json_file_list:
        data_conf = read_json(json_dir_path + "/" + file)
        table_properties = data_conf.get('TableProperties')
        columns_list = table_properties.get('columns_list', 0)
        primary_key = table_properties.get('primary_key', None)
        query = create_table(file.split(".")[0], columns_list, primary_key)
        query_list.append(query)
    write_to_file(default_config['out_file_path'], query_list)
