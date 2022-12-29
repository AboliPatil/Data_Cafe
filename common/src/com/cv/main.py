import configparser
import os

from src.com.cv.utils import mysql_utils


def get_insertion_files_list(insert_config):
    csv_files_directory = insert_config['csv_files_directory']
    exclude = insert_config.get('exclude_files', None)
    exclude_list = exclude.split(",") if exclude is not None else None
    print(exclude_list)
    csv_files_list = os.listdir(csv_files_directory)
    print(csv_files_list)
    final_list = list(set(csv_files_list) - set(exclude_list))
    for file in final_list:
        if not file.endswith(".csv"):
            final_list.remove(file)
    return final_list


def main(default_config, drop_config, insert_config):
    conn = mysql_utils.get_cursor(default_config)
    mysql_utils.create_database_if_not_exists(conn, default_config)
    mysql_utils.drop_tables(conn, drop_config)
    mysql_utils.create_tables_if_not_exists(conn, default_config)

    csv_files_list = get_insertion_files_list(insert_config)
    mysql_utils.insert_data(default_config, insert_config, csv_files_list)


def get_config_path():
    cwd = os.getcwd()
    nvd = os.path.abspath(os.path.join(cwd, os.pardir, os.pardir, os.pardir))
    config_path = os.path.abspath(os.path.join(nvd, "resources/data_ingestor_config", "config.ini"))
    return config_path


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(get_config_path())
    default_config = config['DEFAULT']
    drop_config = config['DROP']
    insert_config = config['INSERT']
    print(default_config["create_table_file"])
    main(default_config, drop_config, insert_config)
