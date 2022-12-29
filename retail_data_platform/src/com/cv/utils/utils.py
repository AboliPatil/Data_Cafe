import os


def get_config_path():
    cwd = os.getcwd()
    nvd = os.path.abspath(os.path.join(cwd, os.pardir))
    config_path = os.path.abspath(os.path.join(nvd, "resources", "config.ini"))
    return config_path
