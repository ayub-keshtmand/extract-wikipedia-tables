import datetime
import os
from glob import glob

import yaml
from src.utils.logger import setup_logging

setup_logging()


def read_yaml_to_dict(filename):
    """
    Reads a YAML file and returns its contents as a dictionary.

    Parameters:
    - filename (str): Path to the YAML file

    Returns:
    - dict: Contents of the YAML file

    Example usage:
    >>> yaml_path = 'path_to_yaml_file.yaml'
    >>> data_dict = read_yaml_to_dict(yaml_path)
    >>> print(data_dict)
    """
    with open(filename, "r") as file:
        data = yaml.safe_load(file)
    return data


def get_date_parts():
    today = datetime.datetime.today()
    return (
        str(today.year),
        str(today.month),
        str(today.day),
    )


def get_sql_file_paths(
    sql_path: str,
    namespace: str,
    pipeline_stage_name: str,
):
    """
    Parameters:
        sql_path (str): Directory path for SQL queries.
        namespace (str): Namespace.
        pipeline_stage_name (str): Zone name e.g. "extract", "clean", "transformed".

    Returns:
        list: List of SQL file paths.
    """
    path = [i for i in [sql_path, namespace, pipeline_stage_name] if i is not None]
    return glob(os.path.join(*path, "*.sql"))


def get_file_path_basename(
    file_path: str,
    exclude_extension: bool = True,
):
    basename = os.path.basename(file_path)
    if exclude_extension:
        return os.path.splitext(basename)[0]
    else:
        return basename
