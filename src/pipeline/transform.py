"""Run SQL transformations queries in the clean directories.
This module is covered in dbt.
"""

import logging

from src.utils.common import get_file_path_basename, get_sql_file_paths
from src.utils.logger import setup_logging
from src.utils.s3 import construct_s3_key, upload_dataframe_to_s3

setup_logging()


def run_transformations(
    data_sources: dict,
    s3_client,
    s3_bucket,
    sql_path,
    duckdb_util,
    replace_dict,
):
    logging.info("Running transformations")

    pipeline_stage_name = "transform"

    for namespace in data_sources.keys():
        # Get SQL queries for pipeline stage
        sql_file_paths = get_sql_file_paths(sql_path, namespace, pipeline_stage_name)

        # Run SQL queries
        for sql_file_path in sql_file_paths:
            df = duckdb_util.run_query(
                sql_file_path=sql_file_path, replace_dict=replace_dict
            ).to_df()
            s3_key = construct_s3_key(
                namespace,
                pipeline_stage_name,
                file_name=get_file_path_basename(sql_file_path),
            )
            upload_dataframe_to_s3(
                s3_client,
                df,
                s3_bucket,
                s3_key,
            )
