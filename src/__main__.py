"""Main ETL pipeline module.

This workflow focuses primarily on ingesting raw data to S3.
Further cleaning and transformations will be applied in dbt.
"""

from src.pipeline import extract

# from src.pipeline import clean, extract, load, transform
from src.settings import (
    aws_access_key_id,
    aws_secret_access_key,
    data_sources,
    date_parts,
    endpoint_url,
    s3_bucket,
    s3_uri,
    sql_path,
    use_ssl,
)
from src.utils.functions import register_functions
from src.utils.s3 import get_s3_client
from src.utils.sql import SQLUtils

s3_client = get_s3_client(
    use_ssl,
    endpoint_url,
    aws_access_key_id,
    aws_secret_access_key,
)
duckdb_util = SQLUtils.from_duckdb_connection()
register_functions(duckdb_util)

kwargs = {
    "data_sources": data_sources,
    "s3_client": s3_client,
    "s3_bucket": s3_bucket,
    "sql_path": sql_path,
    "duckdb_util": duckdb_util,
    "replace_dict": {
        "<s3_uri>": s3_uri,
        "<date_parts>": date_parts,
    },
}

extract.run_extract(data_sources, s3_client, s3_bucket)
# clean.run_cleaner(**kwargs)
# transform.run_transformations(**kwargs)
# load.run_loader()
