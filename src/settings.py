import logging
import os

from decouple import config
from src.utils.common import get_date_parts, read_yaml_to_dict
from src.utils.logger import setup_logging

setup_logging()

# Env variables
logging.info("Loading env variables")
use_ssl = config("USE_SSL")
aws_access_key_id = config("AWS_ACCESS_KEY_ID")
aws_secret_access_key = config("AWS_SECRET_ACCESS_KEY")
endpoint_url = config("ENDPOINT_URL")
s3_bucket = config("S3_BUCKET")
s3_uri = f"{endpoint_url}/{s3_bucket}"

# PostgreSQL
pg_host = config("HOST")
pg_username = config("USERNAME")
pg_password = config("PASSWORD")
pg_database = config("DATABASE")
pg_port = config("PORT")

# Settings
logging.info("Loading settings variables")
settings = read_yaml_to_dict("settings.yaml")

# URLs
url_base = settings["url_base"]
data_sources = settings["data_sources"]

# SQL
sql_path = os.path.join(*settings["sql_path"])

# Dates
date_parts = get_date_parts()
date_parts = os.path.join(*date_parts)
