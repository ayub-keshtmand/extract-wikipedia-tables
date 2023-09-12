import logging

import boto3
import botocore.client
import pandas as pd
from src.utils.common import get_date_parts
from src.utils.logger import setup_logging

setup_logging()


def get_s3_client(
    use_ssl,
    endpoint_url,
    aws_access_key_id,
    aws_secret_access_key,
):
    return boto3.client(
        service_name="s3",
        use_ssl=use_ssl,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


def construct_s3_key(
    namespace: str,
    pipeline_stage_name: str,
    file_name: str,
    extension: str = ".parquet",
):
    """
    Construct an S3 key string.

    Parameters:
    - namespace (str): Name of the namespace.
    - pipeline_stage_name (str): Zone name e.g. "extract", "clean", "transformed".
    - file_name (str): Name of the file.
    - extension (str): File type extension (default: ".parquet").

    Returns
    - str
    """
    date_parts = get_date_parts()
    return "/".join(
        [namespace, pipeline_stage_name, *date_parts, file_name + extension]
    )


def upload_dataframe_to_s3(
    s3_client: botocore.client.BaseClient,
    df: pd.DataFrame,
    s3_bucket: str,
    s3_key: str,
    extension: str = "parquet",
):
    # Do not upload empty DataFrames
    if df.empty:
        logging.info(f"Empty DataFrame - Did not upload to S3: {s3_bucket}")
        return

    num_rows, num_cols = df.shape
    logging.info(
        f"Writing dataframe to bucket {s3_bucket}/{s3_key} "
        f"with {num_rows} rows and {num_cols} columns."
    )
    if extension == "csv":
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=df.to_csv(index=False))
    elif extension == "parquet":
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=df.to_parquet(index=False, engine="pyarrow"),
        )
    else:
        raise ValueError(f"Extension format {extension} not supported.")


def list_objects_in_s3_bucket(
    s3_client: botocore.client.BaseClient,
    s3_bucket: str,
    s3_key_prefix: str,
):
    objects = []
    result = s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_key_prefix)
    if "Contents" in result:
        for content in result["Contents"]:
            key = content["Key"]
            objects.append(key)
    return objects
