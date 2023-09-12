"""Extracts tables from Wikipedia pages and uploads to S3."""

import logging

import pandas as pd
from src.settings import url_base
from src.utils.logger import setup_logging
from src.utils.s3 import construct_s3_key, upload_dataframe_to_s3
from typing import List

setup_logging()


def get_html_tables_from_url(
    url: str, extract_links: str = None
) -> List[pd.DataFrame]:
    """Extract HTML tables from a given URL.

    Args:
        url (str): URL from which to extract the tables.
        extract_links (optional): Determines if the links are to be extracted.
        Defaults to None.

    Returns:
        list[pd.DataFrame]: List of extracted tables as pandas dataframes.
    """
    logging.info(f"Extracting HTML tables from {url}, extract links: {extract_links}")
    dfs = pd.read_html(url, displayed_only=False, extract_links=extract_links)
    if not dfs:
        logging.info("No dataframes returned - check if HTML tables exist in url")
    else:
        logging.info(f"Returned {len(dfs)} dataframes")
    return dfs


def run_extract(
    data_sources,
    s3_client,
    s3_bucket,
):
    """Execute the extraction of data from URLs and upload to S3.

    Args:
        data_sources (dict): Dictionary containing information about data sources.
        s3_client: S3 client to upload data.
        s3_bucket (str): The name of the S3 bucket where data is to be uploaded.

    Returns:
        None
    """
    logging.info("Running extract")

    pipeline_stage_name = "extract"

    for namespace, info in data_sources.items():
        url = url_base + info["path"]
        data_sources[namespace]["dataframes"] = get_html_tables_from_url(url)

    # Upload raw extracts to S3
    for namespace, info in data_sources.items():
        tables_and_dataframes = list(zip(info["tables"], info["dataframes"]))
        for table, dataframe in tables_and_dataframes:
            s3_key = construct_s3_key(namespace, pipeline_stage_name, table)
            upload_dataframe_to_s3(s3_client, dataframe, s3_bucket, s3_key)
