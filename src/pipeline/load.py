"""Loads transformed datasets to PostgreSQL."""

import pandas as pd
from src.utils.logger import setup_logging

setup_logging()


def run_loader(
    data_sources: dict,
    s3_client,
    s3_uri,
    s3_bucket,
    date_parts,
    read_from_pipeline_stage: str,
):
    for namespace in data_sources.keys():
        df = pd.read_csv(f"{s3_uri}{s3_bucket}/{namespace}/{date_parts}/")  # noqa
