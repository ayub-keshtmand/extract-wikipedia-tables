"""SQL Utilities module. Not needed because SQL executed in dbt-duckdb."""

import logging
import os
from typing import Any, Dict, Optional

import duckdb
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from src.settings import (
    aws_access_key_id,
    aws_secret_access_key,
    pg_database,
    pg_host,
    pg_password,
    pg_port,
    pg_username,
    sql_path,
    use_ssl,
)
from src.utils.logger import setup_logging

setup_logging()


class SQLUtils:
    def __init__(self, connection: Any):
        """Initialise SQLUtils instance.

        Args:
            connection (Any): Any connection type.
        """
        self.connection = connection

    @classmethod
    def from_postgresql_connection(cls):
        """Create SQLUtils instance from PostgreSQL connection."""
        return cls(get_postgresql_connection())

    @classmethod
    def from_duckdb_connection(cls):
        """Create SQLUtils instance from DuckDB connection."""
        return cls(get_duckdb_connection())

    def run_query(
        self,
        sql_string: str = "",
        sql_file_name: str = "",
        sql_file_path: str = "",
        replace_dict: Optional[Dict[str, str]] = None,
    ):
        """Run SQL query.

        The SQL query can be provided as a string, the name of a file within the /sql
        directory of this project or the path to a file containing a SQl query.

        Parameters:
            sql_string (str): SQL query string.
            sql_file_name (str): The name of a file within the
                /sql directory of this project.
            sql_file_path (str): The path of a file containing a SQL query.
            replace_dict (Dict[str, str]):
                Dictionary of string replacements to make on
                sql_string or the query string found in the file provided.

        Returns:
            duckdb.DuckDBPyRelation: Query result.
        """
        # Check for valid input
        if not (bool(sql_string) ^ bool(sql_file_name) ^ bool(sql_file_path)):
            raise ValueError(
                "Only one of sql_string, sql_file_name or sql_file_path "
                "should be provided."
            )
        else:
            # Get file path if file name was provided
            if sql_file_name:
                sql_file_path = self.get_file_path(sql_file_name)

            # Read query from file file name or file path was provided
            if sql_file_path:
                sql_string = self.read_query_string_from_file(sql_file_path)

            # Replace values in query template if replace dictionary was provided
            if replace_dict is not None:
                # Iterate over the replacement dictionary
                for string, replacement_string in replace_dict.items():
                    sql_string = sql_string.replace(string, replacement_string)

            # Run the query
            result = self.run_query_string(sql_string)

            return result

    def read_query_string_from_file(
        self,
        file_path: str,
    ) -> str:
        """Read a SQL file to a string.

        Parameters:
            file_path (str): The path to the file containing the query string.

        Returns:
           str: Query string from the file.
        """
        logging.info(f"Reading query string from: {file_path}")
        return open(file_path).read()

    def run_query_string(self, sql_string: str) -> duckdb.DuckDBPyRelation:
        logging.debug(f"Running DuckDB SQL query:\n\n{sql_string}\n")
        return duckdb.sql(sql_string)

    @staticmethod
    def get_file_path(
        file_name: str,
        folder: str = sql_path,
    ) -> str:
        """Get the full file path of files within a sub-folder of this project.

        Parameters:
            file_name (str): The name of a file within this project.
            folder (str): The name of the folder containing file_name.

        Returns:
            str: The full path of the file.
        """
        # Get the execution directory of the script
        script_dir = os.path.dirname(os.path.realpath("__file__"))
        return os.path.join(script_dir, folder, file_name)

    def create_function(self, name, function, argument_type_list, return_type):
        duckdb.create_function(name, function, argument_type_list, return_type)

    def load_dataframe_to_postgres(
        self,
        df: pd.DataFrame,
        postgres_table: str,
        postgres_schema: str = "public",
        if_exists: str = "replace",
        dtype: dict = None,
    ):
        num_rows, num_cols = df.shape
        logging.info(
            f"Loading dataframe with {num_rows} rows and {num_cols} columns "
            f"to PostgreSQL {postgres_schema}.{postgres_table}"
        )
        df.to_sql(
            name=postgres_table,
            con=self.connection,
            schema=postgres_schema,
            if_exists=if_exists,
            index=False,
            dtype=dtype,
        )


def get_postgresql_connection(connection_type: str = "sqlalchemy"):
    if connection_type == "psycopg2":
        logging.info(
            f"Creating PostgreSQL psycopg2 connection object "
            f"on {pg_host} port {pg_port}"
        )
        return psycopg2.connect(
            dbname=pg_database,
            user=pg_username,
            password=pg_password,
            host=pg_host,
            port=pg_port,
        )
    elif connection_type == "sqlalchemy":
        logging.info(
            f"Creating PostgreSQL SQLAlchemy engine on {pg_host} port {pg_port}"
        )
        connection_string = (
            f"postgresql+psycopg2://"
            f"{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_database}"
        )

        return create_engine(connection_string)


def get_duckdb_connection():
    try:
        duckdb.sql(
            f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_access_key_id='{aws_access_key_id}';
            SET s3_secret_access_key='{aws_secret_access_key}';
            SET s3_use_ssl='{use_ssl}';"""
        )
    except Exception as e:
        logging.error(e)
