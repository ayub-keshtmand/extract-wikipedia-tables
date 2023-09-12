"""DuckDB UDF functions using the Python Function API.

Encountering a problem where all registered functions appear to exhibit the behavior
of the first function, despite having distinct names.
"""

import re

import duckdb.typing as T
from src.utils.common import setup_logging
from src.utils.sql import SQLUtils

setup_logging()


def extract_fight_record(record_str):
    """
    Extracts the number of wins, losses, draws, and no contests
    from a fight record string.

    Args:
        record_str (str): The fight record string formatted as "W–L–D (NC)".
                          W, L, D, and NC are all integers.
                          D and NC are optional.

    Returns:
        tuple: A tuple containing four integers (wins, losses, draws, no_contests).

    Examples:
        >>> extract_fight_record("28–11–1 (1 NC)")
        (28, 11, 1, 1)

        >>> extract_fight_record("24–4")
        (24, 4, 0, 0)

        >>> extract_fight_record("19–9 (2 NC)")
        (19, 9, 0, 2)

        >>> extract_fight_record("10–0–1")
        (10, 0, 1, 0)

        >>> extract_fight_record("5–2–3")
        (5, 2, 3, 0)
    """
    # Initialize default values
    wins, losses, draws, no_contests = 0, 0, 0, 0

    # Extracting wins, losses, and draws (if any)
    main_parts = record_str.split(" (")[0]
    elements = main_parts.split("–")
    if len(elements) >= 2:
        wins = int(elements[0])
        losses = int(elements[1])
    if len(elements) == 3:
        draws = int(elements[2])

    # Extracting no contests (if any)
    nc_match = re.search(r"\((\d+) NC\)", record_str)
    if nc_match:
        no_contests = int(nc_match.group(1))

    return (wins, losses, draws, no_contests)


def extract_wins_from_record(record_str: str):
    return extract_fight_record(record_str)[0]


def extract_losses_from_record(record_str: str):
    return extract_fight_record(record_str)[1]


def extract_draws_from_record(record_str: str):
    return extract_fight_record(record_str)[2]


def extract_nc_from_record(record_str: str):
    return extract_fight_record(record_str)[3]


def register_functions(duckdb_util: SQLUtils):
    duckdb_util.create_function(
        "extract_wins_from_record", extract_wins_from_record, [T.VARCHAR], T.SMALLINT
    )
    duckdb_util.create_function(
        "extract_losses_from_record",
        extract_losses_from_record,
        [T.VARCHAR],
        T.SMALLINT,
    )
    duckdb_util.create_function(
        "extract_draws_from_record", extract_draws_from_record, [T.VARCHAR], T.SMALLINT
    )
    duckdb_util.create_function(
        "extract_nc_from_record", extract_nc_from_record, [T.VARCHAR], T.SMALLINT
    )
