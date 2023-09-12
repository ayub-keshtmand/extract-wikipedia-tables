import logging


def setup_logging(log_level=logging.INFO):
    """
    Set up basic logging configuration for the application.

    Parameters:
    - log_level: Desired logging level (e.g., logging.DEBUG, logging.INFO)
    """
    logging.basicConfig(
        level=log_level, format="%(asctime)s | %(levelname)s | %(message)s"
    )
