import logging
from pathlib import Path


def get_logger(
    name: str,
) -> logging.Logger:
    """
    Create and configure a project logger.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    log_directory = Path("logs")
    log_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        formatter
    )

    file_handler = logging.FileHandler(
        log_directory / "pipeline.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(
        formatter
    )

    logger.addHandler(
        console_handler
    )

    logger.addHandler(
        file_handler
    )

    return logger