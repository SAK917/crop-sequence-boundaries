"""
This module contains the logger for the CSB processing.
Each CSB subunit process has its own logger that writes to both the console and a log file.
By default:
    - the log file is stored in the log directory of the CSB subunit
    - the file log level is set to DEBUG providing detailed step-by-step information
    - the console log level is set to INFO providing high-level information
"""

import logging

CONSOLE_LOG_FORMAT = "%(message)s"
FILE_LOG_FORMAT = "%(levelname)-8s %(asctime)s - %(message)s"


def initialize_logger(creation_dir: str, area: str) -> logging.Logger:
    """Initialize the logger for CSB processing"""
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create a handler for the console output with level INFO
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(CONSOLE_LOG_FORMAT))
    logger.addHandler(console_handler)

    # Create a handler for the file output with level DEBUG
    file_handler = logging.FileHandler(f"{creation_dir}/log/{area}.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(FILE_LOG_FORMAT, datefmt="%Y%m%d %H:%M:%S"))
    logger.addHandler(file_handler)

    return logger
