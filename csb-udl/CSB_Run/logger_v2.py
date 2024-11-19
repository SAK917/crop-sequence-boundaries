"""Logging configuration for CSB processing"""

import logging
from pathlib import Path
from typing import Optional

CONSOLE_FORMAT = "%(message)s"
FILE_FORMAT = "%(levelname)-8s %(asctime)s - %(message)s"
DATE_FORMAT = "%Y%m%d %H:%M:%S"


class CSBLogger:
    """Logger for CSB processing"""

    def __init__(self, area: str, log_dir: Path, console_level: int = logging.INFO, file_level: int = logging.DEBUG):
        """Initialize logger

        Args:
            area: Area identifier
            log_dir: Directory for log files
            console_level: Logging level for console output
            file_level: Logging level for file output
        """
        self.logger = logging.getLogger(area)
        self.logger.setLevel(logging.DEBUG)

        # Console handler
        console = logging.StreamHandler()
        console.setLevel(console_level)
        console.setFormatter(logging.Formatter(CONSOLE_FORMAT))
        self.logger.addHandler(console)

        # File handler
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / f"{area}.log")
        file_handler.setLevel(file_level)
        file_handler.setFormatter(logging.Formatter(FILE_FORMAT, datefmt=DATE_FORMAT))
        self.logger.addHandler(file_handler)

    def get_logger(self) -> logging.Logger:
        """Get the configured logger"""
        return self.logger


def initialize_logger(creation_dir: str, area: str) -> logging.Logger:
    """Initialize logger for CSB processing

    Args:
        creation_dir: Creation directory path
        area: Area identifier

    Returns:
        Configured logger instance
    """
    log_dir = Path(creation_dir) / "log"
    csb_logger = CSBLogger(area, log_dir)
    return csb_logger.get_logger()
