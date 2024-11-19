"""Error handling for CSB operations"""

from contextlib import contextmanager
from typing import Generator, TYPE_CHECKING
import arcpy
from logging import Logger

if TYPE_CHECKING:
    from logging import Logger


class CSBError(Exception):
    """Base exception for CSB operations"""

    pass


class ProcessingError(CSBError):
    """Error during CSB processing"""

    pass


class ConfigurationError(CSBError):
    """Configuration error"""

    pass


@contextmanager
def handle_arcpy_errors(logger: Logger, operation: str) -> Generator[None, None, None]:
    """Context manager for handling ArcPy errors"""
    try:
        yield
    except arcpy.ExecuteError as e:
        logger.error(f"ArcPy error during {operation}: {arcpy.GetMessages(2)}")
        raise ProcessingError(f"ArcPy operation failed: {operation}") from e
    except Exception as e:
        logger.error(f"Unexpected error during {operation}: {e}")
        raise ProcessingError(f"Operation failed: {operation}") from e
