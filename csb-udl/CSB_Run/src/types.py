"""Common type definitions"""

from typing import TypeAlias, NewType
from pathlib import Path

GDBPath: TypeAlias = Path
FeatureClass: TypeAlias = str
Year = NewType("Year", int)
Area = NewType("Area", str)


class CSBError(Exception):
    """Base exception for CSB operations"""

    pass


class ProcessingError(CSBError):
    """Error during CSB processing"""

    pass
