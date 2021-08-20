__all__ = ["__version__", "AthenaQuery"]

from importlib import metadata

from .athena import AthenaQuery

__version__ = metadata.version("montoux_athena")
