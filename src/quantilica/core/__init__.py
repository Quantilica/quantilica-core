"""Common foundation utilities for Quantilica data projects."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("quantilica-core")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = ["__version__"]
