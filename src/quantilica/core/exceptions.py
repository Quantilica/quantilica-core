"""Exception hierarchy shared by Quantilica projects."""


class QuantilicaError(Exception):
    """Base class for all domain-neutral Quantilica errors."""


class ConfigError(QuantilicaError):
    """Raised when configuration is missing or invalid."""


class FetchError(QuantilicaError):
    """Raised when remote data cannot be fetched."""


class CorruptPayloadError(FetchError):
    """Raised when a downloaded file is not the data it claims to be.

    An HTML error page saved as CSV, a ZIP with a bad CRC, a 7z that fails its
    test: the download completed, but registering it would archive broken data.
    """


class ParseError(QuantilicaError):
    """Raised when input data cannot be parsed."""


class StorageError(QuantilicaError):
    """Raised when object or file storage operations fail."""


class MetadataError(QuantilicaError):
    """Raised when generic metadata is invalid or incomplete."""


class ValidationError(QuantilicaError):
    """Raised when a value fails validation."""
