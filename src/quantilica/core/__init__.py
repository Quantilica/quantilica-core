"""Common foundation utilities for Quantilica data projects."""

from importlib.metadata import PackageNotFoundError, version

from .files import decompress_archive
from .ftp import parse_ftp_list_line
from .http import (
    AsyncHttpClient,
    HttpClient,
    RateLimiter,
    RetryableHttpStatusError,
)
from .manifests import (
    DatasetManifest,
    DownloadManifest,
    ExecutionManifest,
    RunManifest,
    write_manifest_sidecar,
)
from .sync import (
    FreshnessProbe,
    FtpFreshnessProbe,
    HttpFreshnessProbe,
    IncrementalSyncStrategy,
    RemoteStat,
    is_manifest_valid,
    should_skip,
)

try:
    __version__ = version("quantilica-core")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "AsyncHttpClient",
    "DatasetManifest",
    "DownloadManifest",
    "ExecutionManifest",
    "FreshnessProbe",
    "FtpFreshnessProbe",
    "HttpClient",
    "HttpFreshnessProbe",
    "IncrementalSyncStrategy",
    "RateLimiter",
    "RemoteStat",
    "RetryableHttpStatusError",
    "RunManifest",
    "__version__",
    "decompress_archive",
    "is_manifest_valid",
    "parse_ftp_list_line",
    "should_skip",
    "write_manifest_sidecar",
]
