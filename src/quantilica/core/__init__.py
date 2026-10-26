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
from .lake import (
    DatasetDescriptor,
    dataset_lock,
    encode_partition_value,
    file_key,
    lake_root,
    load_lake_converter,
    parse_partition_path,
    partition_dir,
)
from .manifests import (
    DatasetManifest,
    DownloadManifest,
    ExecutionManifest,
    RunManifest,
    read_manifest_sidecar,
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
    "DatasetDescriptor",
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
    "dataset_lock",
    "decompress_archive",
    "encode_partition_value",
    "file_key",
    "is_manifest_valid",
    "lake_root",
    "load_lake_converter",
    "parse_ftp_list_line",
    "parse_partition_path",
    "partition_dir",
    "read_manifest_sidecar",
    "should_skip",
    "write_manifest_sidecar",
]
