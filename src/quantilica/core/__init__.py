"""Common foundation utilities for Quantilica data projects."""

from importlib.metadata import PackageNotFoundError, version

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
    "DatasetManifest",
    "DownloadManifest",
    "ExecutionManifest",
    "FtpFreshnessProbe",
    "FreshnessProbe",
    "HttpFreshnessProbe",
    "IncrementalSyncStrategy",
    "RemoteStat",
    "RunManifest",
    "__version__",
    "is_manifest_valid",
    "should_skip",
    "write_manifest_sidecar",
]
