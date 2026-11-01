"""Common foundation utilities for Quantilica data projects."""

from importlib.metadata import PackageNotFoundError, version

from .ckan import (
    CkanClient,
    CkanError,
    pick_resources,
)
from .exceptions import (
    EmptyPayloadError,
    InvalidPayloadSignatureError,
    TooManyRedirects,
)
from .files import decompress_archive
from .ftp import parse_ftp_list_line
from .http import (
    AsyncHttpClient,
    HostPolicy,
    HttpClient,
    RateLimiter,
    RetryableHttpStatusError,
    ZipMember,
    remote_zip_members,
)
from .lake import (
    CANONICAL_PARTITION_TYPES,
    DatasetDescriptor,
    LakeConverter,
    Planned,
    ViewSummary,
    composite_file_key,
    converter_inventory,
    dataset_lock,
    encode_partition_value,
    file_key,
    lake_root,
    load_lake_converter,
    materialize_view,
    parse_partition_path,
    partition_dir,
    partition_types_for,
    select_current,
)
from .manifests import (
    DatasetManifest,
    DownloadManifest,
    ExecutionManifest,
    RunManifest,
    read_manifest_sidecar,
    write_manifest_sidecar,
)
from .storage import (
    BaseDataRepository,
    ExpectedFormat,
    format_partition_token,
    validate_payload_signature,
)
from .sync import (
    FreshnessPolicy,
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
    "BaseDataRepository",
    "CANONICAL_PARTITION_TYPES",
    "CkanClient",
    "CkanError",
    "DatasetDescriptor",
    "DatasetManifest",
    "DownloadManifest",
    "EmptyPayloadError",
    "ExecutionManifest",
    "ExpectedFormat",
    "FreshnessPolicy",
    "FreshnessProbe",
    "FtpFreshnessProbe",
    "HostPolicy",
    "HttpClient",
    "HttpFreshnessProbe",
    "IncrementalSyncStrategy",
    "InvalidPayloadSignatureError",
    "LakeConverter",
    "Planned",
    "RateLimiter",
    "RemoteStat",
    "RetryableHttpStatusError",
    "RunManifest",
    "TooManyRedirects",
    "ViewSummary",
    "ZipMember",
    "__version__",
    "composite_file_key",
    "converter_inventory",
    "dataset_lock",
    "decompress_archive",
    "encode_partition_value",
    "file_key",
    "format_partition_token",
    "is_manifest_valid",
    "lake_root",
    "load_lake_converter",
    "materialize_view",
    "parse_ftp_list_line",
    "parse_partition_path",
    "partition_dir",
    "partition_types_for",
    "pick_resources",
    "read_manifest_sidecar",
    "remote_zip_members",
    "select_current",
    "should_skip",
    "validate_payload_signature",
    "write_manifest_sidecar",
]
