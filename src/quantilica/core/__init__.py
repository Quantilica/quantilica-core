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
from .files import decompress_archive, extracted_archive
from .ftp import parse_ftp_list_line
from .http import (
    AsyncHttpClient,
    HostPolicy,
    HttpClient,
    MaintenancePageError,
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
    trusted_sha256,
    write_manifest_sidecar,
)
from .odata import (
    ODataPaginationError,
    http_odata_fetcher,
    iter_odata_key_pages,
    iter_odata_rows,
)
from .ratelimit import CircuitOpenError
from .regeneration import (
    RegenerationResult,
    RegenerationRule,
    detect_regeneration,
    normalized_content_sha256,
    with_content_hash,
)
from .storage import (
    BaseDataRepository,
    ExpectedFormat,
    expected_format_for_path,
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
    "CircuitOpenError",
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
    "MaintenancePageError",
    "ODataPaginationError",
    "Planned",
    "RateLimiter",
    "RegenerationResult",
    "RegenerationRule",
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
    "detect_regeneration",
    "extracted_archive",
    "encode_partition_value",
    "expected_format_for_path",
    "file_key",
    "http_odata_fetcher",
    "format_partition_token",
    "is_manifest_valid",
    "iter_odata_key_pages",
    "iter_odata_rows",
    "lake_root",
    "load_lake_converter",
    "materialize_view",
    "normalized_content_sha256",
    "parse_ftp_list_line",
    "parse_partition_path",
    "partition_dir",
    "partition_types_for",
    "pick_resources",
    "read_manifest_sidecar",
    "remote_zip_members",
    "select_current",
    "should_skip",
    "trusted_sha256",
    "validate_payload_signature",
    "with_content_hash",
    "write_manifest_sidecar",
]
