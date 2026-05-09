# quantilica-core

Foundation utilities for Quantilica data projects.

This package contains domain-neutral building blocks shared by data clients,
pipeline engines, metadata services, and platform applications. It should not
contain source-specific logic for SIDRA, DATASUS, BCB, Tesouro Direto, or any
other provider.

## Current scope

Phase 1 provides:

- Common exception hierarchy
- Logging helpers with optional execution context
- UTC datetime helpers
- File/path helpers, checksums, and atomic writes
- Minimal environment/settings helpers

Phase 2 adds:

- Synchronous HTTP client based on `httpx`
- Retry helpers with exponential backoff
- Deterministic file cache for URL-based downloads

Phase 3 adds:

- Filesystem-backed `LocalStorage` with object-style keys
- Object metadata with size, SHA-256, and modification timestamp
- Download, dataset, and run manifests for provenance

Phase 4 adds:

- Generic metadata models for sources, datasets, resources, variables, dimensions,
  series, periods, territories, and ingestion runs
- Lightweight metadata catalog with reference validation

Phase 5 adds:

- `CatalogRepository` to publish and load platform catalog artifacts
- `CatalogIndex` for basic in-memory lookup and search
- Standard storage layout for ingestion runs and download manifests

## Design rules

- Keep dependencies light.
- Keep domain-specific logic out of this package.
- Prefer small, testable helpers over framework-style abstractions.
- Add heavier integrations as optional extras only when needed.

## Example

```python
from quantilica_core.files import sha256_file, write_bytes_atomic
from quantilica_core.logging import get_logger, log_step

logger = get_logger(__name__)

with log_step(logger, "write-resource", source="example", dataset="sample"):
    path = write_bytes_atomic("data/sample.bin", b"content")
    digest = sha256_file(path)
```

```python
from quantilica_core.cache import FileCache
from quantilica_core.http import HttpClient

client = HttpClient(attempts=3)
cache = FileCache(".cache/quantilica")

url = "https://example.com/data.json"
key = cache.key_for_url(url)

content = cache.get_bytes(key)
if content is None:
    content = client.get_bytes(url)
    cache.write_bytes(key, content, metadata={"url": url})
```

```python
from quantilica_core.manifests import DownloadManifest
from quantilica_core.storage import LocalStorage

storage = LocalStorage("data/raw")
stat = storage.write_bytes("example/data.json", b'{"ok": true}')

manifest = DownloadManifest.from_file(
    source_id="example",
    dataset_id="sample",
    url="https://example.com/data.json",
    file_path=stat.path,
    path=stat.key,
    producer="example-client",
    producer_version="0.1.0",
)
manifest.write_json("data/raw/example/data.manifest.json")
```

```python
from quantilica_core.metadata import Dataset, MetadataCatalog, Resource, Source

catalog = MetadataCatalog(
    sources=[Source(id="ibge", name="IBGE")],
    datasets=[Dataset(id="sidra-ipca", source_id="ibge", name="IPCA")],
    resources=[
        Resource(
            id="7060",
            dataset_id="sidra-ipca",
            name="Tabela SIDRA 7060",
            format="json",
        )
    ],
)
catalog.validate_references()
catalog.write_json("catalog.json")
```

```python
from quantilica_core.platform import CatalogRepository
from quantilica_core.storage import LocalStorage

repository = CatalogRepository(LocalStorage("platform"))
repository.publish_catalog(catalog)

index = repository.load_index()
datasets = index.search_datasets("ipca")
```
