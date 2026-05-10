# quantilica-core 📦

**Foundation utilities for Quantilica data projects.**

`quantilica-core` provides a domain-neutral foundation for data clients, ETL pipelines, and metadata services. It centralizes essential infrastructure like resilient networking, atomic storage, and data provenance, allowing specific projects to focus exclusively on their data domains.

---

## ✨ Key Features

-   **Resilient Networking**: Sync and Async HTTP clients with built-in exponential backoff and jitter.
-   **Data Provenance**: Automatic generation of SHA-256 manifests for every downloaded artifact.
-   **Atomic Storage**: Local filesystem abstraction with object-style keys and atomic write guarantees.
-   **Unified Metadata**: Standard models for Sources, Datasets, Variables, and Dimensions to enable global indexing.
-   **Structured Logging**: Context-aware logging with built-in step tracking and performance timing.

---

## 🚀 Installation

`quantilica-core` is published from this GitHub repository (not on PyPI). Add
it to your project as a git dependency:

```bash
uv add "quantilica-core @ git+https://github.com/Quantilica/quantilica-core.git"
```

Or with pip:

```bash
pip install "quantilica-core @ git+https://github.com/Quantilica/quantilica-core.git"
```

---

## 🛠️ Core Modules

-   **`http`**: `HttpClient` and `AsyncHttpClient` based on `httpx`.
-   **`retry`**: Advanced retry logic for network and transient failures.
-   **`storage`**: `LocalStorage` for managing raw and processed data artifacts.
-   **`manifests`**: `DownloadManifest` and `ExecutionManifest` for full traceability.
-   **`metadata`**: `MetadataCatalog` models for interoperability between datasets.
-   **`logging`**: Structured logging via `get_logger` and `log_step`.
-   **`exceptions`**: Standard hierarchy (`FetchError`, `ParseError`, `StorageError`).

---

## 💡 Design Principles

1.  **Domain Neutrality**: The core never knows about specific data sources (IBGE, DATASUS, etc.).
2.  **Lightweight**: Minimal core dependencies; heavy integrations are optional extras.
3.  **Stability**: High test coverage for all infrastructure components.
4.  **Developer Experience**: Clean, type-hinted APIs and consistent error handling.

---

## 📖 Usage Examples

### Resilient Async Fetching
```python
import asyncio
from quantilica_core.http import AsyncHttpClient

async def main():
    client = AsyncHttpClient(attempts=3)
    data = await client.get_json("https://api.example.com/data")
    print(data)

asyncio.run(main())
```

### Storage with Provenance
```python
from quantilica_core.storage import LocalStorage
from quantilica_core.manifests import DownloadManifest

storage = LocalStorage("data/raw")
# Atomic write
stat = storage.write_bytes("source/file.csv", b"content...")

# Record provenance
manifest = DownloadManifest.from_file(
    source_id="my-source",
    dataset_id="my-dataset",
    url="https://source.com/file.csv",
    file_path=stat.path,
    producer="my-fetcher"
)
manifest.write_json(stat.path.with_suffix(".manifest.json"))
```

### Metadata Cataloging
```python
from quantilica_core.metadata import MetadataCatalog, Source, Dataset

catalog = MetadataCatalog(
    sources=[Source(id="ibge", name="IBGE")],
    datasets=[Dataset(id="ipca", source_id="ibge", name="IPCA")]
)
catalog.validate_references()
print(catalog.to_json())
```

---

## ⚖️ License

Copyright (c) 2026 Komesu, D.K. (Quantilica)  
Licensed under the [MIT License](LICENSE).
