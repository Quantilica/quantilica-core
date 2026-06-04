import asyncio

import httpx

from quantilica.core.fetcher import RemoteResource, download_resources
from quantilica.core.http import AsyncHttpClient
from quantilica.core.storage import StampedDataRepository


def _make_client(content: bytes = b"data") -> AsyncHttpClient:
    def handler(request):
        return httpx.Response(200, content=content)

    return AsyncHttpClient(attempts=1, transport=httpx.MockTransport(handler))


def test_download_resources_basic(tmp_path):
    client = _make_client(b"file content")
    repo = StampedDataRepository(tmp_path)
    resources = [
        RemoteResource(
            name="prices",
            url="https://example.test/prices.csv",
            filename="prices@20250101T000000.csv",
            size=0,
        )
    ]

    results = asyncio.run(
        download_resources(
            resources,
            repo,
            "test-dataset",
            client,
            source_id="test-source",
            producer="test-producer",
        )
    )

    assert len(results) == 1
    assert results[0]["filename"] == "prices@20250101T000000.csv"
    dest = repo.dataset_path("test-dataset", "prices@20250101T000000.csv")
    assert dest.exists()
    assert dest.read_bytes() == b"file content"


def test_download_resources_skips_size_match(tmp_path):
    repo = StampedDataRepository(tmp_path)
    dest_dir = repo.dataset_path("test-dataset")
    dest_dir.mkdir(parents=True)
    existing = dest_dir / "prices@20250101T000000.csv"
    existing.write_bytes(b"x" * 100)

    call_count = 0

    def handler(request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, content=b"x" * 100)

    client = AsyncHttpClient(attempts=1, transport=httpx.MockTransport(handler))
    resources = [
        RemoteResource(
            name="prices",
            url="https://example.test/prices.csv",
            filename="prices@20250201T000000.csv",
            size=100,
        )
    ]

    results = asyncio.run(
        download_resources(
            resources,
            repo,
            "test-dataset",
            client,
            source_id="test-source",
        )
    )

    assert results == []
    assert call_count == 0


def test_download_resources_multiple_concurrent(tmp_path):
    client = _make_client(b"content")
    repo = StampedDataRepository(tmp_path)
    resources = [
        RemoteResource(
            name=f"file{i}",
            url=f"https://example.test/file{i}.csv",
            filename=f"file{i}@20250101T000000.csv",
        )
        for i in range(3)
    ]

    results = asyncio.run(
        download_resources(
            resources,
            repo,
            "test-dataset",
            client,
            source_id="test-source",
            max_concurrency=2,
        )
    )

    assert len(results) == 3
    filenames = {r["filename"] for r in results}
    assert filenames == {
        "file0@20250101T000000.csv",
        "file1@20250101T000000.csv",
        "file2@20250101T000000.csv",
    }


def test_download_resources_error_returns_empty(tmp_path):
    def handler(request):
        return httpx.Response(500)

    client = AsyncHttpClient(attempts=1, transport=httpx.MockTransport(handler))
    repo = StampedDataRepository(tmp_path)
    resources = [
        RemoteResource(
            name="prices",
            url="https://example.test/prices.csv",
            filename="prices@20250101T000000.csv",
        )
    ]

    results = asyncio.run(
        download_resources(
            resources,
            repo,
            "test-dataset",
            client,
            source_id="test-source",
        )
    )

    assert results == []
