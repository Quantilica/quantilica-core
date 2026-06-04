from quantilica.core.cache import FileCache


def test_file_cache_key_for_url_is_deterministic(tmp_path):
    cache = FileCache(tmp_path)

    key_a = cache.key_for_url(
        "https://example.test/data",
        params={"b": "2", "a": "1"},
        headers={"Accept": "application/json"},
    )
    key_b = cache.key_for_url(
        "https://example.test/data",
        params={"a": "1", "b": "2"},
        headers={"Accept": "application/json"},
    )

    assert key_a == key_b
    assert len(key_a) == 64


def test_file_cache_write_and_read_bytes(tmp_path):
    cache = FileCache(tmp_path)
    key = cache.key_for_url("https://example.test/data")

    entry = cache.write_bytes(
        key, b"abc", metadata={"url": "https://example.test/data"}
    )

    assert cache.exists(key)
    assert cache.read_bytes(key) == b"abc"
    assert entry.size_bytes == 3
    assert entry.metadata["url"] == "https://example.test/data"


def test_file_cache_read_metadata(tmp_path):
    cache = FileCache(tmp_path)
    key = cache.key_for_url("https://example.test/data")

    written = cache.write_bytes(key, b"abc")
    read = cache.read_metadata(key)

    assert read.key == written.key
    assert read.sha256 == written.sha256
    assert read.path == written.path


def test_file_cache_get_bytes_returns_none_for_missing_key(tmp_path):
    cache = FileCache(tmp_path)

    assert cache.get_bytes("a" * 64) is None


def test_file_cache_is_fresh_respects_ttl(tmp_path):
    cache = FileCache(tmp_path)
    key = cache.key_for_url("https://example.test/data")
    cache.write_bytes(key, b"abc")

    assert cache.is_fresh(key, ttl_seconds=60)
    assert cache.get_bytes(key, ttl_seconds=60) == b"abc"
