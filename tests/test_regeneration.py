"""Regeneration detector: same data with a new stamp or ETag is "skipped"."""

import gzip
import hashlib
import io
from dataclasses import replace

from quantilica.core.manifests import DownloadManifest, SourceMetadata
from quantilica.core.regeneration import (
    RegenerationRule,
    detect_regeneration,
    normalized_content_sha256,
    with_content_hash,
)

RULE = RegenerationRule(stamp_columns=("DatGeracaoConjuntoDados",))


def _csv(stamp: str, *, extra_row: bool = False) -> bytes:
    lines = [
        "SigAgente;MdaPotencia;DatGeracaoConjuntoDados",
        f"A;10,5;{stamp}",
        f"B;7,0;{stamp}",
    ]
    if extra_row:
        lines.append(f"C;1,0;{stamp}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _manifest(content: bytes, *, etag: str, rule=RULE) -> DownloadManifest:
    manifest = DownloadManifest.from_content(
        source_id="aneel",
        dataset_id="geracao",
        url="https://dados.example/geracao.csv",
        content=content,
    )
    manifest = replace(manifest, source_meta=SourceMetadata(etag=etag))
    return with_content_hash(manifest, normalized_content_sha256(content, rule), rule)


def test_only_the_stamp_changed_is_skipped_even_with_new_etag():
    old = _csv("2026-11-14 03:00:01")
    new = _csv("2026-11-15 03:00:07")
    previous = _manifest(old, etag='"abc"')

    result = detect_regeneration(
        new, RULE, previous_manifest=previous, new_sha256=_sha(new), new_etag='"xyz"'
    )

    assert result.status == "skipped"
    assert result.skipped
    assert result.raw_changed is True  # the bytes did change...
    assert result.etag_changed is True  # ...and so did the ETag
    assert result.reason == "stamp/ETag only"


def test_same_data_new_etag_identical_bytes_is_skipped():
    content = _csv("2026-11-14 03:00:01")
    previous = _manifest(content, etag='"abc"')
    result = detect_regeneration(
        content, RULE, previous_manifest=previous, new_etag='"new"'
    )
    assert result.status == "skipped"
    assert result.etag_changed is True


def test_real_change_is_not_skipped():
    previous = _manifest(_csv("2026-11-14"), etag='"abc"')
    result = detect_regeneration(
        _csv("2026-11-15", extra_row=True), RULE, previous_manifest=previous
    )
    assert result.status == "changed"
    assert not result.skipped
    assert result.reason == "normalized content differs"


def test_no_previous_is_new():
    assert detect_regeneration(_csv("x"), RULE).status == "new"


def test_previous_without_content_hash_is_changed_not_guessed():
    content = _csv("2026-11-14")
    bare = DownloadManifest.from_content(
        source_id="s", dataset_id="d", url="https://x/y.csv", content=content
    )
    result = detect_regeneration(content, RULE, previous_manifest=bare)
    assert result.status == "changed"


def test_hash_from_another_normalizer_is_not_comparable():
    content = _csv("2026-11-14")
    other = RegenerationRule(stamp_columns=("Outra",))
    previous = _manifest(content, etag='"a"', rule=other)
    assert detect_regeneration(content, RULE, previous_manifest=previous).status == (
        "changed"
    )


def test_previous_source_fallback_when_manifest_lacks_hash(tmp_path):
    old = tmp_path / "old.csv"
    old.write_bytes(_csv("2026-11-14"))
    bare = DownloadManifest.from_content(
        source_id="s", dataset_id="d", url="https://x/y.csv", content=b"x"
    )
    result = detect_regeneration(
        _csv("2026-11-15"), RULE, previous_manifest=bare, previous_source=old
    )
    assert result.status == "skipped"


def test_normalizer_and_hash_are_recorded_in_the_manifest():
    content = _csv("2026-11-14")
    manifest = _manifest(content, etag='"a"')
    assert manifest.normalizer == RULE.name
    assert manifest.content_sha256 == normalized_content_sha256(content, RULE)
    assert "datgeracaoconjuntodados" in RULE.name


def test_container_differences_do_not_matter():
    base = _csv("2026-11-14")
    crlf = base.replace(b"\n", b"\r\n")
    bom = b"\xef\xbb\xbf" + base
    gz = gzip.compress(base)
    expected = normalized_content_sha256(base, RULE)
    assert normalized_content_sha256(crlf, RULE) == expected
    assert normalized_content_sha256(bom, RULE) == expected
    assert normalized_content_sha256(gz, RULE) == expected
    assert normalized_content_sha256(io.BytesIO(base), RULE) == expected


def test_stamp_column_in_the_middle_and_case_insensitive():
    rule = RegenerationRule(stamp_columns=("carimbo",))
    a = b"id;Carimbo;valor\n1;t1;9\n2;t1;8\n"
    b = b"id;Carimbo;valor\n1;t2;9\n2;t2;8\n"
    assert normalized_content_sha256(a, rule) == normalized_content_sha256(b, rule)


def test_reading_a_path_and_latin1(tmp_path):
    rule = RegenerationRule(stamp_columns=("ts",), encoding="latin-1")
    path = tmp_path / "a.csv"
    path.write_bytes("nome;ts\nJoão;1\n".encode("latin-1"))
    other = "nome;ts\nJoão;2\n".encode("latin-1")
    assert normalized_content_sha256(path, rule) == normalized_content_sha256(
        other, rule
    )


def test_sort_rows_ignores_row_order():
    rule = RegenerationRule(stamp_columns=("ts",), sort_rows=True)
    a = b"id;ts\n1;x\n2;x\n"
    b = b"id;ts\n2;y\n1;y\n"
    assert normalized_content_sha256(a, rule) == normalized_content_sha256(b, rule)
    ordered = RegenerationRule(stamp_columns=("ts",))
    assert normalized_content_sha256(a, ordered) != normalized_content_sha256(
        b, ordered
    )


def test_stream_is_left_open_when_not_owned():
    stream = io.BytesIO(_csv("x"))
    normalized_content_sha256(stream, RULE)
    assert not stream.closed


def test_header_difference_is_a_real_change():
    assert normalized_content_sha256(b"id;x\n1;2\n", RULE) != (
        normalized_content_sha256(b"id;y\n1;2\n", RULE)
    )


def _sha(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
