import logging

import pytest

pytest.importorskip("rich")

from quantilica.core.cli import (  # noqa: E402
    expand_years_cli,
    get_console,
    make_batch_progress,
    make_download_progress,
    setup_rich_logging,
)


def test_get_console_is_shared():
    assert get_console() is get_console()


def test_setup_rich_logging_levels():
    setup_rich_logging(verbose=False)
    assert logging.getLogger().level == logging.WARNING

    setup_rich_logging(verbose=True)
    assert logging.getLogger().level == logging.DEBUG


def test_make_batch_progress_builds_progress():
    from rich.progress import Progress

    assert isinstance(make_batch_progress(), Progress)


def test_make_download_progress_builds_progress():
    from rich.progress import Progress

    assert isinstance(make_download_progress(), Progress)


def test_expand_years_cli():
    # Test normal expansion
    assert expand_years_cli(["2020:2022", "2024"]) == [2020, 2021, 2022, 2024]
    # Test default range when input is None/empty
    assert expand_years_cli(None, default_range="2018:2020") == [2018, 2019, 2020]
    # Test with invalid year (should print warning and skip it, returning only the valid one)
    assert expand_years_cli(["2020", "invalid", "2022"]) == [2020, 2022]

