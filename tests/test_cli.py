import logging

import pytest

pytest.importorskip("rich")

from quantilica_core.cli import (  # noqa: E402
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
