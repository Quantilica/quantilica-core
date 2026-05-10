import io
import logging

import pytest

from quantilica_core.logging import (
    bind_context,
    configure_cli_logging,
    get_logger,
    log_step,
)


def test_bind_context_appends_sorted_fields():
    message = bind_context("hello", dataset="ipca", source="ibge")

    assert message == "hello dataset=ipca source=ibge"


def test_get_logger_adds_null_handler():
    logger = get_logger("quantilica_core.tests.sample")

    assert any(isinstance(handler, logging.NullHandler) for handler in logger.handlers)


def test_log_step_logs_start_and_finish(caplog):
    logger = logging.getLogger("quantilica_core.tests.log_step")

    with caplog.at_level(logging.INFO, logger=logger.name):
        with log_step(logger, "download", source="ibge"):
            pass

    messages = [record.getMessage() for record in caplog.records]
    assert any("Starting download source=ibge" in message for message in messages)
    assert any("Finished download" in message for message in messages)


def test_configure_cli_logging_defaults_to_info():
    stream = io.StringIO()
    configure_cli_logging(stream=stream)
    try:
        assert logging.getLogger().level == logging.INFO

        logger = logging.getLogger("quantilica_core.tests.cli_default")
        logger.info("visible")
        logger.debug("hidden")

        output = stream.getvalue()
        assert "visible" in output
        assert "hidden" not in output
    finally:
        logging.basicConfig(force=True)


def test_configure_cli_logging_verbose_enables_debug():
    stream = io.StringIO()
    configure_cli_logging(verbose=True, stream=stream)
    try:
        assert logging.getLogger().level == logging.DEBUG

        logger = logging.getLogger("quantilica_core.tests.cli_verbose")
        logger.debug("debug-line")

        assert "debug-line" in stream.getvalue()
    finally:
        logging.basicConfig(force=True)


def test_configure_cli_logging_replaces_previous_handlers():
    first = io.StringIO()
    second = io.StringIO()

    configure_cli_logging(stream=first)
    configure_cli_logging(stream=second)
    try:
        logging.getLogger("quantilica_core.tests.cli_replace").info("only-second")

        assert "only-second" not in first.getvalue()
        assert "only-second" in second.getvalue()
    finally:
        logging.basicConfig(force=True)


def test_log_step_logs_failure(caplog):
    logger = logging.getLogger("quantilica_core.tests.log_step_failure")

    with pytest.raises(RuntimeError):
        with caplog.at_level(logging.INFO, logger=logger.name):
            with log_step(logger, "download", source="ibge"):
                raise RuntimeError("failed")

    messages = [record.getMessage() for record in caplog.records]
    assert any("Failed download" in message for message in messages)
