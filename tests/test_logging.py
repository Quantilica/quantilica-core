import logging

import pytest

from quantilica_core.logging import bind_context, get_logger, log_step


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


def test_log_step_logs_failure(caplog):
    logger = logging.getLogger("quantilica_core.tests.log_step_failure")

    with pytest.raises(RuntimeError):
        with caplog.at_level(logging.INFO, logger=logger.name):
            with log_step(logger, "download", source="ibge"):
                raise RuntimeError("failed")

    messages = [record.getMessage() for record in caplog.records]
    assert any("Failed download" in message for message in messages)
