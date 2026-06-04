import pytest

from quantilica.core.retry import RetryError, exponential_delay, retry_call, with_retry


def test_exponential_delay_caps_at_max_delay():
    assert exponential_delay(1, base_delay=2, max_delay=10) == 2
    assert exponential_delay(2, base_delay=2, max_delay=10) == 4
    assert exponential_delay(4, base_delay=2, max_delay=10) == 10


def test_exponential_delay_rejects_invalid_attempt():
    with pytest.raises(ValueError):
        exponential_delay(0)


def test_retry_call_retries_until_success():
    calls = 0

    def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TimeoutError("not yet")
        return "ok"

    result = retry_call(
        flaky,
        attempts=3,
        base_delay=0,
        retry_exceptions=(TimeoutError,),
        sleep=lambda _: None,
    )

    assert result == "ok"
    assert calls == 3


def test_retry_call_raises_retry_error_when_exhausted():
    with pytest.raises(RetryError) as exc_info:
        retry_call(
            lambda: (_ for _ in ()).throw(TimeoutError("nope")),
            attempts=2,
            base_delay=0,
            retry_exceptions=(TimeoutError,),
            sleep=lambda _: None,
        )

    assert exc_info.value.attempts == 2


def test_with_retry_decorator():
    calls = 0

    @with_retry(attempts=2, base_delay=0, retry_exceptions=(RuntimeError,))
    def flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("try again")
        return "ok"

    assert flaky() == "ok"
    assert calls == 2
