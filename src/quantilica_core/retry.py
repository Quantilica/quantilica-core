"""Small retry helpers with exponential backoff."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import ParamSpec, TypeVar

from .exceptions import QuantilicaError

P = ParamSpec("P")
T = TypeVar("T")


class RetryError(QuantilicaError):
    """Raised when a retry policy exhausts all attempts."""

    def __init__(self, message: str, *, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts


def exponential_delay(
    attempt: int,
    *,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
) -> float:
    """Return the delay for a one-based retry attempt number."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    if jitter > 0:
        delay += random.uniform(0, jitter)
    return delay


def retry_call(
    func: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call a function using retry with exponential backoff."""
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except retry_exceptions as exc:
            last_error = exc
            if attempt == attempts:
                break
            sleep(
                exponential_delay(
                    attempt,
                    base_delay=base_delay,
                    max_delay=max_delay,
                    jitter=jitter,
                )
            )

    message = f"Operation failed after {attempts} attempt(s)"
    raise RetryError(message, attempts=attempts) from last_error


async def async_retry_call(
    func: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Call an async function using retry with exponential backoff."""
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await func()
        except retry_exceptions as exc:
            last_error = exc
            if attempt == attempts:
                break
            await sleep(
                exponential_delay(
                    attempt,
                    base_delay=base_delay,
                    max_delay=max_delay,
                    jitter=jitter,
                )
            )

    message = f"Async operation failed after {attempts} attempt(s)"
    raise RetryError(message, attempts=attempts) from last_error


def with_retry(
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorate a function with retry behavior."""

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return retry_call(
                lambda: func(*args, **kwargs),
                attempts=attempts,
                base_delay=base_delay,
                max_delay=max_delay,
                jitter=jitter,
                retry_exceptions=retry_exceptions,
            )

        return wrapper

    return decorator


def with_async_retry(
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorate an async function with retry behavior."""

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return await async_retry_call(
                lambda: func(*args, **kwargs),
                attempts=attempts,
                base_delay=base_delay,
                max_delay=max_delay,
                jitter=jitter,
                retry_exceptions=retry_exceptions,
            )

        return wrapper

    return decorator
