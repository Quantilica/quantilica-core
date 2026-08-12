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
        """Initialize the RetryError.

        Args:
            message: The error message.
            attempts: The number of attempts made before failing.
        """
        super().__init__(message)
        self.attempts = attempts


def exponential_delay(
    attempt: int,
    *,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
) -> float:
    """Return the delay for a one-based retry attempt number.

    Args:
        attempt: The current attempt number (1-based).
        base_delay: The base delay in seconds.
        max_delay: The maximum delay in seconds.
        jitter: The maximum jitter to add to the delay.

    Returns:
        float: The calculated delay in seconds.

    Raises:
        ValueError: If attempt is less than 1.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
    if jitter > 0:
        delay += random.uniform(0, jitter)
    return delay


def retry_call[T](
    func: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call a function using retry with exponential backoff.

    Args:
        func: The function to call.
        attempts: Maximum number of attempts.
        base_delay: Base delay between attempts.
        max_delay: Maximum delay between attempts.
        jitter: Maximum jitter to add.
        retry_exceptions: Tuple of exceptions to catch and retry.
        sleep: Function to use for sleeping.

    Returns:
        T: The return value of the function.

    Raises:
        ValueError: If attempts is less than 1.
        RetryError: If all attempts fail.
    """
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


async def async_retry_call[T](
    func: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: float = 0.0,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Call an async function using retry with exponential backoff.

    Args:
        func: The async function to call.
        attempts: Maximum number of attempts.
        base_delay: Base delay between attempts.
        max_delay: Maximum delay between attempts.
        jitter: Maximum jitter to add.
        retry_exceptions: Tuple of exceptions to catch and retry.
        sleep: Async function to use for sleeping.

    Returns:
        T: The return value of the function.

    Raises:
        ValueError: If attempts is less than 1.
        RetryError: If all attempts fail.
    """
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
    """Decorate a function with retry behavior.

    Args:
        attempts: Maximum number of attempts.
        base_delay: Base delay between attempts.
        max_delay: Maximum delay between attempts.
        jitter: Maximum jitter to add.
        retry_exceptions: Tuple of exceptions to catch and retry.

    Returns:
        Callable[[Callable[P, T]], Callable[P, T]]: The decorator function.
    """

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
    """Decorate an async function with retry behavior.

    Args:
        attempts: Maximum number of attempts.
        base_delay: Base delay between attempts.
        max_delay: Maximum delay between attempts.
        jitter: Maximum jitter to add.
        retry_exceptions: Tuple of exceptions to catch and retry.

    Returns:
        Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]: The decorator.
    """

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
