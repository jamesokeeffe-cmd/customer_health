from __future__ import annotations

"""Retry helpers for extractor API calls.

Provides:
- `mount_retry_adapter`: configure automatic retries on a ``requests.Session``
  (used by Intercom and Jira extractors).
- `retry_on_transient`: decorator that retries a function on transient exceptions
  with exponential backoff (used by Looker and Salesforce SDK calls).
"""

import logging
import time
from functools import wraps
from typing import Callable, TypeVar

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)

# HTTP status codes considered transient
_RETRY_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


def mount_retry_adapter(
    session: Session,
    *,
    total: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: frozenset[int] = _RETRY_STATUS_CODES,
) -> None:
    """Mount a retry-enabled HTTPAdapter on *session* for both http and https.

    Uses urllib3's built-in retry with exponential backoff.  Sleeps are:
    ``backoff_factor * (2 ** (retry_number - 1))`` seconds, i.e. 1s, 2s, 4s
    for the defaults.
    """
    retry = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        allowed_methods=["GET", "POST", "PUT", "PATCH"],
        raise_on_status=False,  # let requests raise_for_status() handle it
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


def retry_on_transient(
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    transient_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """Decorator that retries a function on transient exceptions.

    Intended for SDK calls (Looker, Salesforce) where we can't use
    urllib3-level retries.  Non-transient exceptions (e.g. ValueError)
    should be excluded from *transient_exceptions* by the caller.

    Backoff: ``backoff_factor * (2 ** (attempt - 1))`` seconds.
    """
    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except transient_exceptions as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        delay = backoff_factor * (2 ** (attempt - 1))
                        logger.warning(
                            "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                            fn.__qualname__, attempt, max_retries, exc, delay,
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "%s failed after %d attempts: %s",
                            fn.__qualname__, max_retries, exc,
                        )
            raise last_exc  # type: ignore[misc]
        return wrapper  # type: ignore[return-value]
    return decorator
