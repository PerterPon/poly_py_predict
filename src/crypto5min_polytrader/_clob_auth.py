"""Shared helper: derive CLOB API credentials with retry.

Polymarket's ``POST /auth/api-key`` endpoint returns HTTP 400 Bad Request
for this account type.  This wrapper skips that endpoint entirely and calls
``derive_api_key()`` directly (GET /auth/derive-api-key), which always works.
"""

from __future__ import annotations

import logging
import time

log = logging.getLogger(__name__)


def derive_api_creds_with_retry(
    client,
    *,
    max_retries: int = 3,
    base_delay: float = 2.0,
) -> None:
    """Call ``derive_api_key()`` directly with exponential backoff.

    Bypasses ``create_or_derive_api_creds()`` which always attempts
    ``POST /auth/api-key`` first (returns 400 for this account) before
    falling back to the derive endpoint.  Going direct eliminates the
    guaranteed 400 failure on every auth call.

    On success the client's API creds are set in-place.
    On failure after all retries, raises ``RuntimeError``.
    """
    last_err: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            creds = client.derive_api_key()
            client.set_api_creds(creds)
            if attempt > 1:
                log.info('CLOB auth (derive) succeeded on attempt %d/%d', attempt, max_retries)
            else:
                log.debug('CLOB auth (derive) succeeded')
            return
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                delay = base_delay * (2 ** (attempt - 1))
                log.warning(
                    'CLOB auth attempt %d/%d failed (%s), retrying in %.1fs ...',
                    attempt,
                    max_retries,
                    e,
                    delay,
                )
                time.sleep(delay)
            else:
                log.error('CLOB auth failed after %d attempts: %s', max_retries, e)
    raise RuntimeError(
        f'CLOB auth failed after {max_retries} attempts: {last_err}'
    ) from last_err
