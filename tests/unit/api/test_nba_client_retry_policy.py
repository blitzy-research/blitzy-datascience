"""Retry-classification contract for ``api.nba_client`` -- the transport stage.

This module pins the four behaviours that decide whether a failed HTTPS GET is
retried, and how the resulting terminal failure is labelled:

1. The **complete truth table** of ``api.nba_client._is_transient`` -- ten
   rows, one per exception shape the transport layer can encounter.
2. The **exactness of the ``>= 500`` cut** -- 500 is retried, 499 is not, both
   at the predicate level and end-to-end through ``NBAClient.get``.
3. The **permanent-4xx non-retry attempt count** -- a 404 issues exactly ONE
   HTTP attempt, not ``config.RETRY_ATTEMPTS`` of them.
4. The **``http_5xx`` and ``http_4xx_non_429`` reason labels** carried by
   ``nba_request_failures_total``.

The verified gap this module closes
-----------------------------------
Three greps across the whole ``tests/`` tree, run before this module existed:

* ``_is_transient`` -- **3 hits, and all three are docstring/comment PROSE
  only**, in ``tests/unit/api/test_nba_client.py`` at lines 57, 68 and 542.
  The classification decision itself had **zero** functional coverage.
* ``http_5xx`` -- **0 hits.**
* ``http_4xx_non_429`` -- **0 hits.**

The 26 tests in the sibling module cover required headers, rate-limit
ordering, metrics emission, URL construction, the timeout keyword,
``raise_for_status`` ordering, the correlation header, three
retry-then-success paths, two retry-exhaustion paths and session reuse --
but never *which* failures are worth retrying.

The headline mutation this module detects
-----------------------------------------
Making ``_is_transient`` return ``True`` for *every* ``HTTPError`` would cause
every permanent 404 to be retried five times -- and every pre-existing test in
the suite would still pass. ``api/nba_client.py`` lines 190-197 document
precisely that hazard:

    Using ``retry_if_exception_type(HTTPError)`` is too broad because
    ``requests.Response.raise_for_status`` raises ``HTTPError`` for *every*
    non-2xx status uniformly -- including permanent 4xx statuses (400 Bad
    Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 418 I'm a
    teapot, 422 Unprocessable Entity, etc.). Retrying those wastes NBA Stats
    API budget, masks configuration errors behind long exponential-backoff
    delays, and risks triggering upstream abuse protections.

Every test below therefore names, in its own docstring, the single-statement
mutation it detects. Mutation resistance -- not a coverage percentage -- is the
acceptance bar: this project ships no coverage instrument and none may be
added, so no percentage is claimed anywhere.

No snapshot assertions
----------------------
Every expected value here is a **structural constant read from
``api/nba_client.py`` at a cited line**, never a value captured by running the
code under test:

* the ten booleans of the truth table come from the predicate body at lines
  231-247;
* the exactly-one-attempt count follows from ``retry_if_exception``
  (``_is_transient``) plus ``reraise=True`` at lines 541-551;
* the three reason labels come from the closed taxonomy at lines 495-504;
* the ``1.0`` failure-counter value follows from the ``inc`` call sitting
  inside ``get()``'s ``except RequestException`` block at lines 505-508 --
  once per *invocation*, never once per *attempt*.

Placement, tier and isolation
-----------------------------
This is a focused **sibling** of ``tests/unit/api/test_nba_client.py`` rather
than an addition to it: that module spans 20,676 bytes across 26 tests and is
frozen (no existing test may be deleted, skipped, weakened or relaxed), and a
separate module keeps the retry-classification concern reviewable on its own.
It is auto-collected because ``pytest.ini`` sets ``python_files = test_*.py``.

The module carries **no pytest marker**, so it runs on the default offline
tier, and it performs **no network I/O**: every request is served by a
``MagicMock`` installed on the client's session through ``monkeypatch``.

``_make_response``, ``fast_tenacity_sleep``, ``mock_rate_limiter`` and
``client`` are deliberate **module-local replicas** of the same doubles in the
sibling module. They are intentionally neither imported from that sibling
(which would couple two test modules and make a frozen file a dependency) nor
promoted into ``tests/conftest.py`` (which is shared by every test module and
must not grow for one module's benefit).

The exception classes are obtained from the ``api.nba_client`` namespace --
which binds them at module scope on lines 88-91 -- rather than by reaching for
the HTTP library directly. This honours the ``tests/conftest.py`` do-not list
("Do NOT import :mod:`requests`" -- Rule 1, Single HTTP Client) and the
package directive in ``tests/unit/api/__init__.py`` lines 28-31. They are the
*same class objects*, so every ``isinstance`` check inside the production
predicate behaves identically.

The shared ``conftest.py`` autouse fixtures reset
``utils.correlation.correlation_id``, ``utils.metrics.registry`` and the
``utils.logger`` handlers before **and** after every test, so every counter
delta asserted below starts from zero, and each test passes alone, within this
module, and in the full suite in any order.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

import config
from api.nba_client import (
    HTTPError,
    NBAClient,
    RequestsConnectionError,
    Timeout,
    _is_transient,
)
from utils import metrics


# ---------------------------------------------------------------------------
# Module constants -- each read from the production source, not from a run
# ---------------------------------------------------------------------------

# Endpoint label reused by every test. Endpoint names are call-site arguments
# rather than configuration, so there is no ``config`` constant to reference;
# centralising the literal here keeps the label sets below in lock-step.
ENDPOINT = "leaguedashplayerstats"

# The closed reason taxonomy from api/nba_client.py lines 496, 502 and 504.
REASON_TIMEOUT = "timeout"
REASON_HTTP_5XX = "http_5xx"
REASON_HTTP_4XX_NON_429 = "http_4xx_non_429"

# Counter names. ``nba_request_failures_total`` is incremented by the ``inc``
# call at api/nba_client.py lines 505-508, which sits inside ``get()``'s
# ``except RequestException`` block -- ONCE PER get() CALL, never once per
# retry attempt. ``nba_requests_total`` (line 450) is likewise per call.
# ``nba_retries_total`` (line 151) is the only per-attempt counter: it is
# incremented from the tenacity ``before_sleep`` callback.
FAILURES_COUNTER = "nba_request_failures_total"
REQUESTS_COUNTER = "nba_requests_total"
RETRIES_COUNTER = "nba_retries_total"

# A non-transient failure is not retried at all, so tenacity invokes the
# decorated ``_request`` exactly once: api/nba_client.py lines 548-550 wire
# ``retry=retry_if_exception(_is_transient)`` together with ``reraise=True``.
EXPECTED_SINGLE_ATTEMPT = 1

# ``MetricsRegistry.get_counter_value`` (utils/metrics.py line 856) always
# returns a Python float, and returns 0.0 for a label set that was registered
# but never incremented -- so both comparisons below are exact and meaningful.
EXPECTED_COUNTER_HIT = 1.0
EXPECTED_COUNTER_MISS = 0.0

# Statuses named by the predicate at api/nba_client.py line 243 (429 and the
# ``>= 500`` cut) and by the permanent-4xx roll-call in the design comment at
# lines 193-194.
STATUS_RATE_LIMITED = 429
STATUS_SERVER_ERROR = 500
STATUS_SERVICE_UNAVAILABLE = 503
STATUS_LAST_NON_5XX = 499
STATUS_BAD_REQUEST = 400
STATUS_UNAUTHORIZED = 401
STATUS_FORBIDDEN = 403
STATUS_NOT_FOUND = 404
STATUS_TEAPOT = 418
STATUS_UNPROCESSABLE = 422


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(
    status_code: int = 200,
    body: Optional[Dict[str, Any]] = None,
) -> MagicMock:
    """Return a MagicMock mimicking the subset of the HTTP response we touch.

    The mock exposes ``status_code``, ``json()`` returning ``body`` (defaulting
    to an empty ``resultSets`` envelope), and ``raise_for_status()``. For
    non-2xx status codes the ``raise_for_status`` side_effect is an
    ``HTTPError`` whose ``.response`` points back at the mock.
    """
    response = MagicMock()
    response.status_code = status_code
    response.json = MagicMock(
        return_value=body if body is not None else {"resultSets": []}
    )
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        error = HTTPError(f"HTTP {status_code}")
        # Load-bearing: ``_is_transient`` reads ``exc.response.status_code``
        # (api/nba_client.py line 239) and the reason classifier reads
        # ``exc.response.status_code >= 500`` (line 500). Without this
        # attachment every classification silently degrades to
        # False / http_4xx_non_429 and the tests below would pass for
        # entirely the wrong reason.
        error.response = response
        response.raise_for_status.side_effect = error
    return response


def _http_error_with_status(status_code: int) -> HTTPError:
    """Return a synthetic ``HTTPError`` carrying ``status_code``.

    Used by the pure-predicate tests, which need no HTTP round-trip at all:
    only the ``.response.status_code`` attribute chain that
    ``api.nba_client._is_transient`` reads on line 239.
    """
    error = HTTPError(f"HTTP {status_code}")
    response = MagicMock()
    response.status_code = status_code
    error.response = response
    return error


def _failure_count(reason: str) -> float:
    """Read ``nba_request_failures_total`` for ``ENDPOINT`` and ``reason``.

    The label dict is spelled out exactly as api/nba_client.py lines 505-508
    emit it, so every call site asserts against the real, complete label set
    rather than a loosely matched subset.
    """
    return metrics.registry.get_counter_value(
        FAILURES_COUNTER,
        {"endpoint": ENDPOINT, "reason": reason},
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fast_tenacity_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Short-circuit retry sleeps so the suite stays fast.

    ``tenacity.nap.sleep`` internally calls ``time.sleep``; patching both
    makes every retry resolve immediately regardless of the
    ``wait_exponential`` parameters baked into the decorator at class-
    definition time (``config.RETRY_MULTIPLIER`` 2, ``RETRY_MIN_WAIT`` 1,
    ``RETRY_MAX_WAIT`` 60 -- so a single missed patch costs real seconds).
    """
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("tenacity.nap.sleep", lambda _seconds: None)


@pytest.fixture
def mock_rate_limiter() -> MagicMock:
    """Return a MagicMock standing in for utils.rate_limiter.RateLimiter.

    Mocked so that no real sleeping happens on the critical path; ``wait`` is
    a ``MagicMock`` so call counts stay inspectable.
    """
    limiter = MagicMock()
    limiter.wait = MagicMock(return_value=None)
    limiter.interval = 1.0  # production __init__ logs rate_limiter.interval
    return limiter


@pytest.fixture
def client(mock_rate_limiter: MagicMock) -> NBAClient:
    """Default NBAClient with an injected mocked rate_limiter.

    ``NBAClient.__init__`` is keyword-only (the bare ``*`` separator at
    api/nba_client.py lines 296-302), so the collaborator is passed by name.
    The logger and metrics default to the production singletons, which the
    autouse fixtures in ``conftest.py`` reset between tests.
    """
    return NBAClient(rate_limiter=mock_rate_limiter)


# ---------------------------------------------------------------------------
# _is_transient truth table
# ---------------------------------------------------------------------------
# The complete contract, transcribed from the predicate body at
# api/nba_client.py lines 231-247 -- NOT captured by executing the predicate:
#
#   line 231-232  Timeout / ConnectionError            -> True
#   line 238-243  HTTPError, status == 429             -> True
#   line 238-243  HTTPError, status >= 500             -> True
#   line 238-243  HTTPError, any other status          -> False
#   line 238-243  HTTPError, status is None            -> False
#   line 247      anything else (e.g. ValueError)      -> False
#
# Laid out as an explicit (exception, expected) table so a reviewer sees the
# whole contract at a glance and can check it against those six lines.
# ---------------------------------------------------------------------------
_IS_TRANSIENT_TRUTH_TABLE = [
    pytest.param(Timeout("socket read timed out"), True, id="timeout"),
    pytest.param(
        RequestsConnectionError("connection reset by peer"),
        True,
        id="connection_error",
    ),
    pytest.param(
        _http_error_with_status(STATUS_RATE_LIMITED), True, id="http_429"
    ),
    pytest.param(
        _http_error_with_status(STATUS_SERVER_ERROR), True, id="http_500"
    ),
    pytest.param(
        _http_error_with_status(STATUS_SERVICE_UNAVAILABLE),
        True,
        id="http_503",
    ),
    pytest.param(
        _http_error_with_status(STATUS_LAST_NON_5XX), False, id="http_499"
    ),
    pytest.param(
        _http_error_with_status(STATUS_NOT_FOUND), False, id="http_404"
    ),
    pytest.param(
        _http_error_with_status(STATUS_BAD_REQUEST), False, id="http_400"
    ),
    pytest.param(
        HTTPError("no response attached"),
        False,
        id="http_error_without_response",
    ),
    pytest.param(ValueError("body was not JSON"), False, id="value_error"),
]


@pytest.mark.parametrize(("exc", "expected"), _IS_TRANSIENT_TRUTH_TABLE)
def test_is_transient_classifies_every_exception_shape_exactly(
    exc: BaseException,
    expected: bool,
) -> None:
    """_is_transient matches its ten-row contract and returns a real bool.

    Mutation detected: any edit to the predicate body at api/nba_client.py
    lines 231-247 -- widening the transient set to every ``HTTPError``
    (the headline mutation: permanent 404s would be retried five times),
    dropping ``Timeout`` or ``ConnectionError`` from the transport tuple on
    line 231, returning the bare ``status`` instead of the comparison on line
    243 (a truthy non-``bool``), or deleting the ``return False`` default on
    line 247.
    """
    # Arrange -- mirror the production attribute chain purely for diagnostics.
    observed_status = getattr(getattr(exc, "response", None), "status_code", None)

    # Act
    result = _is_transient(exc)

    # Assert -- identity, not truthiness, so a truthy non-bool is caught too.
    assert result is expected, (
        f"_is_transient({type(exc).__name__}, status={observed_status!r}) "
        f"expected {expected!r} per api/nba_client.py lines 231-247, "
        f"got {result!r}"
    )
    assert type(result) is bool, (
        f"_is_transient must return a real bool so tenacity's "
        f"retry_if_exception predicate is unambiguous; got "
        f"{type(result).__name__} ({result!r})"
    )


# ---------------------------------------------------------------------------
# Retry boundary: the >= 500 cut is exact
# ---------------------------------------------------------------------------


def test_is_transient_boundary_between_499_and_500_is_exact() -> None:
    """The 5xx cut is ``>= 500``: 500 and 503 retry, 499 does not.

    Mutation detected: changing ``status >= 500`` (api/nba_client.py line 243)
    to ``> 500``, which would stop retrying the single most common server
    error; or to ``>= 499``, which would start retrying a permanent client
    error. Dropping the ``status is not None`` guard on that same line is
    caught by the ``http_error_without_response`` row of the truth table,
    where ``None >= 500`` would raise ``TypeError`` instead of returning
    ``False``.
    """
    # Arrange -- three synthetic HTTPErrors straddling the documented cut.
    just_below_cut = _http_error_with_status(STATUS_LAST_NON_5XX)
    at_cut = _http_error_with_status(STATUS_SERVER_ERROR)
    above_cut = _http_error_with_status(STATUS_SERVICE_UNAVAILABLE)

    # Act / Assert -- the predicate is pure, so no HTTP is involved.
    assert _is_transient(at_cut) is True, (
        f"status {STATUS_SERVER_ERROR} is the FIRST 5xx and must be "
        f"transient; got {_is_transient(at_cut)!r}"
    )
    assert _is_transient(above_cut) is True, (
        f"status {STATUS_SERVICE_UNAVAILABLE} is above the cut and must be "
        f"transient; got {_is_transient(above_cut)!r}"
    )
    assert _is_transient(just_below_cut) is False, (
        f"status {STATUS_LAST_NON_5XX} is the LAST non-5xx and must NOT be "
        f"transient; got {_is_transient(just_below_cut)!r}"
    )


def test_status_500_is_retried_to_exhaustion_through_client_get(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistent 500 consumes every configured attempt, then re-raises.

    The predicate-level boundary is observable in the real retry loop, which
    is where a mutation would actually bite.

    Mutation detected: narrowing the 5xx branch on api/nba_client.py line 243
    so a server error is treated as permanent -- the attempt count would
    collapse from ``config.RETRY_ATTEMPTS`` to 1 and a recoverable upstream
    blip would surface to the caller as a hard failure. Removing
    ``reraise=True`` (line 550) is caught too: the caller would see
    ``tenacity.RetryError`` instead of ``HTTPError``.
    """
    # Arrange -- return_value (not a side_effect list) so every attempt, no
    # matter how many tenacity makes, receives the same 500.
    mock_get = MagicMock(return_value=_make_response(STATUS_SERVER_ERROR))
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert
    assert mock_get.call_count == config.RETRY_ATTEMPTS, (
        f"a transient {STATUS_SERVER_ERROR} must be retried until "
        f"config.RETRY_ATTEMPTS={config.RETRY_ATTEMPTS} attempts are spent; "
        f"got {mock_get.call_count} attempts"
    )
    assert config.RETRY_ATTEMPTS > EXPECTED_SINGLE_ATTEMPT, (
        f"config.RETRY_ATTEMPTS={config.RETRY_ATTEMPTS} must exceed "
        f"{EXPECTED_SINGLE_ATTEMPT} for the retried-versus-not contrast in "
        f"this module to be observable at all"
    )


def test_status_499_is_not_retried_through_client_get(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 499 is one below the cut: exactly one attempt, HTTPError propagates.

    Mutation detected: relaxing the cut on api/nba_client.py line 243 to
    ``>= 499`` -- the attempt count would jump from 1 to
    ``config.RETRY_ATTEMPTS`` and a permanent client error would burn the
    upstream request budget four extra times.
    """
    # Arrange -- a one-element side_effect list, so an unexpected second
    # attempt fails loudly with StopIteration instead of passing silently.
    mock_get = MagicMock(side_effect=[_make_response(STATUS_LAST_NON_5XX)])
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert
    assert mock_get.call_count == EXPECTED_SINGLE_ATTEMPT, (
        f"status {STATUS_LAST_NON_5XX} is NOT 5xx and must issue exactly "
        f"{EXPECTED_SINGLE_ATTEMPT} HTTP attempt; got "
        f"{mock_get.call_count} against config.RETRY_ATTEMPTS="
        f"{config.RETRY_ATTEMPTS}"
    )
    assert _failure_count(REASON_HTTP_4XX_NON_429) == EXPECTED_COUNTER_HIT, (
        f"status {STATUS_LAST_NON_5XX} must be labelled "
        f"{REASON_HTTP_4XX_NON_429!r} because ``499 >= 500`` is False in the "
        f"classifier at api/nba_client.py line 500; got "
        f"{_failure_count(REASON_HTTP_4XX_NON_429)!r}"
    )
    assert _failure_count(REASON_HTTP_5XX) == EXPECTED_COUNTER_MISS, (
        f"status {STATUS_LAST_NON_5XX} must NOT be labelled "
        f"{REASON_HTTP_5XX!r}; got {_failure_count(REASON_HTTP_5XX)!r}"
    )


# ---------------------------------------------------------------------------
# Permanent 4xx: non-retry
# ---------------------------------------------------------------------------


def test_permanent_404_issues_exactly_one_http_attempt_and_propagates(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanent 404 is attempted ONCE and the HTTPError reaches the caller.

    This is the headline case. ``retry_if_exception(_is_transient)`` combined
    with ``reraise=True`` (api/nba_client.py lines 548-550) means a ``False``
    verdict short-circuits the loop and the *original* exception surfaces.

    Mutation detected: widening ``_is_transient`` to accept every
    ``HTTPError`` -- a permanent 404 would then be retried
    ``config.RETRY_ATTEMPTS`` times, wasting NBA Stats API budget, masking the
    configuration error behind exponential-backoff delays and risking upstream
    abuse protections, exactly as api/nba_client.py lines 190-197 warn.
    """
    # Arrange -- one-element side_effect list: a second attempt would raise
    # StopIteration rather than quietly succeeding.
    mock_get = MagicMock(side_effect=[_make_response(STATUS_NOT_FOUND)])
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act -- the original HTTPError must propagate, never tenacity.RetryError.
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert
    assert mock_get.call_count == EXPECTED_SINGLE_ATTEMPT, (
        f"a permanent {STATUS_NOT_FOUND} must issue exactly "
        f"{EXPECTED_SINGLE_ATTEMPT} HTTP attempt (non-transient); got "
        f"{mock_get.call_count} attempts against config.RETRY_ATTEMPTS="
        f"{config.RETRY_ATTEMPTS}"
    )
    assert config.RETRY_ATTEMPTS > EXPECTED_SINGLE_ATTEMPT, (
        f"config.RETRY_ATTEMPTS={config.RETRY_ATTEMPTS} must exceed "
        f"{EXPECTED_SINGLE_ATTEMPT}, otherwise the single-attempt assertion "
        f"above would hold even for a fully transient failure and would prove "
        f"nothing"
    )


@pytest.mark.parametrize(
    "status_code",
    [
        pytest.param(STATUS_BAD_REQUEST, id="http_400"),
        pytest.param(STATUS_UNAUTHORIZED, id="http_401"),
        pytest.param(STATUS_FORBIDDEN, id="http_403"),
        pytest.param(STATUS_NOT_FOUND, id="http_404"),
        pytest.param(STATUS_TEAPOT, id="http_418"),
        pytest.param(STATUS_UNPROCESSABLE, id="http_422"),
    ],
)
def test_permanent_4xx_family_is_never_retried(
    status_code: int,
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every permanent 4xx named in the design comment gets exactly one attempt.

    The status list is the roll-call from api/nba_client.py lines 193-194 --
    400 Bad Request, 401 Unauthorized, 403 Forbidden, 404 Not Found, 418 I'm a
    teapot, 422 Unprocessable Entity.

    Mutation detected: special-casing only some permanent statuses as
    non-transient (for example an ``exc.response.status_code != 404`` test),
    which would leave the rest of the family silently retried.
    """
    # Arrange
    mock_get = MagicMock(side_effect=[_make_response(status_code)])
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert
    assert mock_get.call_count == EXPECTED_SINGLE_ATTEMPT, (
        f"permanent status {status_code} must issue exactly "
        f"{EXPECTED_SINGLE_ATTEMPT} HTTP attempt; got {mock_get.call_count}"
    )
    assert _failure_count(REASON_HTTP_4XX_NON_429) == EXPECTED_COUNTER_HIT, (
        f"permanent status {status_code} must be counted once under "
        f"{REASON_HTTP_4XX_NON_429!r}; got "
        f"{_failure_count(REASON_HTTP_4XX_NON_429)!r}"
    )


# ---------------------------------------------------------------------------
# Failure reason labels
# ---------------------------------------------------------------------------


def test_http_5xx_reason_label_counted_once_per_call_not_once_per_attempt(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 500 exhaustion emits reason="http_5xx" exactly once, not five times.

    ``config.RETRY_ATTEMPTS`` HTTP attempts happen, yet
    ``nba_request_failures_total`` gains ``1.0`` -- because the ``inc`` at
    api/nba_client.py lines 505-508 lives in ``get()``'s ``except
    RequestException`` block, outside the retried ``_request``. The same holds
    for ``nba_requests_total`` (line 450), which counts invocations too.

    Mutation detected: moving either ``inc`` inside the retried ``_request``
    body -- five failures would be reported for one failed invocation and
    every dashboard rate would be inflated fivefold -- or mis-routing the 5xx
    branch of the classifier at lines 497-502.
    """
    # Arrange
    mock_get = MagicMock(return_value=_make_response(STATUS_SERVER_ERROR))
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert -- the retry loop really did run to exhaustion ...
    assert mock_get.call_count == config.RETRY_ATTEMPTS, (
        f"a transient {STATUS_SERVER_ERROR} must exhaust "
        f"config.RETRY_ATTEMPTS={config.RETRY_ATTEMPTS} attempts; got "
        f"{mock_get.call_count}"
    )
    # ... and yet exactly ONE terminal failure was recorded.
    assert _failure_count(REASON_HTTP_5XX) == EXPECTED_COUNTER_HIT, (
        f"{FAILURES_COUNTER}{{endpoint={ENDPOINT!r}, "
        f"reason={REASON_HTTP_5XX!r}}} must be exactly "
        f"{EXPECTED_COUNTER_HIT} (once per failed get() call, NOT once per "
        f"attempt); got {_failure_count(REASON_HTTP_5XX)!r}"
    )
    assert _failure_count(REASON_TIMEOUT) == EXPECTED_COUNTER_MISS, (
        f"an HTTP 500 is not a transport stall, so reason="
        f"{REASON_TIMEOUT!r} must stay at {EXPECTED_COUNTER_MISS}; got "
        f"{_failure_count(REASON_TIMEOUT)!r}"
    )
    assert _failure_count(REASON_HTTP_4XX_NON_429) == EXPECTED_COUNTER_MISS, (
        f"an HTTP 500 is a server error, so reason="
        f"{REASON_HTTP_4XX_NON_429!r} must stay at {EXPECTED_COUNTER_MISS}; "
        f"got {_failure_count(REASON_HTTP_4XX_NON_429)!r}"
    )
    requests_total = metrics.registry.get_counter_value(
        REQUESTS_COUNTER, {"endpoint": ENDPOINT}
    )
    assert requests_total == EXPECTED_COUNTER_HIT, (
        f"{REQUESTS_COUNTER}{{endpoint={ENDPOINT!r}}} counts invocations, not "
        f"attempts, so it must be exactly {EXPECTED_COUNTER_HIT} after one "
        f"failed get(); got {requests_total!r}"
    )


def test_http_4xx_non_429_reason_label_recorded_for_permanent_404(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanent 404 is counted once under reason="http_4xx_non_429".

    Mutation detected: collapsing the closed taxonomy at api/nba_client.py
    lines 495-504 to a single label, or letting a permanent 4xx fall into the
    ``http_5xx`` bucket -- either change would make the ``reason``-filtered
    triage query in docs/OBSERVABILITY.md answer the wrong question.
    """
    # Arrange
    mock_get = MagicMock(side_effect=[_make_response(STATUS_NOT_FOUND)])
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert
    assert _failure_count(REASON_HTTP_4XX_NON_429) == EXPECTED_COUNTER_HIT, (
        f"{FAILURES_COUNTER}{{endpoint={ENDPOINT!r}, "
        f"reason={REASON_HTTP_4XX_NON_429!r}}} must be exactly "
        f"{EXPECTED_COUNTER_HIT} after one permanent "
        f"{STATUS_NOT_FOUND}; got {_failure_count(REASON_HTTP_4XX_NON_429)!r}"
    )
    assert _failure_count(REASON_HTTP_5XX) == EXPECTED_COUNTER_MISS, (
        f"a permanent {STATUS_NOT_FOUND} must NOT land in the "
        f"{REASON_HTTP_5XX!r} bucket; got "
        f"{_failure_count(REASON_HTTP_5XX)!r}"
    )
    assert _failure_count(REASON_TIMEOUT) == EXPECTED_COUNTER_MISS, (
        f"a permanent {STATUS_NOT_FOUND} must NOT land in the "
        f"{REASON_TIMEOUT!r} bucket; got "
        f"{_failure_count(REASON_TIMEOUT)!r}"
    )


def test_http_error_without_a_response_is_not_retried_and_labelled_4xx(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A response-less HTTPError gets one attempt and the catch-all label.

    api/nba_client.py lines 206-208 state the intent verbatim: "Everything
    else -- including HTTPError without an attached response, and any other
    exception type -- is treated as non-transient and propagates on the first
    attempt." Line 239's double ``getattr`` yields ``status = None``, and line
    499's ``exc.response is not None`` guard routes the label to the catch-all
    bucket.

    Mutation detected: dropping the ``status is not None`` guard on line 243
    (``None >= 500`` raises ``TypeError``) or the ``exc.response is not None``
    guard on line 499 (``None.status_code`` raises ``AttributeError``) -- in
    either case a malformed upstream error would crash the transport instead
    of propagating cleanly.
    """
    # Arrange -- a naked HTTPError: requests leaves ``.response`` at None.
    mock_get = MagicMock(side_effect=[HTTPError("no response attached")])
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert
    assert mock_get.call_count == EXPECTED_SINGLE_ATTEMPT, (
        f"an HTTPError with no attached response is non-transient and must "
        f"issue exactly {EXPECTED_SINGLE_ATTEMPT} HTTP attempt; got "
        f"{mock_get.call_count}"
    )
    assert _failure_count(REASON_HTTP_4XX_NON_429) == EXPECTED_COUNTER_HIT, (
        f"a response-less HTTPError must fall into the "
        f"{REASON_HTTP_4XX_NON_429!r} bucket exactly once; got "
        f"{_failure_count(REASON_HTTP_4XX_NON_429)!r}"
    )
    assert _failure_count(REASON_HTTP_5XX) == EXPECTED_COUNTER_MISS, (
        f"a response-less HTTPError carries no status, so it must NOT be "
        f"labelled {REASON_HTTP_5XX!r}; got "
        f"{_failure_count(REASON_HTTP_5XX)!r}"
    )


def test_persistent_429_is_retried_yet_labelled_http_4xx_non_429(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistent 429 is retried by the predicate but not labelled 5xx.

    The retry predicate (line 243, which admits ``status == 429``) and the
    reason classifier (lines 497-504, which admit only ``status >= 500``) are
    deliberately DIFFERENT tests. A rate-limited request is therefore retried
    to exhaustion and then lands in the catch-all bucket -- precisely what the
    design comment at lines 488-494 documents.

    Mutation detected: rewriting the classifier to reuse ``_is_transient``,
    which would mislabel a persistent 429 as ``http_5xx`` and point on-call at
    the wrong upstream; or dropping ``status == 429`` from line 243, which
    would collapse the attempt count to 1 and remove the reactive backoff that
    Rule 2's spacing depends on.
    """
    # Arrange
    mock_get = MagicMock(return_value=_make_response(STATUS_RATE_LIMITED))
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert -- transient, so the full attempt budget is spent ...
    assert mock_get.call_count == config.RETRY_ATTEMPTS, (
        f"status {STATUS_RATE_LIMITED} is transient and must exhaust "
        f"config.RETRY_ATTEMPTS={config.RETRY_ATTEMPTS} attempts; got "
        f"{mock_get.call_count}"
    )
    # ... but the terminal label is the catch-all, not http_5xx.
    assert _failure_count(REASON_HTTP_4XX_NON_429) == EXPECTED_COUNTER_HIT, (
        f"a persistent {STATUS_RATE_LIMITED} must be counted once under "
        f"{REASON_HTTP_4XX_NON_429!r} because {STATUS_RATE_LIMITED} is not "
        f">= {STATUS_SERVER_ERROR}; got "
        f"{_failure_count(REASON_HTTP_4XX_NON_429)!r}"
    )
    assert _failure_count(REASON_HTTP_5XX) == EXPECTED_COUNTER_MISS, (
        f"the reason classifier must NOT reuse the retry predicate: status "
        f"{STATUS_RATE_LIMITED} is retryable yet is not a server error, so "
        f"{REASON_HTTP_5XX!r} must stay at {EXPECTED_COUNTER_MISS}; got "
        f"{_failure_count(REASON_HTTP_5XX)!r}"
    )


def test_connection_error_is_labelled_timeout_not_http_4xx(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dropped connection is retried and labelled reason="timeout".

    api/nba_client.py line 495 groups ``Timeout`` and ``ConnectionError`` into
    one transport-stall bucket. The sibling module asserts that label only for
    ``Timeout``, so the ``ConnectionError`` half of the tuple was unpinned.

    Mutation detected: dropping ``RequestsConnectionError`` from the tuple on
    line 495, which would silently reclassify every dropped connection as a
    client error; or from the tuple on line 231, which would collapse the
    attempt count to 1 and stop retrying a textbook transient failure.
    """
    # Arrange
    mock_get = MagicMock(
        side_effect=RequestsConnectionError("connection reset by peer")
    )
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(RequestsConnectionError):
        client.get(ENDPOINT, {})

    # Assert
    assert mock_get.call_count == config.RETRY_ATTEMPTS, (
        f"a dropped connection is transient and must exhaust "
        f"config.RETRY_ATTEMPTS={config.RETRY_ATTEMPTS} attempts; got "
        f"{mock_get.call_count}"
    )
    assert _failure_count(REASON_TIMEOUT) == EXPECTED_COUNTER_HIT, (
        f"a ConnectionError shares the {REASON_TIMEOUT!r} bucket with "
        f"Timeout and must be counted once; got "
        f"{_failure_count(REASON_TIMEOUT)!r}"
    )
    assert _failure_count(REASON_HTTP_4XX_NON_429) == EXPECTED_COUNTER_MISS, (
        f"a ConnectionError is a transport stall, not a client error, so "
        f"{REASON_HTTP_4XX_NON_429!r} must stay at {EXPECTED_COUNTER_MISS}; "
        f"got {_failure_count(REASON_HTTP_4XX_NON_429)!r}"
    )


def test_retries_counter_increments_once_per_retry_on_500_exhaustion(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausting N attempts fires the before_sleep callback N-1 times.

    ``nba_retries_total`` is the only per-attempt counter in the transport
    layer: api/nba_client.py line 151 increments it from the tenacity
    ``before_sleep`` callback wired on line 549. A sleep precedes every
    attempt except the first, so the expected value is symbolically
    ``config.RETRY_ATTEMPTS - 1``.

    Mutation detected: removing ``before_sleep=_retry_log_before_sleep`` from
    the decorator (retries would become invisible to operators), or moving the
    increment somewhere that also fires after the final failed attempt, which
    would over-report retries by one on every exhausted request.
    """
    # Arrange -- expectation derived from config, never from a captured run.
    expected_retries = float(config.RETRY_ATTEMPTS - 1)
    mock_get = MagicMock(return_value=_make_response(STATUS_SERVER_ERROR))
    monkeypatch.setattr(client._session, "get", mock_get)

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert
    assert mock_get.call_count == config.RETRY_ATTEMPTS, (
        f"the retry budget must be fully spent before this assertion is "
        f"meaningful: expected config.RETRY_ATTEMPTS="
        f"{config.RETRY_ATTEMPTS} attempts, got {mock_get.call_count}"
    )
    observed_retries = metrics.registry.get_counter_value(
        RETRIES_COUNTER, {"endpoint": ENDPOINT}
    )
    assert observed_retries == expected_retries, (
        f"{RETRIES_COUNTER}{{endpoint={ENDPOINT!r}}} must equal "
        f"config.RETRY_ATTEMPTS - 1 = {expected_retries} (one before_sleep "
        f"per retry, none before the first attempt); got {observed_retries!r}"
    )
