"""Retry-classification contract for ``api.nba_client`` -- the transport stage.

Four behaviours decide whether a failed HTTPS GET is retried and how the
resulting terminal failure is labelled. This module pins all four:

1. The **truth table** of ``api.nba_client._is_transient``: ten rows covering
   all six of its branches -- the transport tuple, ``HTTPError`` at 429, at or
   above 500, at any other status, with no status at all, and every remaining
   exception type. Rows are chosen per BRANCH, not per exception class: the
   closing ``return False`` stands in for every class the predicate does not
   name, so ``ValueError`` represents that family rather than exhausting it.
2. The **exactness of the ``>= 500`` cut**: 500 is retried, 499 is not -- both
   at the predicate level and end-to-end through ``NBAClient.get``.
3. The **permanent-4xx non-retry attempt count**: a 404 issues exactly ONE
   HTTP attempt, not ``config.RETRY_ATTEMPTS`` of them.
4. The **``http_5xx`` and ``http_4xx_non_429`` reason labels** carried by
   ``nba_request_failures_total``.

Why the predicate exists
------------------------
``requests.Response.raise_for_status`` raises ``HTTPError`` uniformly for every
4xx and 5xx status -- it does not raise for 3xx -- so a retry condition of
``retry_if_exception_type(HTTPError)`` would be far too broad: permanent client
errors (400, 401, 403, 404, 418, 422) would be retried, wasting NBA Stats
request budget, masking configuration mistakes behind long exponential-backoff
delays and risking upstream abuse protections. Narrowing the condition with
``_is_transient`` restricts retries to 429 and 5xx, so every other ``HTTPError``
propagates on the first attempt. The headline mutation is widening the predicate
back to every ``HTTPError``: each permanent 404 would then consume
``config.RETRY_ATTEMPTS`` attempts instead of one. Each test below names the
single-statement mutation it detects, because mutation resistance -- not a
coverage percentage -- is the acceptance bar; no coverage instrument ships with
this project and none may be added.

Derivation, tier and isolation
------------------------------
Expected values are derived from the STRUCTURE of ``api/nba_client.py``, never
captured from a run: the ten booleans from the predicate body; the
exactly-one-attempt count from ``retry_if_exception`` plus ``reraise=True`` on
the ``_request`` decorator; the reason labels from the closed taxonomy in
``get()``; and the ``1.0`` failure count from that ``inc`` sitting inside
``get()``'s ``except RequestException`` block -- once per *invocation*, never
once per *attempt*.

No test carries a pytest marker, so all run on the default offline tier, and
none performs network I/O: every request is served by a ``MagicMock`` installed
on the client's session through ``monkeypatch``. The doubles are module-local
rather than imported from a neighbouring test module (which would couple two
modules) or added to the shared ``tests/conftest.py``. Exception classes come
from the ``api.nba_client`` namespace rather than the HTTP library directly,
honouring the ``tests/conftest.py`` do-not list ("Do NOT import
:mod:`requests`" -- Rule 1, Single HTTP Client); they are the *same class
objects*, so production ``isinstance`` checks behave identically. The shared
autouse fixtures reset the correlation id, the metrics registry and the logger
handlers around every test, so each counter delta starts from zero and each test
passes alone, in this module, and in the full suite in any order.
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
# rather than configuration, so no ``config`` constant exists to reference;
# centralising the literal keeps every label set below in lock-step.
ENDPOINT = "leaguedashplayerstats"

# The closed reason taxonomy assigned in ``get()``'s failure branch.
REASON_TIMEOUT = "timeout"
REASON_HTTP_5XX = "http_5xx"
REASON_HTTP_4XX_NON_429 = "http_4xx_non_429"

# Counter names. ``nba_request_failures_total`` is incremented inside ``get()``'s
# ``except RequestException`` block -- ONCE PER get() CALL, never once per retry
# attempt. ``nba_retries_total`` is the only per-attempt counter: it is
# incremented from the tenacity ``before_sleep`` callback.
FAILURES_COUNTER = "nba_request_failures_total"
RETRIES_COUNTER = "nba_retries_total"

# A non-transient failure is not retried at all, so tenacity invokes the
# decorated ``_request`` exactly once: its decorator wires
# ``retry=retry_if_exception(_is_transient)`` together with ``reraise=True``.
EXPECTED_SINGLE_ATTEMPT = 1

# ``MetricsRegistry.get_counter_value`` always returns a Python float, and
# returns 0.0 for a label set that was registered but never incremented -- so
# both comparisons below are exact and meaningful.
EXPECTED_COUNTER_HIT = 1.0
EXPECTED_COUNTER_MISS = 0.0

# Statuses named by the predicate (429 and the ``>= 500`` cut) and by the
# permanent-4xx roll-call in its design comment, from which 400 Bad Request and
# 404 Not Found are exercised.
STATUS_RATE_LIMITED = 429
STATUS_SERVER_ERROR = 500
STATUS_SERVICE_UNAVAILABLE = 503
STATUS_LAST_NON_5XX = 499
STATUS_BAD_REQUEST = 400
STATUS_NOT_FOUND = 404


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(
    status_code: int = 200,
    body: Optional[Dict[str, Any]] = None,
) -> MagicMock:
    """Return a MagicMock mimicking the subset of the HTTP response we touch.

    The mock exposes ``status_code``, ``json()`` returning ``body`` (defaulting
    to an empty ``resultSets`` envelope), and ``raise_for_status()``. For any
    ``status_code >= 400`` -- 4xx and 5xx, the range for which ``requests``
    itself raises -- the ``raise_for_status`` side_effect is an ``HTTPError``
    whose ``.response`` points back at the mock.
    """
    response = MagicMock()
    response.status_code = status_code
    response.json = MagicMock(
        return_value=body if body is not None else {"resultSets": []}
    )
    response.raise_for_status = MagicMock()
    if status_code >= 400:
        error = HTTPError(f"HTTP {status_code}")
        # Load-bearing: both ``_is_transient`` and the reason classifier read
        # ``exc.response.status_code``. Without this attachment the status reads
        # back as ``None``, so every classification degrades to non-transient /
        # ``http_4xx_non_429``. The 5xx tests would then FAIL -- 500 would be
        # attempted once instead of ``config.RETRY_ATTEMPTS`` times, so the
        # ``http_5xx`` label and the retries counter are never reached. Only the
        # already-non-transient subjects (the permanent 4xx family and 499)
        # could still pass, and only vacuously: their single-attempt and
        # catch-all-label expectations hold without the status being read.
        error.response = response
        response.raise_for_status.side_effect = error
    return response


def _http_error_with_status(status_code: int) -> HTTPError:
    """Return a synthetic ``HTTPError`` carrying ``status_code``.

    The pure-predicate tests need no HTTP round-trip -- only the
    ``.response.status_code`` chain that ``_is_transient`` reads.
    """
    error = HTTPError(f"HTTP {status_code}")
    response = MagicMock()
    response.status_code = status_code
    error.response = response
    return error


def _failure_count(reason: str) -> float:
    """Read ``nba_request_failures_total`` for ``ENDPOINT`` and ``reason``.

    The label dict is spelled out exactly as the production ``inc`` emits it, so
    every call site asserts the real, complete label set rather than a subset.
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

    ``time.sleep`` is the patch that does the work. The ``@retry`` decorator
    builds its tenacity controller when ``NBAClient`` is defined, and that
    controller keeps a direct reference to the ``tenacity.nap.sleep`` function
    object captured at that moment, so rebinding the module attribute afterwards
    cannot reach it. The retained function resolves ``time.sleep`` at CALL time,
    which is what makes the nap instant here. The second ``setattr`` covers a
    caller that looks ``tenacity.nap.sleep`` up at call time; it is NOT a second
    safeguard for the already-built controller and must not be relied on as one.
    Without the ``time.sleep`` patch the decorator's ``wait_exponential``
    parameters would be honoured for real and each exhaustion test would cost
    seconds of wall clock.
    """
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("tenacity.nap.sleep", lambda _seconds: None)


@pytest.fixture
def mock_rate_limiter() -> MagicMock:
    """Return a MagicMock standing in for utils.rate_limiter.RateLimiter.

    Mocked so no real sleeping happens on the critical path; ``wait`` is a
    ``MagicMock`` so call counts stay inspectable.
    """
    limiter = MagicMock()
    limiter.wait = MagicMock(return_value=None)
    limiter.interval = 1.0  # production __init__ logs rate_limiter.interval
    return limiter


@pytest.fixture
def client(mock_rate_limiter: MagicMock) -> NBAClient:
    """Default NBAClient with an injected mocked rate_limiter.

    ``NBAClient.__init__`` is keyword-only, so the collaborator is passed by
    name. Logger and metrics default to the production singletons, which the
    autouse fixtures in ``conftest.py`` reset between tests.
    """
    return NBAClient(rate_limiter=mock_rate_limiter)


# ---------------------------------------------------------------------------
# _is_transient truth table
# ---------------------------------------------------------------------------
# The six branches of the predicate, transcribed from its body -- NOT captured
# by executing it:
#
#   isinstance(exc, (Timeout, ConnectionError))   -> True
#   HTTPError, status == 429                      -> True
#   HTTPError, status >= 500                      -> True
#   HTTPError, any other status                   -> False
#   HTTPError, status is None                     -> False
#   anything else (e.g. ValueError)               -> False
#
# Laid out as an explicit (exception, expected) table so the whole contract is
# visible at a glance and checkable against those six branches.
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

    Mutation detected: any edit to the predicate body -- widening the transient
    set to every ``HTTPError`` (the headline mutation: permanent 404s would be
    retried ``config.RETRY_ATTEMPTS`` times), dropping ``Timeout`` or
    ``ConnectionError`` from the transport tuple, returning the bare ``status``
    instead of the comparison (a truthy non-``bool``), or deleting the closing
    ``return False`` default.
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

    Mutation detected: changing ``status >= 500`` to ``> 500``, which would stop
    retrying the single most common server error; or to ``>= 499``, which would
    start retrying a permanent client error. Dropping the ``status is not None``
    guard from that same expression is caught instead by the
    ``http_error_without_response`` row of the truth table, where
    ``None >= 500`` raises ``TypeError`` rather than returning ``False``.
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

    This observes the predicate-level boundary through the real retry loop,
    which is where a mutation would actually bite.

    Mutation detected: narrowing the 5xx branch so a server error is treated as
    permanent -- the attempt count collapses from ``config.RETRY_ATTEMPTS`` to 1
    and a recoverable upstream blip surfaces as a hard failure. Removing
    ``reraise=True`` from the decorator is caught too: the caller would see
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

    Mutation detected: relaxing the cut to ``>= 499`` -- the attempt count would
    jump from 1 to ``config.RETRY_ATTEMPTS``, so a permanent client error would
    burn ``config.RETRY_ATTEMPTS - 1`` extra requests from the upstream budget.
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

    The headline case. ``retry_if_exception(_is_transient)`` combined with
    ``reraise=True`` on the ``_request`` decorator means a ``False`` verdict
    short-circuits the loop and the *original* exception surfaces.

    Mutation detected: widening ``_is_transient`` to accept every ``HTTPError``
    -- a permanent 404 would then be retried ``config.RETRY_ATTEMPTS`` times,
    wasting NBA Stats API budget, masking the configuration error behind
    exponential-backoff delays and risking the upstream abuse protections the
    predicate exists to avoid.
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


# ---------------------------------------------------------------------------
# Failure reason labels
# ---------------------------------------------------------------------------


def test_http_5xx_reason_label_counted_once_per_call_not_once_per_attempt(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 500 exhaustion emits reason="http_5xx" once per call, not per attempt.

    ``config.RETRY_ATTEMPTS`` HTTP attempts happen, yet
    ``nba_request_failures_total`` gains ``1.0`` -- because that ``inc`` lives in
    ``get()``'s ``except RequestException`` block, outside the retried
    ``_request``.

    Mutation detected: moving that ``inc`` inside the retried ``_request`` so it
    fires per attempt -- one failed invocation would be reported as
    ``config.RETRY_ATTEMPTS`` failures, inflating every dashboard rate by that
    factor -- or mis-routing the 5xx branch of the reason classifier.
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


def test_http_4xx_non_429_reason_label_recorded_for_permanent_404(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanent 404 is counted once under reason="http_4xx_non_429".

    Mutation detected: collapsing the closed reason taxonomy in ``get()`` to a
    single label, or letting a permanent 4xx fall into the ``http_5xx`` bucket.
    Either change would put the exposed series at odds with the metric catalogue
    in ``docs/OBSERVABILITY.md``, which binds ``nba_request_failures_total`` to
    an ``endpoint`` label plus exactly this ``reason`` set (``timeout``,
    ``http_5xx``, ``http_4xx_non_429``) -- so a permanent client error would be
    attributed to the upstream server instead of to the caller.
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


def test_retries_counter_increments_once_per_retry_on_500_exhaustion(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausting N attempts fires the before_sleep callback N-1 times.

    ``nba_retries_total`` is the transport layer's only per-attempt counter,
    incremented from the tenacity ``before_sleep`` callback wired into the
    ``_request`` decorator. A sleep precedes every attempt except the first, so
    the expected value is symbolically ``config.RETRY_ATTEMPTS - 1``.

    Mutation detected: removing ``before_sleep=_retry_log_before_sleep`` from the
    decorator (retries become invisible to operators), or moving the increment
    somewhere that also fires after the final failed attempt, which over-reports
    retries by one on every exhausted request.
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
