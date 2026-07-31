"""Retry-classification contract for ``api.nba_client`` -- the transport stage.

Five behaviours decide whether a failed HTTPS GET is retried, how the resulting
terminal failure is labelled, and where the evidence of it is written. This
module pins all five:

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
5. The **destination and schema of the failure log records**: one WARNING per
   retry plus one ERROR per exhausted request, each carrying the exact fields of
   its production format string, and all of them written to a ``tmp_path`` file
   rather than appended to the operator's durable ``logs/pipeline.log``.

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

Isolation extends to the durable log sink, which handler resetting alone cannot
provide: the ``client`` fixture depends on ``tmp_log_dir`` so ``config.LOG_FILE``
is already redirected into ``tmp_path`` when the first ``get_logger`` call
configures logging. Fabricated 500s and 404s therefore leave no trace in the
operator's ``logs/pipeline.log`` and cause none of its rotation.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
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

# Logging destination and schema. The transport layer has TWO writers on the
# failure path and both resolve the same logger name: the instance logger built
# in ``NBAClient.__init__`` and the one ``_retry_log_before_sleep`` acquires for
# itself. ``config.LOG_FORMAT`` --
# ``"%(asctime)s %(levelname)s corr=%(correlation_id)s %(name)s %(message)s"``
# -- renders that name as the fourth whitespace-delimited field of every line,
# which is what makes the parser below able to select exactly these records.
LOGGER_NAME = "nba_client"
LEVEL_WARNING = "WARNING"
LEVEL_ERROR = "ERROR"

# The two format strings those writers use, transcribed from the production
# source (api/nba_client.py: ``_retry_log_before_sleep`` and the
# ``except RequestException`` block of ``NBAClient.get``) rather than captured
# from a run. ``%``-formatting them here reproduces exactly what ``logging``
# does when it renders the record.
RETRY_LOG_TEMPLATE = (
    "NBAClient retrying endpoint=%s attempt=%s exc_class=%s status=%s"
)
EXHAUSTION_LOG_TEMPLATE = (
    "NBAClient request exhausted retries endpoint=%s reason=%s"
)

# ``type(exc).__name__`` for the error ``_make_response`` attaches to
# ``raise_for_status``, i.e. the ``HTTPError`` imported at the top of this
# module.
EXPECTED_EXC_CLASS = "HTTPError"

# The directory and file names ``tests/conftest.py::tmp_log_dir`` creates under
# ``tmp_path`` and points ``config.LOG_DIR`` / ``config.LOG_FILE`` at.
TMP_LOG_DIR_NAME = "logs"
TMP_LOG_FILE_NAME = "pipeline.log"


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


def _nba_client_log_records(log_text: str) -> List[Tuple[str, str]]:
    """Return the ``(levelname, message)`` of each ``nba_client`` record, in order.

    ``config.LOG_FORMAT`` is
    ``"%(asctime)s %(levelname)s corr=%(correlation_id)s %(name)s %(message)s"``
    and none of the first four fields can contain a space --
    ``config.LOG_DATE_FORMAT`` is ``"%Y-%m-%dT%H:%M:%S"``, the level and logger
    names are single tokens and the correlation id is a hex token -- so a
    four-way split isolates the message exactly. Records emitted by any other
    logger are dropped, so the returned list is the transport layer's own
    output and can be compared for full equality.
    """
    records: List[Tuple[str, str]] = []
    for line in log_text.splitlines():
        fields = line.split(" ", 4)
        if len(fields) == 5 and fields[3] == LOGGER_NAME:
            records.append((fields[1], fields[4]))
    return records


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
def client(mock_rate_limiter: MagicMock, tmp_log_dir: Path) -> NBAClient:
    """Default NBAClient with an injected mocked rate_limiter, logging to tmp.

    ``NBAClient.__init__`` is keyword-only, so the collaborator is passed by
    name. Metrics default to the production singleton, which the autouse
    fixtures in ``conftest.py`` reset between tests.

    Why ``tmp_log_dir`` is a dependency of this fixture
    ---------------------------------------------------
    Every retry driven below is a *synthetic* upstream failure, and each one
    makes the transport layer emit a real WARNING (per retry) and a real ERROR
    (on exhaustion). ``utils.logger._configure`` attaches a
    ``RotatingFileHandler`` bound to ``config.LOG_FILE``, so without a redirect
    those synthetic records would be appended to the operator's own
    ``logs/pipeline.log`` -- polluting a durable forensic artifact with
    fabricated incidents and driving avoidable rotation of it. The autouse
    ``_reset_logger_handlers_between_tests`` fixture detaches handlers around
    each test but does NOT relocate the sink, so it cannot prevent that on its
    own. ``tmp_log_dir`` monkeypatches ``config.LOG_DIR`` and ``config.LOG_FILE``
    to ``tmp_path``; because pytest builds a fixture's dependencies BEFORE its
    body runs, the redirect is in place before ``NBAClient(...)`` triggers the
    first ``get_logger`` call, and the reset fixture's ``_configured = False``
    guarantees ``_configure`` re-runs against the temporary path.

    Injecting a per-instance ``logger=`` would NOT be sufficient: the tenacity
    ``before_sleep`` callback ``api.nba_client._retry_log_before_sleep`` is a
    module-level function with no access to ``self``, so it calls
    ``get_logger("nba_client")`` itself and would resolve the production sink
    regardless of what this instance holds. Redirecting the configured
    destination is therefore the only fix that covers both writers.
    ``test_retry_logging_is_confined_to_the_temporary_log_file`` pins this, so
    dropping the dependency fails the module rather than silently resuming the
    pollution.
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


# ---------------------------------------------------------------------------
# Log-destination isolation
# ---------------------------------------------------------------------------


def test_retry_logging_is_confined_to_the_temporary_log_file(
    client: NBAClient,
    fast_tenacity_sleep: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Every synthetic retry record lands in tmp_path, not in the real log file.

    The retries driven throughout this module are fabricated upstream failures,
    and each one makes the transport layer emit durable records: a WARNING per
    retry from ``_retry_log_before_sleep`` and an ERROR per exhausted request
    from ``NBAClient.get``. ``utils.logger._configure`` binds a
    ``RotatingFileHandler`` to ``config.LOG_FILE``, so unless that destination is
    redirected those records are appended to the operator's own
    ``logs/pipeline.log`` -- a durable forensic artifact that would then contain
    invented incidents and be rotated for no operational reason. The ``client``
    fixture prevents this by depending on ``tmp_log_dir``; this test is what
    makes that dependency enforced rather than merely intended.

    Mutation detected: dropping ``tmp_log_dir`` from the ``client`` fixture --
    ``config.LOG_FILE`` reverts to the production path, which assertion (1)
    reports directly; assertions (2) and (3) independently encode the same
    property, one on the attached handler and one on the file contents.
    Assertion (3) additionally catches a renamed field in either format string,
    a changed level, and a retry WARNING emitted after the final attempt,
    because it compares the complete ordered record list rather than a
    substring.
    """
    # Arrange -- the destination is derived from the fixture chain rather than
    # read back from the code under test: tmp_log_dir creates
    # ``tmp_path / "logs"`` and repoints config.LOG_DIR / config.LOG_FILE at it.
    expected_log_file = tmp_path / TMP_LOG_DIR_NAME / TMP_LOG_FILE_NAME
    mock_get = MagicMock(return_value=_make_response(STATUS_SERVER_ERROR))
    monkeypatch.setattr(client._session, "get", mock_get)

    # Hand-derived from the two production format strings. ``before_sleep`` runs
    # after every failed attempt except the last, and ``attempt_number`` is the
    # number of the attempt that just failed, so the WARNING attempt numbers are
    # 1 .. config.RETRY_ATTEMPTS - 1 (1, 2, 3, 4 at the shipped value of 5).
    # The single ERROR follows from get()'s ``except RequestException`` block
    # once the budget is spent, labelled ``http_5xx`` because 500 >= 500.
    expected_records: List[Tuple[str, str]] = [
        (
            LEVEL_WARNING,
            RETRY_LOG_TEMPLATE
            % (ENDPOINT, attempt, EXPECTED_EXC_CLASS, STATUS_SERVER_ERROR),
        )
        for attempt in range(1, config.RETRY_ATTEMPTS)
    ]
    expected_records.append(
        (LEVEL_ERROR, EXHAUSTION_LOG_TEMPLATE % (ENDPOINT, REASON_HTTP_5XX))
    )

    # Act
    with pytest.raises(HTTPError):
        client.get(ENDPOINT, {})

    # Assert (0) -- the retry budget really was spent, so the record list below
    # is compared against a fully exercised failure rather than a short one.
    assert mock_get.call_count == config.RETRY_ATTEMPTS, (
        f"this test only observes the full logging sequence when every attempt "
        f"is spent: expected config.RETRY_ATTEMPTS={config.RETRY_ATTEMPTS} "
        f"attempts, got {mock_get.call_count}"
    )

    # Assert (1) -- the configured sink is the temporary one.
    assert config.LOG_FILE == expected_log_file, (
        f"config.LOG_FILE must be redirected into tmp_path before the client "
        f"is built (the ``client`` fixture depends on tmp_log_dir for exactly "
        f"this reason); expected {expected_log_file}, got {config.LOG_FILE}"
    )

    # Assert (2) -- and no durable handler writes anywhere else. Exactly one
    # RotatingFileHandler is attached by ``utils.logger._configure``; the
    # console StreamHandler it also attaches is not a FileHandler and pytest's
    # own capture handlers are not either, so this list is the complete set of
    # files the logging tree can touch. (An explicit ``--log-file`` invocation
    # would legitimately add one and is not part of the project's run command.)
    file_destinations = [
        handler.baseFilename
        for handler in logging.getLogger().handlers
        if isinstance(handler, logging.FileHandler)
    ]
    assert file_destinations == [str(expected_log_file)], (
        f"the only durable log sink during this test must be "
        f"{str(expected_log_file)!r}; got {file_destinations!r} -- any other "
        f"entry is a real file being written by synthetic failures"
    )

    # Assert (3) -- and that file holds exactly the expected sequence, proving
    # BOTH writers were redirected: the module-level before_sleep callback
    # (which resolves its own logger, so a per-instance ``logger=`` injection
    # could not have covered it) and the instance logger inside get().
    observed_records = _nba_client_log_records(
        expected_log_file.read_text(encoding="utf-8")
    )
    assert observed_records == expected_records, (
        f"the temporary log must hold config.RETRY_ATTEMPTS - 1 = "
        f"{config.RETRY_ATTEMPTS - 1} retry WARNINGs followed by one "
        f"exhaustion ERROR, with the exact field names and values of the "
        f"production format strings; expected {expected_records!r}, got "
        f"{observed_records!r}"
    )
