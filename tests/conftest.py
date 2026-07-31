"""Shared pytest fixtures for the NBA Data Ingestion Pipeline test suite.

This module is the single source of shared fixtures across
``tests/unit/``, ``tests/integration/``, and ``tests/invariants/``.

Exposed fixture and helper categories
-------------------------------------
* **Project-root discovery** — ``PROJECT_ROOT`` / ``project_root`` /
  ``production_python_files`` so invariant tests can enumerate
  production source files for grep-based Rule 1 and Rule 7 checks.
* **Filesystem isolation** — ``tmp_output_dir``, ``tmp_log_dir``, and
  ``isolated_filesystem`` redirect :mod:`config` paths to pytest's
  ``tmp_path`` via :meth:`monkeypatch.setattr` so tests never touch
  the operator's real working directory.
* **`resultSets` envelope payloads** — ``sample_*_payload`` fixtures
  covering the happy path, multi-table responses, empty ``rowSet``,
  nested-cell violations, row-length mismatches, the singular
  ``resultSet`` shape, and the missing-key pathological case. Exercise
  ``utils/schema_normalizer.py`` for Rule 4.
* **Flat DataFrame fixtures** — ``flat_df``, ``nested_df``,
  ``list_cell_df``, ``empty_df``, and the reproducible ``large_df`` for
  writer round-trip and pipeline tests.
* **Mock collaborators** — :class:`RecordingClient`,
  :class:`RecordingWriter`, :class:`RecordingCheckpoint` are handwritten
  spies (not :class:`~unittest.mock.MagicMock`) so interface drift is
  caught at instantiation time rather than masked by attribute-access
  magic. Factory fixtures return fresh instances per test.
* **Deterministic clock** — :class:`FakeClock` + ``fake_clock`` fixture
  monkeypatches ``time.monotonic`` and ``time.sleep`` so rate-limiter
  tests (Rule 2) run instantly.
* **CLI harness** — ``cli_runner`` returns a fresh
  :class:`click.testing.CliRunner` per test for Gate 13 verification
  of ``run.py`` subcommand dispatch.
* **Autouse state resets** — ``_reset_correlation_id_between_tests``,
  ``_reset_metrics_registry_between_tests``, and
  ``_reset_logger_handlers_between_tests`` run before AND after every
  test so correlation-ID context, metrics registry, and logger
  handlers never leak across tests.
* **CSV round-trip helper** — ``read_csv_as_df`` function and
  ``csv_reader`` fixture for verifying ``CSVWriter``-produced files.

Authoritative references
------------------------
AAP §0.2.3, §0.4.1.2, §0.5.1.8, product brief §5 Rules 1–7,
Validation Gates 1, 2, 8, 9, 10, 12, 13.

Do-not list
-----------
* Do NOT import :mod:`requests` — Rule 1 (Single HTTP Client).
* Do NOT call :meth:`pandas.DataFrame.to_csv` — Rule 7 (Pluggable
  Storage); use ``CSVWriter`` or a per-test temporary file.
* Do NOT register module-level pytest hooks
  (e.g. ``pytest_collection_modifyitems``) — markers belong in
  ``pytest.ini`` and hooks belong in a dedicated hooks module.
* Do NOT introduce Faker, hypothesis, pytest-mock, pytest-cov, or any
  third-party test library outside AAP §0.3.1.
"""

from __future__ import annotations

import json  # noqa: F401 - re-exported for fixture authors constructing JSON blobs
import logging  # noqa: F401 - documents stdlib-only F-008 commitment; available for typing
import sys
import time
from contextlib import contextmanager  # noqa: F401 - re-exported for fixture authors
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock  # noqa: F401 - re-exported for ad-hoc test authors

import pandas as pd
import pytest
from click.testing import CliRunner

# ---------------------------------------------------------------------------
# Project-root discovery and ``sys.path`` bootstrap
# ---------------------------------------------------------------------------

#: Absolute path of the repository root (the directory that contains
#: ``run.py``, ``config.py``, and the top-level ``api/``, ``endpoints/``,
#: ``pipelines/``, ``storage/``, ``utils/`` packages). Computed once at
#: module-import time via :meth:`Path.resolve` so the value is stable
#: regardless of the pytest invocation directory.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

# Defensive ``sys.path`` insertion so ``import config``, ``import api``,
# etc. resolve from the repository root even when pytest is invoked from
# a subdirectory. ``pytest.ini`` ``testpaths`` usually handles this, but
# the explicit insertion keeps the import contract robust against
# unusual invocation patterns (e.g. ``cd tests && pytest``).
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


#: Production package directories enumerated by
#: ``production_python_files``; consumed by invariant tests that grep
#: every ``.py`` file to enforce Rule 1 (sole HTTP client) and Rule 7
#: (sole ``to_csv`` call-site).
PRODUCTION_DIRS: Tuple[str, ...] = (
    "api",
    "endpoints",
    "pipelines",
    "storage",
    "utils",
)

#: Root-level production Python files (outside the package directories).
PRODUCTION_ROOT_FILES: Tuple[str, ...] = (
    "run.py",
    "config.py",
)


# ---------------------------------------------------------------------------
# CSV round-trip helper (module-level function)
# ---------------------------------------------------------------------------


def read_csv_as_df(path: Path) -> pd.DataFrame:
    """Read a CSV file produced by ``CSVWriter.write`` back into a DataFrame.

    Tests call this in place of ``pandas.read_csv`` so the intent
    ("round-trip verification") is explicit, and so any future change
    to ``CSVWriter``'s encoding or delimiter can be mirrored here in
    one place.

    Parameters
    ----------
    path:
        Filesystem location of the CSV artifact to read.

    Returns
    -------
    pandas.DataFrame
        The parsed DataFrame with UTF-8 decoded cells.
    """
    return pd.read_csv(path, encoding="utf-8")


# ---------------------------------------------------------------------------
# Handwritten spy classes (NOT ``MagicMock`` — see module docstring)
# ---------------------------------------------------------------------------


class FakeClock:
    """Controllable monotonic clock and sleep sink for rate-limiter tests.

    Replaces ``time.monotonic`` and ``time.sleep`` in the :pyfixture:`fake_clock`
    pytest fixture. Every call to :meth:`sleep` is recorded on
    :attr:`sleeps` and advances :attr:`now` by the requested duration
    (clamped at zero so negative durations do not rewind the clock).
    :meth:`advance` lets tests simulate the passage of real time between
    requests without invoking :meth:`sleep`.

    Attributes
    ----------
    now:
        Current fake monotonic time in seconds.
    sleeps:
        Ordered list of every ``sleep(duration)`` call's ``duration``
        argument, allowing tests to assert both whether ``sleep`` was
        called and how long each call waited for.
    """

    def __init__(self, start: float = 1000.0) -> None:
        self.now: float = float(start)
        self.sleeps: List[float] = []

    def monotonic(self) -> float:
        """Return the current fake time — drop-in replacement for ``time.monotonic``."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Record a ``sleep`` call and advance the clock.

        Durations ≤ 0 are recorded but do not rewind the clock. The
        clamp mirrors the production ``time.sleep`` behavior where a
        negative duration is a no-op rather than a time-travel event.
        """
        duration = float(seconds)
        self.sleeps.append(duration)
        if duration > 0:
            self.now += duration

    def advance(self, seconds: float) -> None:
        """Advance the clock by ``seconds`` without appending to :attr:`sleeps`.

        Raises
        ------
        ValueError
            If ``seconds`` is negative — the rate-limiter invariant
            (Rule 2) relies on a monotonically non-decreasing clock.
        """
        delta = float(seconds)
        if delta < 0:
            raise ValueError(
                "FakeClock.advance() requires a non-negative delta; "
                f"got {delta!r}"
            )
        self.now += delta


class RecordingClient:
    """Spy-style stand-in for :class:`api.nba_client.NBAClient`.

    Records every ``(endpoint, params)`` tuple on :attr:`calls`.
    Returns responses from :attr:`responses` keyed by endpoint name,
    or a minimal 1×1 default envelope for unmapped endpoints so that
    pipeline tests do not trip on unknown endpoint lookups.

    Configure :attr:`raise_for` with a mapping of ``endpoint`` →
    :class:`BaseException` instance to make :meth:`get` raise for that
    endpoint. This is the primary mechanism used by the Rule 6
    fail-safe games-iteration canary test
    (``tests/unit/pipelines/test_ingest_games.py``).

    Attributes
    ----------
    calls:
        Ordered list of ``(endpoint, params)`` tuples recorded by every
        :meth:`get` invocation. ``params`` is a shallow copy, so tests
        can inspect the exact dict passed in without worrying about
        subsequent mutation by the caller.
    responses:
        Mapping of endpoint-name → response-envelope dict. Tests seed
        this dict with the payload fixtures
        (``sample_single_table_payload``, ``sample_schedule_payload``,
        …) for each endpoint the system under test is expected to call.
    raise_for:
        Mapping of endpoint-name → exception instance. When
        :meth:`get` is called with a matching endpoint, the exception
        is raised instead of a response being returned. Used to
        verify retry/fail-safe behavior without hitting the live API.
    """

    def __init__(
        self,
        responses: Optional[Dict[str, Any]] = None,
        raise_for: Optional[Dict[str, BaseException]] = None,
    ) -> None:
        self.responses: Dict[str, Any] = dict(responses or {})
        self.raise_for: Dict[str, BaseException] = dict(raise_for or {})
        self.calls: List[Tuple[str, Dict[str, Any]]] = []

    def get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Stand-in for ``NBAClient.get`` — records and returns or raises.

        Mirrors the production signature so that swapping a
        ``RecordingClient`` for a real ``NBAClient`` is purely a
        constructor-level substitution (AAP §0.4.1.2 explicit
        dependency-injection contract).
        """
        # ``dict(params)`` defensively snapshots the parameter map so
        # that later mutation by the caller does not retroactively
        # change what was "recorded" here.
        self.calls.append((str(endpoint), dict(params or {})))
        if endpoint in self.raise_for:
            raise self.raise_for[endpoint]
        if endpoint in self.responses:
            return self.responses[endpoint]
        # Fallback: minimal flat envelope so pipeline tests that iterate
        # through a list of endpoint names do not trip on an unmapped
        # endpoint. Tests that want strict endpoint coverage should
        # assert ``client.calls`` directly.
        return {
            "resultSets": [
                {
                    "name": str(endpoint),
                    "headers": ["A"],
                    "rowSet": [[1]],
                }
            ]
        }

    def reset(self) -> None:
        """Clear :attr:`calls` without disturbing :attr:`responses` or
        :attr:`raise_for`. Useful between phases of a single test."""
        self.calls.clear()

    def assert_called_with_endpoint(self, endpoint: str) -> None:
        """Raise ``AssertionError`` unless ``endpoint`` was called at least once.

        The error message includes the full list of observed endpoint
        names so a failing assertion pinpoints exactly what was missed.
        """
        observed = [c[0] for c in self.calls]
        if endpoint not in observed:
            raise AssertionError(
                f"expected RecordingClient.get() to have been called "
                f"with endpoint={endpoint!r}; recorded endpoints: "
                f"{observed!r}"
            )


class RecordingWriter:
    """Spy-style stand-in for :class:`storage.csv_writer.CSVWriter`.

    Does NOT touch the filesystem for the payload itself — the recorded
    DataFrame is a shallow ``.copy()`` so subsequent mutation by the
    pipeline under test does not retroactively change the recorded
    value. A :attr:`output_dir` directory IS created on disk so the
    returned ``Path`` values point at a real (but empty) location that
    downstream assertions can stat without ``FileNotFoundError``.

    When :attr:`raise_on` equals the ``name`` argument, :meth:`write`
    raises :class:`RuntimeError` — used by negative tests that verify
    the pipeline does NOT call ``CheckpointManager.mark_completed``
    after a write failure (Rule 5 correctness guard).

    Attributes
    ----------
    output_dir:
        Temporary directory where synthetic :class:`~pathlib.Path`
        values are rooted. Always an existing directory.
    writes:
        Ordered list of write records. Each entry is a dict with keys
        ``df`` (snapshot DataFrame), ``name`` (str), ``season`` (str),
        and ``rows`` (int length of the DataFrame at write time).
    raise_on:
        If not ``None``, the ``name`` argument that triggers a
        :class:`RuntimeError`; all other writes are recorded normally.
    """

    def __init__(
        self,
        output_dir: Path,
        raise_on: Optional[str] = None,
    ) -> None:
        self.output_dir: Path = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.writes: List[Dict[str, Any]] = []
        self.raise_on: Optional[str] = raise_on

    def write(self, df: pd.DataFrame, name: str, season: str) -> Path:
        """Record the write call (or raise) and return a synthetic path.

        Mirrors the production ``BaseWriter.write`` signature (AAP
        §0.4.1.1) so ``RecordingWriter`` is a drop-in substitute in
        pipeline unit tests.
        """
        if self.raise_on is not None and name == self.raise_on:
            raise RuntimeError(f"synthetic write failure for {name!r}")
        # Snapshot the DataFrame so later mutation by the pipeline
        # under test does not retroactively change what was recorded.
        self.writes.append(
            {
                "df": df.copy() if df is not None else None,
                "name": str(name),
                "season": str(season),
                "rows": int(len(df)) if df is not None else 0,
            }
        )
        return self.output_dir / f"{name}.csv"


class RecordingCheckpoint:
    """Spy-style stand-in for :class:`utils.checkpoint.CheckpointManager`.

    In-memory only — no disk I/O — so tests run at memory speed and
    filesystem isolation (Rule 5's on-disk manifest) is orthogonal to
    pipeline logic assertions.

    Every ``is_completed``, ``mark_completed``, and ``get_pending``
    invocation is recorded so tests can assert both the arguments AND
    the call order — critical for Rule 5 correctness, which requires
    ``mark_completed`` to be called AFTER a successful write (not
    before).

    Attributes
    ----------
    marks:
        Ordered list of ``(domain, key)`` tuples recorded by every
        :meth:`mark_completed` call.
    checks:
        Ordered list of ``(domain, key)`` tuples recorded by every
        :meth:`is_completed` call.
    pendings:
        Ordered list of ``(domain, all_keys)`` tuples recorded by every
        :meth:`get_pending` call; ``all_keys`` is stored as a
        :class:`tuple` so the record is hashable and immutable.
    """

    def __init__(
        self,
        completed: Optional[Dict[str, Iterable[str]]] = None,
    ) -> None:
        self._completed: Dict[str, set] = {
            str(domain): {str(k) for k in keys}
            for domain, keys in (completed or {}).items()
        }
        self.marks: List[Tuple[str, str]] = []
        self.checks: List[Tuple[str, str]] = []
        self.pendings: List[Tuple[str, Tuple[str, ...]]] = []

    def is_completed(self, domain: str, key: str) -> bool:
        """Return whether ``(domain, key)`` has been recorded as completed."""
        d, k = str(domain), str(key)
        self.checks.append((d, k))
        return k in self._completed.get(d, set())

    def mark_completed(self, domain: str, key: str) -> None:
        """Record ``(domain, key)`` as completed.

        Mirrors the production contract: calls are synchronous and
        durable-equivalent (for in-memory testing). Production code
        asserts ``mark_completed`` is called immediately after a
        successful ``CSVWriter.write`` (Rule 5).
        """
        d, k = str(domain), str(key)
        self.marks.append((d, k))
        self._completed.setdefault(d, set()).add(k)

    def get_pending(self, domain: str, all_keys: Iterable[str]) -> List[str]:
        """Return the subset of ``all_keys`` not yet marked completed for ``domain``."""
        d = str(domain)
        keys_tuple: Tuple[str, ...] = tuple(str(k) for k in all_keys)
        self.pendings.append((d, keys_tuple))
        done = self._completed.get(d, set())
        return [k for k in keys_tuple if k not in done]


# ---------------------------------------------------------------------------
# Session-scoped discovery fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Absolute path of the repository root.

    Session-scoped because the project root does not change during a
    pytest run. Consumed by invariant tests that shell out to
    ``subprocess.run(["grep", "-rn", ...])`` against production
    directories (Rule 1, Rule 7 enforcement).
    """
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def production_python_files(project_root: Path) -> List[Path]:
    """All production ``.py`` files (excluding ``tests/``) as absolute paths.

    Session-scoped because the production file set is stable during a
    pytest run. Returns an empty list if a production directory has not
    yet been created (e.g. when tests run against a partially-built
    repository), so that invariant tests degrade gracefully rather than
    error out at collection time.
    """
    files: List[Path] = []
    for root_file in PRODUCTION_ROOT_FILES:
        candidate = project_root / root_file
        if candidate.exists():
            files.append(candidate)
    for subdir in PRODUCTION_DIRS:
        folder = project_root / subdir
        if not folder.exists():
            continue
        files.extend(sorted(folder.rglob("*.py")))
    return files


@pytest.fixture(scope="session")
def checkpoint_keys() -> Dict[str, str]:
    """Canonical checkpoint-key format strings, one per domain.

    Each value is a Python ``str.format``-style template that tests can
    materialize with ``.format(season=...)`` to produce the exact
    ``(domain, key)`` tuple the production pipelines will pass to
    :meth:`CheckpointManager.mark_completed`. Centralizing the keys
    here prevents test files from duplicating the string literals and
    drifting when the production format evolves.
    """
    return {
        "schedule": "leaguegamefinder:{season}",
        "teams": "leaguedashteamstats:{season}",
        "lineups": "leaguedashlineups:{season}",
        "players_primary": "leaguedashplayerstats:{season}",
        "players_tracking": "leaguedashptstats:{season}",
    }


# ---------------------------------------------------------------------------
# Autouse project-wide state resets
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_correlation_id_between_tests() -> Iterator[None]:
    """Reset :data:`utils.correlation.correlation_id` to the empty string.

    Runs **before AND after** every test so a correlation ID minted by
    one test does not leak into the next. Uses the raw
    ``ContextVar.set("")`` setter (as opposed to
    :func:`utils.correlation.set_correlation_id`, which auto-mints a
    fresh UUID when called with an empty string) so that tests start
    from a truly empty context and can assert the absence of a
    correlation ID in log records.

    Lazily imports :mod:`utils.correlation` with an ``ImportError``
    guard so test collection does not break when the utils module is
    not yet present (early-stage development) — the fixture still
    ``yield``s so other autouse fixtures and the test body run
    normally.
    """
    try:
        from utils import correlation as correlation_module
    except ImportError:
        yield
        return
    correlation_module.correlation_id.set("")
    try:
        yield
    finally:
        correlation_module.correlation_id.set("")


@pytest.fixture(autouse=True)
def _reset_metrics_registry_between_tests() -> Iterator[None]:
    """Clear every counter and histogram in :data:`utils.metrics.registry`.

    Metric counters are a shared mutable singleton — one test's
    ``inc("nba_requests_total")`` would otherwise pollute a subsequent
    test that asserts the counter is zero. Lazily imports the registry
    with an ``ImportError`` guard so the fixture degrades gracefully
    when ``utils.metrics`` has not yet been implemented.
    """
    try:
        from utils.metrics import registry
    except ImportError:
        yield
        return
    registry.reset()
    try:
        yield
    finally:
        registry.reset()


@pytest.fixture(autouse=True)
def _reset_logger_handlers_between_tests() -> Iterator[None]:
    """Tear down stdout and RotatingFileHandler instances between tests.

    Invokes :func:`utils.logger._reset_for_tests` which detaches and
    closes every handler on the root logger and flips ``_configured``
    back to ``False`` so the next :func:`get_logger` call re-runs
    configuration against whatever :data:`config.LOG_FILE` value is
    active at that moment. This is what makes :pyfixture:`tmp_log_dir`
    reconfiguration actually take effect — without this reset, the
    RotatingFileHandler from a prior test would continue writing to
    the (now-deleted) ``tmp_path`` directory from the prior test.

    Lazily imports :mod:`utils.logger` with an ``ImportError`` guard
    so early-stage test collection does not fail when the utils module
    is not yet implemented.

    Note on caplog interaction
    --------------------------
    pytest's ``caplog`` fixture manages its own log-capture handler
    via the ``_pytest.logging`` plugin. That plugin attaches and
    detaches its handler through pytest's own per-test hooks, not
    through the autouse fixture contract, so calling
    ``_reset_for_tests()`` here is safe in the context of
    non-caplog-using tests (which is the current contract — no tests
    under ``tests/unit/`` consume caplog). Tests that need caplog in
    the future can override this fixture locally or mark with
    ``pytest.mark.usefixtures`` to sequence the resets.
    """
    try:
        from utils import logger as logger_module
    except ImportError:
        yield
        return
    logger_module._reset_for_tests()
    try:
        yield
    finally:
        logger_module._reset_for_tests()


# ---------------------------------------------------------------------------
# Filesystem-isolation fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_output_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect :data:`config.OUTPUT_DIR` and :data:`config.CHECKPOINT_PATH` to ``tmp_path``.

    Creates ``tmp_path / "output"`` on disk and monkeypatches both
    ``config.OUTPUT_DIR`` and ``config.CHECKPOINT_PATH`` to point under
    it. ``raising=True`` (the default) ensures that if a future
    refactor renames either attribute, the monkeypatch fails fast
    rather than silently creating a new attribute on :mod:`config`.

    Lazily imports :mod:`config` so this fixture remains collectible
    even when ``config.py`` has not yet been implemented — the
    ``ImportError`` surfaces at fixture-request time with a clear
    traceback pointing at the consuming test.

    Yields
    ------
    pathlib.Path
        The concrete output directory under ``tmp_path`` so the
        consuming test can inspect produced artifacts with
        :meth:`Path.exists`, :meth:`Path.stat`, etc.
    """
    import config  # noqa: WPS433 - intentional lazy import, see docstring
    output = tmp_path / "output"
    output.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(config, "OUTPUT_DIR", output, raising=True)
    monkeypatch.setattr(config, "CHECKPOINT_PATH", output / "checkpoint.json", raising=True)
    return output


@pytest.fixture
def tmp_log_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect :data:`config.LOG_DIR` and :data:`config.LOG_FILE` to ``tmp_path``.

    Symmetric to :pyfixture:`tmp_output_dir` — creates ``tmp_path /
    "logs"`` and monkeypatches ``config.LOG_DIR`` + ``config.LOG_FILE``
    so the :pyfixture:`_reset_logger_handlers_between_tests` fixture
    can re-run :func:`utils.logger._configure` against the temporary
    path on the next :func:`get_logger` call.

    Yields
    ------
    pathlib.Path
        The concrete log directory under ``tmp_path``.
    """
    import config  # noqa: WPS433 - intentional lazy import
    logs = tmp_path / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(config, "LOG_DIR", logs, raising=True)
    monkeypatch.setattr(config, "LOG_FILE", logs / "pipeline.log", raising=True)
    return logs


@pytest.fixture
def isolated_filesystem(
    tmp_output_dir: Path, tmp_log_dir: Path
) -> Dict[str, Path]:
    """Combined output + log directory isolation for whole-pipeline tests.

    Returns a dict with keys ``"output"`` and ``"logs"`` pointing at
    the respective temporary directories. Tests that run a full
    pipeline subcommand (``run.py players``, etc.) request this
    fixture to isolate both artifact and log filesystems in one go.
    """
    return {"output": tmp_output_dir, "logs": tmp_log_dir}


# ---------------------------------------------------------------------------
# NBA Stats API ``resultSets`` envelope payload fixtures
# ---------------------------------------------------------------------------
#
# The NBA Stats API returns JSON shaped ``{"resultSets": [{"name", "headers",
# "rowSet"}, ...]}``. Some endpoints (e.g. ``playercareerstats``) return the
# singular key ``resultSet`` with a single object instead of a list. The
# fixtures below cover every shape :mod:`utils.schema_normalizer` must handle
# plus pathological cases used to verify error handling (Rule 4 / Gate 1).


@pytest.fixture
def sample_single_table_payload() -> Dict[str, Any]:
    """Canonical one-table envelope modeling ``leaguedashplayerstats``.

    All cells are scalar (Rule 4 compliant); contains three rows from
    the 2025-26 season with realistic ``PLAYER_ID`` / ``TEAM_ID``
    values so tests exercising team-join semantics have non-trivial
    data.
    """
    return {
        "resource": "leaguedashplayerstats",
        "parameters": {"Season": "2025-26", "SeasonType": "Regular Season"},
        "resultSets": [
            {
                "name": "LeagueDashPlayerStats",
                "headers": ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "PTS"],
                "rowSet": [
                    [203999, "Nikola Jokić", 1610612743, 29.6],
                    [1629029, "Luka Dončić", 1610612742, 32.4],
                    [1628369, "Jayson Tatum", 1610612738, 26.9],
                ],
            }
        ],
    }


@pytest.fixture
def sample_multi_table_payload() -> Dict[str, Any]:
    """Two-table envelope modeling ``boxscoretraditionalv2`` (player + team).

    Exercises the normalizer's multi-table branch: a single payload
    must produce one DataFrame per ``resultSets`` entry, keyed by the
    entry's ``name`` field, with zero cross-contamination between
    them.
    """
    return {
        "resource": "boxscoretraditionalv2",
        "parameters": {"GameID": "0022500001"},
        "resultSets": [
            {
                "name": "PlayerStats",
                "headers": ["GAME_ID", "PLAYER_ID", "TEAM_ID", "PTS"],
                "rowSet": [
                    ["0022500001", 203999, 1610612743, 28],
                    ["0022500001", 1629029, 1610612742, 35],
                ],
            },
            {
                "name": "TeamStats",
                "headers": ["GAME_ID", "TEAM_ID", "PTS"],
                "rowSet": [
                    ["0022500001", 1610612743, 118],
                    ["0022500001", 1610612742, 112],
                ],
            },
        ],
    }


@pytest.fixture
def sample_schedule_payload() -> Dict[str, Any]:
    """``leaguegamefinder`` envelope with duplicate ``GAME_ID`` rows.

    The ``leaguegamefinder`` endpoint emits two rows per game (one for
    the home team, one for the away team). This fixture includes three
    distinct games across five rows to exercise the deduplication
    logic in ``endpoints.schedule.enumerate_game_ids``.
    """
    return {
        "resource": "leaguegamefinder",
        "parameters": {"LeagueID": "00", "Season": "2025-26"},
        "resultSets": [
            {
                "name": "LeagueGameFinderResults",
                "headers": ["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"],
                "rowSet": [
                    ["22025", 1610612747, "0022500001", "2025-10-21"],
                    ["22025", 1610612744, "0022500001", "2025-10-21"],
                    ["22025", 1610612738, "0022500002", "2025-10-22"],
                    ["22025", 1610612739, "0022500002", "2025-10-22"],
                    ["22025", 1610612737, "0022500003", "2025-10-23"],
                ],
            }
        ],
    }


@pytest.fixture
def sample_playbyplay_payload() -> Dict[str, Any]:
    """``playbyplayv2`` envelope with a small sequence of period-1 events.

    Includes the canonical ``EVENTNUM`` column used by ``games.csv``
    ordering. All cells are scalar so Rule 4 is satisfied.
    """
    return {
        "resource": "playbyplayv2",
        "parameters": {"GameID": "0022500001", "StartPeriod": 1, "EndPeriod": 14},
        "resultSets": [
            {
                "name": "PlayByPlay",
                "headers": ["GAME_ID", "EVENTNUM", "EVENTMSGTYPE", "PERIOD"],
                "rowSet": [
                    ["0022500001", 1, 12, 1],
                    ["0022500001", 2, 10, 1],
                    ["0022500001", 3, 1, 1],
                ],
            }
        ],
    }


@pytest.fixture
def sample_empty_payload() -> Dict[str, Any]:
    """``resultSets`` table with ``headers`` but zero ``rowSet`` rows.

    The normalizer must produce a zero-row DataFrame (preserving
    column names) without raising, so downstream pipelines can safely
    write a header-only CSV for slow days with no games.
    """
    return {
        "resource": "leaguedashplayerstats",
        "parameters": {"Season": "2025-26"},
        "resultSets": [
            {
                "name": "LeagueDashPlayerStats",
                "headers": ["PLAYER_ID", "PTS"],
                "rowSet": [],
            }
        ],
    }


@pytest.fixture
def sample_nested_violation_payload() -> Dict[str, Any]:
    """Pathological payload with a :class:`dict` embedded in a cell.

    Exercises the Rule 4 (flat CSV) post-flatten assertion in
    ``utils.schema_normalizer.normalize_result_sets``. The normalizer
    must raise a :class:`ValueError` (or equivalent domain error) that
    identifies the offending column so operators can trace the
    violation quickly.
    """
    return {
        "resource": "synthetic_bad_payload",
        "parameters": {},
        "resultSets": [
            {
                "name": "BadTable",
                "headers": ["ID", "META"],
                "rowSet": [
                    [1, {"embedded": "dict"}],
                    [2, {"also": "dict"}],
                ],
            }
        ],
    }


@pytest.fixture
def sample_row_mismatch_payload() -> Dict[str, Any]:
    """``rowSet`` rows whose length does not match ``headers``.

    Covers the defensive-shape branch of the normalizer: a payload
    with 3 headers but rows of length 2 and 3 must raise
    :class:`ValueError` rather than silently truncating or padding.
    """
    return {
        "resource": "synthetic_bad_shape",
        "parameters": {},
        "resultSets": [
            {
                "name": "BadShape",
                "headers": ["A", "B", "C"],
                "rowSet": [[1, 2], [3, 4, 5]],
            }
        ],
    }


@pytest.fixture
def sample_result_set_singular_payload() -> Dict[str, Any]:
    """Envelope using the singular ``resultSet`` key (e.g. ``playercareerstats``).

    A handful of NBA Stats endpoints return a single table under the
    singular key ``resultSet`` (an object) rather than the plural
    ``resultSets`` (a list). The normalizer must treat the singular
    form as equivalent to a single-element ``resultSets`` list.
    """
    return {
        "resource": "playercareerstats",
        "parameters": {"PlayerID": 203999},
        "resultSet": {
            "name": "SeasonTotalsRegularSeason",
            "headers": ["PLAYER_ID", "SEASON_ID", "PTS"],
            "rowSet": [
                [203999, "2024-25", 1700],
                [203999, "2025-26", 1800],
            ],
        },
    }


@pytest.fixture
def sample_missing_resultsets_payload() -> Dict[str, Any]:
    """Envelope missing both ``resultSets`` and ``resultSet``.

    A real upstream never emits this shape; the fixture lets the
    normalizer verify that its defensive-coding path raises a
    descriptive :class:`ValueError` rather than :class:`KeyError` (the
    latter would bubble up cryptically to operators).
    """
    return {"resource": "broken_upstream", "parameters": {}}


# ---------------------------------------------------------------------------
# DataFrame fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def flat_df() -> pd.DataFrame:
    """Canonical Rule-4-compliant DataFrame with scalar cells only.

    ``PLAYER_ID`` is intentionally the first column because the CSV
    writer contract in ``tests/unit/storage/test_csv_writer.py``
    asserts that ``first_line.split(",")[0] == "PLAYER_ID"`` on the
    produced CSV. Additional columns (``PLAYER_NAME``, ``TEAM_ID``,
    ``PTS``) cover string, integer, and float column dtypes so
    DataFrame-round-trip tests exercise pandas' CSV parsing for each
    Python scalar type.
    """
    return pd.DataFrame(
        {
            "PLAYER_ID": [203999, 1629029, 1628369],
            "PLAYER_NAME": ["Nikola Jokić", "Luka Dončić", "Jayson Tatum"],
            "TEAM_ID": [1610612743, 1610612742, 1610612738],
            "PTS": [29.6, 32.4, 26.9],
        }
    )


@pytest.fixture
def nested_df() -> pd.DataFrame:
    """Pathological DataFrame with :class:`dict` cells (Rule 4 violation).

    The column name ``"STATS"`` is referenced literally by
    ``tests/unit/storage/test_csv_writer.py::test_a4_dict_cell_rejected``
    to assert that the Rule 4 violation message identifies the
    offending column by name. Do NOT rename this column without
    updating the assertion.
    """
    return pd.DataFrame(
        {
            "PLAYER_ID": [203999, 1629029],
            "STATS": [{"PTS": 30, "AST": 8}, {"PTS": 20, "AST": 5}],
        }
    )


@pytest.fixture
def list_cell_df() -> pd.DataFrame:
    """Pathological DataFrame with :class:`list` cells (Rule 4 violation).

    The column name ``"ROSTER"`` is referenced literally by
    ``tests/unit/storage/test_csv_writer.py::test_a4_list_cell_rejected``
    to assert that the Rule 4 violation message identifies the
    offending column by name. Do NOT rename this column without
    updating the assertion.
    """
    return pd.DataFrame(
        {
            "TEAM_ID": [1610612743, 1610612742, 1610612738],
            "ROSTER": [[101, 102], [201, 202], [301, 302]],
        }
    )


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Zero-row DataFrame with only headers.

    The CSV writer must emit a single-line CSV (headers only) when
    given a zero-row frame. The Rule 4 guard MUST short-circuit on
    :attr:`DataFrame.empty` so the assertion does not misfire on an
    empty column object.
    """
    return pd.DataFrame(columns=["PLAYER_ID", "PLAYER_NAME", "PTS"])


@pytest.fixture
def large_df() -> pd.DataFrame:
    """Reproducible 1000×50 DataFrame for the large-payload test.

    Consumed by ``tests/unit/storage/test_csv_writer.py``'s
    ``TestJ5_LargeDataFrame`` which verifies ``CSVWriter`` handles
    non-trivial volumes without regressions. Built via a
    seed-42-initialised :class:`numpy.random.Generator` so the row
    values are deterministic across runs — failing tests are thus
    reproducible without snapshot files.

    The first column is ``PLAYER_ID`` (maintaining the first-column
    invariant from :pyfixture:`flat_df`); remaining 49 columns are
    ``COL_01`` … ``COL_49`` with ``float64`` random values. All cells
    are scalar so Rule 4 is satisfied.
    """
    import numpy as np  # noqa: WPS433 - lazy import; numpy is a pandas transitive dep
    rng = np.random.default_rng(seed=42)
    n_rows = 1000
    n_cols = 49
    data: Dict[str, Any] = {"PLAYER_ID": np.arange(1, n_rows + 1, dtype=np.int64)}
    for i in range(n_cols):
        data[f"COL_{i + 1:02d}"] = rng.random(n_rows)
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Factory fixtures for the handwritten spy classes
# ---------------------------------------------------------------------------


@pytest.fixture
def recording_client() -> Callable[..., RecordingClient]:
    """Factory returning fresh :class:`RecordingClient` instances per test.

    Factory form (rather than a pre-built instance) lets each test
    seed ``responses`` and ``raise_for`` differently without one
    test's configuration leaking into another.

    Example
    -------
    >>> def test_schedule_enumeration(recording_client,
    ...                               sample_schedule_payload):
    ...     client = recording_client(
    ...         responses={"leaguegamefinder": sample_schedule_payload}
    ...     )
    ...     result = client.get("leaguegamefinder", {"Season": "2025-26"})
    ...     assert result == sample_schedule_payload
    ...     client.assert_called_with_endpoint("leaguegamefinder")
    """

    def _factory(
        responses: Optional[Dict[str, Any]] = None,
        raise_for: Optional[Dict[str, BaseException]] = None,
    ) -> RecordingClient:
        return RecordingClient(responses=responses, raise_for=raise_for)

    return _factory


@pytest.fixture
def recording_writer(tmp_path: Path) -> Callable[..., RecordingWriter]:
    """Factory returning fresh :class:`RecordingWriter` instances per test.

    Every instance uses ``tmp_path / "output"`` as its output
    directory, so fake writes and any test that inspects the synthetic
    return :class:`Path` do not pollute the operator's real
    filesystem. Parameter ``raise_on`` lets a test inject a synthetic
    failure on a specific artifact name (e.g. ``raise_on="games"``)
    to verify negative-path behavior (Rule 5 — checkpoint must NOT be
    marked completed on failed write).

    Example
    -------
    >>> def test_pipeline_handles_write_failure(recording_writer,
    ...                                         recording_checkpoint,
    ...                                         flat_df):
    ...     writer = recording_writer(raise_on="games")
    ...     with pytest.raises(RuntimeError):
    ...         writer.write(flat_df, "games", "2025-26")
    """

    def _factory(raise_on: Optional[str] = None) -> RecordingWriter:
        return RecordingWriter(output_dir=tmp_path / "output", raise_on=raise_on)

    return _factory


@pytest.fixture
def recording_checkpoint() -> Callable[..., RecordingCheckpoint]:
    """Factory returning fresh :class:`RecordingCheckpoint` instances per test.

    Parameter ``completed`` is a ``{domain: iterable-of-keys}``
    mapping used to pre-seed the fake's completed set — the primary
    mechanism used by Rule 5 resume tests that assert
    already-completed keys are NOT re-fetched.

    Example
    -------
    >>> def test_resume_skips_completed(recording_checkpoint):
    ...     ckpt = recording_checkpoint(
    ...         completed={"players": ["leaguedashplayerstats:2025-26"]}
    ...     )
    ...     assert ckpt.is_completed("players",
    ...                               "leaguedashplayerstats:2025-26")
    """

    def _factory(
        completed: Optional[Dict[str, Iterable[str]]] = None,
    ) -> RecordingCheckpoint:
        return RecordingCheckpoint(completed=completed)

    return _factory


# ---------------------------------------------------------------------------
# CSV round-trip helper fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def csv_reader() -> Callable[[Path], pd.DataFrame]:
    """Return the :func:`read_csv_as_df` helper as a first-class callable.

    Exposing the helper as a fixture (rather than having tests import
    :func:`read_csv_as_df` directly) gives us a single choke-point to
    evolve round-trip semantics later (e.g. if
    :class:`~storage.csv_writer.CSVWriter` switches to a different
    encoding or delimiter, only the helper needs to change).
    """

    def _read(path: Path) -> pd.DataFrame:
        return read_csv_as_df(path)

    return _read


# ---------------------------------------------------------------------------
# Click CLI harness fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_runner() -> CliRunner:
    """Fresh :class:`click.testing.CliRunner` per test.

    Intentionally constructed without ``mix_stderr=False`` — click
    8.3.2 (the version pinned by ``requirements.txt``) removed the
    ``mix_stderr`` kwarg from :class:`CliRunner.__init__` and raises
    :class:`TypeError` if it is passed. Click ≥ 8.2 captures stdout
    and stderr separately by default and exposes them via
    ``result.stdout`` and ``result.stderr`` respectively, which is the
    behavior the Gate 13 tests rely on.
    """
    return CliRunner()


# ---------------------------------------------------------------------------
# Deterministic clock / sleep fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Replace :func:`time.monotonic` and :func:`time.sleep` with a fake.

    Returns the :class:`FakeClock` instance so tests can inspect
    :attr:`FakeClock.sleeps` and drive time forward with
    :meth:`FakeClock.advance`. Because ``monkeypatch`` scopes the
    replacement to the current test, the real ``time`` module is
    restored automatically at teardown.

    Usage pattern for the Rule 2 rate-limiter test::

        def test_rate_limiter_enforces_floor(fake_clock):
            from utils.rate_limiter import RateLimiter
            limiter = RateLimiter()
            limiter.wait()
            limiter.wait()
            # second wait() must have slept at least RULE2_FLOOR
            assert fake_clock.sleeps
            assert all(s >= 0 for s in fake_clock.sleeps)
    """
    clock = FakeClock(start=1000.0)
    monkeypatch.setattr(time, "monotonic", clock.monotonic)
    monkeypatch.setattr(time, "sleep", clock.sleep)
    return clock


# ---------------------------------------------------------------------------
# Canonical mini-season fixtures — hand-derived, mutation-sensitive test data
# ---------------------------------------------------------------------------
#
# The five fixtures below hold **input** data only: every value is a literal,
# never a value produced by running a pipeline. The expected values a consuming
# test compares against are derived by hand in that test module, which is what
# keeps the "no snapshot assertions" contract auditable.
#
# Within each artifact the per-game row counts vary rather than repeat, and the
# box-score distribution differs from the play-by-play one. That variation is
# what keeps several quantities separable: cumulative write sizes, which only
# grow, against per-game sizes, which rise and fall; a row-count metric
# emitting the per-game delta against one emitting the cumulative total; and
# each artifact's sequence against the other's. Uniform counts would instead
# make the per-game write sizes a flat but still non-decreasing sequence. The
# counts, the row order, and the game order are therefore load-bearing — every
# ordered quantity derived from these fixtures follows from exactly these
# numbers, and the derivations are recorded in the payload docstrings below.
#
# Every cell in the mini-season and mixed-ID fixtures is a scalar, so Rule 4
# (flat CSV — no nested dicts or lists) holds for every frame the normalizer
# builds from them. The sole intentional exception is
# ``malformed_result_set_payloads``, whose entries exist precisely to trip the
# normalizer's validation guards.


@pytest.fixture
def mini_season_boxscore_payloads() -> Dict[str, Any]:
    """``boxscoretraditionalv2`` envelopes for the canonical 3-game mini-season.

    Supplies the box-score half of the mini-season for game-ingestion
    aggregation tests, keyed by ``GAME_ID`` in iteration order —
    ``"0022500001"``, ``"0022500002"``, ``"0022500003"`` — matching
    :pyfixture:`mini_season_game_ids` one-for-one.

    **Row distribution is 2 / 3 / 2** (see this section's header comment for
    why the counts vary). Each envelope carries a single ``resultSets``
    entry named ``"PlayerStats"`` — the first and therefore primary table
    ``pipelines.ingest_games._select_primary_df`` selects, so the arithmetic
    below has one unambiguous source.

    Four players across three games, with the per-game points laid out so
    that two independent hand derivations of the total must agree:

    ====================  =========  ==========  ======  ======  ======
    Player                PLAYER_ID  TEAM_ID     G1 PTS  G2 PTS  G3 PTS
    ====================  =========  ==========  ======  ======  ======
    Nikola Jokić             203999  1610612743      30      25       —
    Luka Dončić             1629029  1610612742      28       —      35
    Jayson Tatum            1628369  1610612738       —      31       —
    Stephen Curry            201939  1610612744       —      22      18
    ====================  =========  ==========  ======  ======  ======

    Hand-derived arithmetic these rows support:

    * Cumulative ``games.csv`` write sizes: 2 ; 2+3=5 ; 5+2=7 → ``[2, 5, 7]``
    * Row-count conservation: 2+3+2 → ``7``
    * ``PTS`` column sum, by game: G1 30+28=58 ; G2 25+31+22=78 ;
      G3 35+18=53 ; 58+78+53 → ``189``
    * ``PTS`` cross-check, by player: Jokić 30+25=55 ; Dončić 28+35=63 ;
      Tatum 31 ; Curry 22+18=40 ; 55+63+31+40 → ``189`` — agrees
    * Rows per ``GAME_ID`` in the combined frame: ``[2, 3, 2]``
    * Distinct games ``3``; distinct players ``4``

    ``GAME_ID`` cells are strings in the canonical 10-character zero-padded
    form; ``PLAYER_ID``, ``TEAM_ID``, and ``PTS`` are integers.
    """
    return {
        "0022500001": {
            "resource": "boxscoretraditionalv2",
            "parameters": {"GameID": "0022500001"},
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": ["GAME_ID", "PLAYER_ID", "TEAM_ID", "PTS"],
                    "rowSet": [
                        ["0022500001", 203999, 1610612743, 30],
                        ["0022500001", 1629029, 1610612742, 28],
                    ],
                }
            ],
        },
        "0022500002": {
            "resource": "boxscoretraditionalv2",
            "parameters": {"GameID": "0022500002"},
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": ["GAME_ID", "PLAYER_ID", "TEAM_ID", "PTS"],
                    "rowSet": [
                        ["0022500002", 203999, 1610612743, 25],
                        ["0022500002", 1628369, 1610612738, 31],
                        ["0022500002", 201939, 1610612744, 22],
                    ],
                }
            ],
        },
        "0022500003": {
            "resource": "boxscoretraditionalv2",
            "parameters": {"GameID": "0022500003"},
            "resultSets": [
                {
                    "name": "PlayerStats",
                    "headers": ["GAME_ID", "PLAYER_ID", "TEAM_ID", "PTS"],
                    "rowSet": [
                        ["0022500003", 1629029, 1610612742, 35],
                        ["0022500003", 201939, 1610612744, 18],
                    ],
                }
            ],
        },
    }


@pytest.fixture
def mini_season_playbyplay_payloads() -> Dict[str, Any]:
    """``playbyplayv2`` envelopes for the canonical 3-game mini-season.

    Supplies the play-by-play half of the mini-season, alongside
    :pyfixture:`mini_season_boxscore_payloads`. A games pipeline fetches a
    box score *and* a play-by-play per game, so both mappings are keyed by
    the same three ``GAME_ID`` values in the same order.

    The ``parameters`` shape mirrors :pyfixture:`sample_playbyplay_payload`
    (``GameID`` plus the ``StartPeriod`` / ``EndPeriod`` pair), and the
    single ``resultSets`` entry is named ``"PlayByPlay"`` — the primary
    table ``pipelines.ingest_games._select_primary_df`` selects.

    **Row distribution is 4 / 1 / 3**, different from the box-score
    2 / 3 / 2 distribution so that the two artifacts' cumulative sequences
    cannot be confused with one another.

    Hand-derived arithmetic these rows support:

    * Cumulative ``play_by_play.csv`` write sizes: 4 ; 4+1=5 ; 5+3=8 →
      ``[4, 5, 8]``
    * Row-count conservation: 4+1+3 → ``8``
    * Interleaved with the box-score counts, the ordered
      ``pipeline_rows_written_total`` increments are (games, pbp) per
      iteration — (2,4), (3,1), (2,3) → ``[2, 4, 3, 1, 2, 3]``, whereas
      emitting the cumulative frame length instead of the per-game count
      would read ``[2, 4, 5, 5, 7, 8]``.

    ``EVENTNUM`` values are integers numbered from 1 within each game;
    ``EVENTDESC`` values are short scalar strings, and no cell is ``None``.
    """
    return {
        "0022500001": {
            "resource": "playbyplayv2",
            "parameters": {"GameID": "0022500001", "StartPeriod": 1, "EndPeriod": 14},
            "resultSets": [
                {
                    "name": "PlayByPlay",
                    "headers": ["GAME_ID", "EVENTNUM", "EVENTDESC"],
                    "rowSet": [
                        ["0022500001", 1, "Jump Ball won by Denver"],
                        ["0022500001", 2, "Jokić 12' Jump Shot (2 PTS)"],
                        ["0022500001", 3, "Dončić Driving Layup (2 PTS)"],
                        ["0022500001", 4, "Denver Rebound"],
                    ],
                }
            ],
        },
        "0022500002": {
            "resource": "playbyplayv2",
            "parameters": {"GameID": "0022500002", "StartPeriod": 1, "EndPeriod": 14},
            "resultSets": [
                {
                    "name": "PlayByPlay",
                    "headers": ["GAME_ID", "EVENTNUM", "EVENTDESC"],
                    "rowSet": [
                        ["0022500002", 1, "Tatum 26' 3PT Jump Shot (3 PTS)"],
                    ],
                }
            ],
        },
        "0022500003": {
            "resource": "playbyplayv2",
            "parameters": {"GameID": "0022500003", "StartPeriod": 1, "EndPeriod": 14},
            "resultSets": [
                {
                    "name": "PlayByPlay",
                    "headers": ["GAME_ID", "EVENTNUM", "EVENTDESC"],
                    "rowSet": [
                        ["0022500003", 1, "Curry 28' 3PT Jump Shot (3 PTS)"],
                        ["0022500003", 2, "Dončić Free Throw 1 of 2 (1 PTS)"],
                        ["0022500003", 3, "SUB: Curry FOR Podziemski"],
                    ],
                }
            ],
        },
    }


@pytest.fixture
def mini_season_game_ids() -> List[str]:
    """The ordered canonical ``GAME_ID`` list for the mini-season.

    Supplies the enumeration input for game-ingestion tests: the three
    identifiers a games pipeline receives from schedule enumeration, in the
    order it receives them.

    **Order is significant, not incidental.** A games pipeline iterates this
    sequence, appends each game's frame to its buffer in that order,
    re-writes the cumulative artifact after every game, and records one
    checkpoint mark per game, so every ordered quantity derived from the
    mini-season follows from this exact ordering.

    Every element is the canonical 10-character zero-padded form, and the
    three values correspond one-for-one with the keys of
    :pyfixture:`mini_season_boxscore_payloads` and
    :pyfixture:`mini_season_playbyplay_payloads`.
    """
    return ["0022500001", "0022500002", "0022500003"]


@pytest.fixture
def schedule_mixed_game_id_payload() -> Dict[str, Any]:
    """``leaguegamefinder`` envelope where one game arrives under two wire types.

    Supplies the mixed-wire-type input for ``GAME_ID`` canonicalization and
    deduplication tests.

    The envelope shape follows :pyfixture:`sample_schedule_payload`
    (``leaguegamefinder`` resource, ``LeagueGameFinderResults`` table,
    headers ``SEASON_ID`` / ``TEAM_ID`` / ``GAME_ID`` / ``GAME_DATE``), but
    the row distribution is **2 + 1**: two rows for one game and a single
    row for another, three rows describing **two** distinct games in total.

    1. ``GAME_ID`` as the bare integer ``22500001``
    2. **the same game**, ``GAME_ID`` as the string ``"0022500001"``
    3. a distinct control game, ``"0022500002"``

    The int-versus-str asymmetry between rows 1 and 2 is the entire point:
    row 1's cell is a bare Python :class:`int`, with no quotes and no
    leading zeros. The shape is realistic rather than contrived — the
    inline commentary in ``endpoints/schedule.py`` records that the upstream
    occasionally returns numeric types for identifiers that look numeric.

    The keying arithmetic follows from that asymmetry: ``str(22500001)`` is
    eight characters while ``str("0022500001")`` is ten, so deduplicating on
    the unpadded string form keys these three rows as ``['22500001',
    '0022500001', '0022500002']`` — **three identifiers for two games**,
    which has a games pipeline fetch and append one game twice and duplicate
    its rows in ``games.csv``. ``CheckpointManager.get_pending`` cannot
    absorb such a duplicate, because it is an order-preserving filter that
    does not deduplicate its input. Keying on the 10-character zero-padded
    canonical form instead yields ``['0022500001', '0022500002']`` — two
    identifiers, every element ten characters wide.
    """
    return {
        "resource": "leaguegamefinder",
        "parameters": {"LeagueID": "00", "Season": "2025-26"},
        "resultSets": [
            {
                "name": "LeagueGameFinderResults",
                "headers": ["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"],
                "rowSet": [
                    ["22025", 1610612747, 22500001, "2025-10-21"],
                    ["22025", 1610612744, "0022500001", "2025-10-21"],
                    ["22025", 1610612738, "0022500002", "2025-10-22"],
                ],
            }
        ],
    }


@pytest.fixture
def malformed_result_set_payloads() -> Dict[str, Any]:
    """Ten payloads targeting selected ``normalize_result_sets`` branches.

    Supplies the malformed-envelope inputs for schema-normalizer validation
    tests, keyed by case name so the cases can be parametrized and matched
    against exact :class:`ValueError` message text. The branches sit in
    ``_extract_tables``, ``_require_str``, ``_require_list``, the
    non-string-headers check in ``normalize_result_sets``, and the row type
    and width checks in ``_build_dataframe`` — each reached through the
    public ``utils.schema_normalizer.normalize_result_sets`` entry point
    rather than by importing a private helper.

    Both multi-condition guards are covered on **every** condition rather
    than only on their absent-key condition, because each is a single
    ``or``-joined predicate whose conditions are independently mutable:

    * ``_require_str`` rejects an absent, a non-string, **and** an empty
      ``name`` (``if not isinstance(value, str) or not value``), so
      ``missing_name`` / ``name_not_string`` / ``name_empty`` exercise its
      three conditions in turn.
    * ``_require_list`` rejects an absent **and** a non-list ``rowSet``
      (``if not isinstance(value, list)``), so ``missing_row_set`` supplies
      the absent key and ``row_set_not_list`` a concrete wrong type.

    Each envelope is the minimal shape that trips exactly one branch. That
    requires care about evaluation order inside ``normalize_result_sets``:
    ``name`` is resolved and snake-cased *before* ``headers`` and ``rowSet``
    are validated, and the non-string-headers check runs *after* both
    ``_require_list`` calls. So, for example, the non-string-header envelope
    must still carry a valid ``name`` and a list-typed ``rowSet``, or it
    would trip an earlier guard instead. The result-set name is ``"t"``
    wherever a name is present, because ``_snake_case("t")`` returns
    ``"t"`` and the message text therefore quotes it verbatim.

    Case name → the exact message raised:

    * ``row_width_mismatch`` → ``Result set 't' row 0 has 2 values but 3
      headers are declared``
    * ``missing_name`` → ``Result set is missing required string field
      'name' (got NoneType)``
    * ``name_not_string`` → ``Result set is missing required string field
      'name' (got int)``
    * ``name_empty`` → ``Result set is missing required string field 'name'
      (got str)``
    * ``headers_not_list`` → ``Result set field 'headers' must be a list;
      got dict``
    * ``missing_row_set`` → ``Result set field 'rowSet' must be a list; got
      NoneType``
    * ``row_set_not_list`` → ``Result set field 'rowSet' must be a list;
      got dict``
    * ``non_string_header`` → ``Result set 't' contains non-string headers:
      ['A', 7]``
    * ``row_not_sequence`` → ``Result set 't' row 0 is dict, expected
      list/tuple``
    * ``result_sets_wrong_type`` → ``'resultSets' must be a list or dict,
      got str``

    Branches outside this set: a payload with no result sets, a singular
    ``resultSet`` of the wrong type, a non-dict payload
    (:class:`TypeError`, not :class:`ValueError`), and Rule 4 nested cells
    — covered by :pyfixture:`sample_nested_violation_payload`.
    """
    return {
        "row_width_mismatch": {
            "resultSets": [
                {"name": "t", "headers": ["A", "B", "C"], "rowSet": [[1, 2]]}
            ]
        },
        "missing_name": {
            "resultSets": [
                {"headers": ["A"], "rowSet": [[1]]}
            ]
        },
        # ``name`` is PRESENT but is an int, so ``isinstance(value, str)``
        # fails and the rendered type name is "int" rather than "NoneType".
        "name_not_string": {
            "resultSets": [
                {"name": 7, "headers": ["A"], "rowSet": [[1]]}
            ]
        },
        # ``name`` is PRESENT and is a str, so only the second condition
        # (``not value``) can reject it; the rendered type name is "str".
        "name_empty": {
            "resultSets": [
                {"name": "", "headers": ["A"], "rowSet": [[1]]}
            ]
        },
        "headers_not_list": {
            "resultSets": [
                {"name": "t", "headers": {"A": 1}, "rowSet": [[1]]}
            ]
        },
        "missing_row_set": {
            "resultSets": [
                {"name": "t", "headers": ["A"]}
            ]
        },
        # ``rowSet`` is PRESENT but is a dict, so the guard reports the
        # concrete wrong type "dict" instead of the absent-key "NoneType".
        # ``headers`` stays a valid list because it is validated first.
        "row_set_not_list": {
            "resultSets": [
                {"name": "t", "headers": ["A"], "rowSet": {}}
            ]
        },
        "non_string_header": {
            "resultSets": [
                {"name": "t", "headers": ["A", 7], "rowSet": [[1, 2]]}
            ]
        },
        "row_not_sequence": {
            "resultSets": [
                {"name": "t", "headers": ["A"], "rowSet": [{"A": 1}]}
            ]
        },
        "result_sets_wrong_type": {"resultSets": "not-a-list"},
    }
