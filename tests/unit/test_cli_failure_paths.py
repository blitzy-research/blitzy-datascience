"""CLI failure-path contracts — the ``except Exception`` boundary of ``run.py``.

What this module pins
---------------------
Five contracts that the pre-existing suite leaves completely undefended:

1. **Non-zero exit on failure.** Each of the five data subcommands
   (``players``, ``teams``, ``games``, ``lineups``, ``schedule``) must
   exit ``1`` when its pipeline raises, because every ``except
   Exception`` block in ``run.py`` ends with a bare ``raise``
   (L265, L297, L338, L370, L402, L458).
2. **Exception fidelity.** The *original* exception instance must reach
   the caller un-swallowed and un-rewrapped.
3. **Metric label sets and values.** ``pipeline_runs_total`` must be
   incremented exactly once with ``outcome="error"`` on the failure path
   and exactly once with ``outcome="success"`` on the happy path, under
   the ``pipeline`` label values spelled out in ``run.py`` at L256/L262
   (players), L288/L294 (teams), L329/L335 (games), L361/L367
   (lineups), L393/L399 (schedule) and L449/L455 (``all``).
4. **``all`` is fail-fast.** ``run.py``'s ``all_cmd`` (L421) wraps the
   *whole* dispatch loop in ONE ``try`` (L437-L458), so the first
   failing pipeline must prevent every later pipeline from running.
5. **``ready`` translates status into an exit code.** ``ready_cmd``
   (L496) echoes the probe body and *then* calls ``sys.exit(1)``
   (L513-L515) when ``status != "ready"``.

Why this is a separate sibling module
-------------------------------------
``tests/unit/test_cli.py`` is a 38,902-byte, 19-test module whose remit
is Gate 13 (registration-invocation pairing), and whose
``test_ready_subcommand_when_configured`` (L612) deliberately accepts
``exit_code in (0, 1)`` for robustness against partial CI environments.
That test is **frozen**: this module *supplements* it by making the
readiness verdict deterministic in both directions, and never edits it.
A focused sibling keeps the failure-path concern reviewable in isolation
and is collected automatically because ``pytest.ini`` sets
``python_files = test_*.py``.

The gap this module closes
--------------------------
Verified by grep across the whole ``tests/`` tree before this module
existed:

===========================  ==================
Pattern                      Hits in ``tests/``
===========================  ==================
``outcome="error"``          0
``SystemExit``               0
``_build_collaborators``     0
===========================  ==================

``"pipeline_runs_total"`` appeared exactly once — as a mere *substring*
check at ``tests/unit/test_cli.py`` L671 — so the counter's value and
label set were asserted nowhere at all. Two plausible one-line mutations
were therefore invisible to every one of the 698 passing tests:

* deleting the bare ``raise`` that closes an ``except`` block, which
  would make the CLI report success on a total pipeline failure; and
* moving ``all_cmd``'s ``try``/``except`` *inside* the dispatch loop,
  which would run all five pipelines and exit 0 after the first failure.

``_build_collaborators`` (``run.py`` L88) is exercised *implicitly*: it
is the first statement of every data subcommand, so each invocation here
constructs the real ``RateLimiter``/``NBAClient``/``CSVWriter``/
``CheckpointManager`` graph against the ``tmp_path``-rooted config. It is
never imported, promoted, or given a test-only hook.

No captured output
------------------
Every expected value below is a **structural constant read from
``run.py``** at the line cited in its comment — an exit code, an
exception class, a float counter value, a label dict, or an ordered list
of domain names. Nothing here records the CLI's current output and
asserts equality against it, and no expectation would change if the
production code were rewritten while keeping its documented contract.

Marker posture
--------------
This module registers **no** pytest marker, so it runs on the default
offline tier. ``pytest.ini`` registers only ``integration`` and
``invariant``, and ``--strict-markers`` turns any third marker into a
hard collection error.

``catch_exceptions`` posture — do NOT "helpfully" change this
-------------------------------------------------------------
Failure-path invocations deliberately use :meth:`CliRunner.invoke`'s
**default** ``catch_exceptions=True`` so the propagated exception is
recorded on ``result.exception`` where it can be asserted. Passing
``catch_exceptions=False`` — correct for the happy-path tests in
``test_cli.py`` — would let the injected exception escape ``invoke()``
and abort the test instead of being measured. The happy-path tests in
*this* module do pass ``catch_exceptions=False``, matching the
neighbouring module, precisely because no exception is expected there.

Isolation
---------
Every test consumes ``tmp_output_dir`` and ``tmp_log_dir`` so no
operator directory is touched, and relies on the three autouse fixtures
in ``tests/conftest.py`` that reset the correlation ID, the metrics
registry, and the logger handlers before AND after every test. That
registry reset is what makes the exact ``== 0.0`` assertions meaningful
rather than vacuous: a label set that was never incremented reads
``0.0`` by the Prometheus convention documented on
``MetricsRegistry.get_counter_value`` (``utils/metrics.py`` L856).

Import scope
------------
Per the file schema's ``depends_on_files`` whitelist this module imports
only stdlib primitives (``__future__``, ``json``, ``typing``),
``pytest``, :mod:`config`, :mod:`run` (the ``cli`` group plus the module
object used as the readiness patch target), the five
:mod:`pipelines.ingest_<domain>` modules that ``run.py`` itself imports,
and :mod:`utils.metrics` for the counter read-back. :mod:`requests` is
never imported (Rule 1) and no third-party test library is introduced.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Dict, List

import pytest

import config
import run as run_module
from pipelines import (
    ingest_games,
    ingest_lineups,
    ingest_players,
    ingest_schedule,
    ingest_teams,
)
from run import cli
from utils import metrics

# ---------------------------------------------------------------------------
# Structural constants.
#
# Every value in this block was READ FROM the production source at the
# cited line — never captured from a run. This is what makes the module
# snapshot-free: a reviewer can verify each expectation by opening
# ``run.py`` at the referenced line, without executing anything.
# ---------------------------------------------------------------------------

#: The counter ``run.py`` increments on both the success and the failure
#: path of every subcommand (for example L254-L257 and L260-L263). It is
#: pre-registered in ``utils/metrics.py`` L509-L512, which is why
#: :meth:`get_counter_value` returns a well-defined ``0.0`` — rather than
#: raising — for a label set that was never incremented.
RUNS_COUNTER: str = "pipeline_runs_total"

#: ``(cli subcommand name, metric ``pipeline`` label)`` for all five data
#: subcommands. The two strings DIFFER — the CLI name is ``teams`` while
#: the label is ``ingest_teams`` — so each pair is spelled out literally
#: instead of being derived with an ``f"ingest_{name}"`` expression. A
#: literal table turns a label rename in ``run.py`` into a failure here;
#: a derived one would silently follow the rename.
DATA_SUBCOMMAND_LABELS: tuple = (
    ("players", "ingest_players"),
    ("teams", "ingest_teams"),
    ("games", "ingest_games"),
    ("lineups", "ingest_lineups"),
    ("schedule", "ingest_schedule"),
)

#: The aggregate subcommand's ``pipeline`` label. ``run.py`` L449/L455
#: use the bare marker ``"all"`` — NOT ``"ingest_all"`` — to distinguish
#: whole-run outcomes from per-pipeline outcomes (see the rationale
#: comment at ``run.py`` L221-L227).
ALL_PIPELINE_LABEL: str = "all"

#: The label the aggregate subcommand must NEVER emit. Asserting that
#: this series stays at zero converts a silent label typo into a test
#: failure, because :meth:`get_counter_value` reports ``0.0`` for an
#: unseen series instead of raising.
ALL_PIPELINE_LABEL_TYPO: str = "ingest_all"

#: The two ``outcome`` label values, read from ``run.py`` L256 and L262.
OUTCOME_SUCCESS: str = "success"
OUTCOME_ERROR: str = "error"

#: ``run.py`` increments ``pipeline_runs_total`` EXACTLY once per
#: invocation — every ``metrics.registry.inc`` call sits outside any loop
#: — so one invocation moves the observed series to ``1.0`` and leaves
#: every other series at ``0.0``.
EXPECTED_COUNTER_HIT: float = 1.0
EXPECTED_COUNTER_MISS: float = 0.0

#: Exit code Click reports when a command callback lets an exception
#: escape, which is exactly what ``run.py``'s bare ``raise`` statements
#: (L265, L297, L338, L370, L402, L458) cause. It is also the literal
#: argument ``ready_cmd`` hands to ``sys.exit`` at L515.
EXIT_FAILURE: int = 1

#: Exit code for a subcommand whose callback returns without raising.
EXIT_SUCCESS: int = 0

#: The ordered dispatch table of ``run.py``'s ``all_cmd``, transcribed
#: from the ``order`` list literal at L429-L435. The sequence is binding
#: (AAP §0.4.5): ``games`` consumes the ``GAME_ID`` list that
#: ``schedule`` produces, and ``lineups`` depends on earlier artifacts.
EXPECTED_ALL_ORDER: List[str] = ["schedule", "games", "teams", "players", "lineups"]

#: The name of the pipeline deliberately failed in the ``all`` fail-fast
#: test. It is the FIRST entry of :data:`EXPECTED_ALL_ORDER`, so every
#: other pipeline is downstream of the injected failure.
FIRST_ALL_PIPELINE: str = "schedule"

#: The complete dispatch record ``all`` may show once its FIRST pipeline
#: fails. Derived structurally rather than observed: ``all_cmd`` wraps
#: the entire ``for`` loop in ONE ``try`` (L437-L458), so the raise
#: unwinds the loop instead of advancing to the next entry.
EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE: List[str] = ["schedule"]

#: Message carried by the injected exception. Asserted verbatim so the
#: test proves the original exception INSTANCE crossed the CLI boundary,
#: not merely some other object of the same class.
INJECTED_FAILURE_MESSAGE: str = "injected pipeline failure for CLI failure-path testing"

#: Deterministic stand-in results for :func:`utils.health.check_readiness`.
#: ``run.py`` L513 feeds the value straight to :func:`json.dumps` and
#: L514 indexes it with ``["status"]``, so each must be JSON-serializable
#: and must carry a ``"status"`` key or the callback would raise
#: ``KeyError`` before reaching the branch under test. The nested
#: sub-probe shape mirrors ``utils/health.py`` L144-L147. Neither dict is
#: ever mutated, by this module or by the production code.
NOT_READY_PROBE_RESULT: Dict[str, Any] = {
    "status": "not_ready",
    "checks": {"output_dir_writable": {"status": "fail", "detail": "injected by test"}},
}
READY_PROBE_RESULT: Dict[str, Any] = {
    "status": "ready",
    "checks": {"output_dir_writable": {"status": "ok", "detail": "injected by test"}},
}

#: CLI subcommand name -> the pipeline module whose ``run`` attribute
#: must be patched. ``run.py`` performs ``from pipelines import
#: ingest_<domain>`` (L69-L75) and resolves ``.run`` at CALL time, so
#: patching the module object here is what the production dispatch
#: actually sees. Patching a name local to this test file, or patching
#: ``endpoints``/``pipelines`` re-exports, would silently no-op.
PIPELINE_MODULES: Dict[str, Any] = {
    "players": ingest_players,
    "teams": ingest_teams,
    "games": ingest_games,
    "lineups": ingest_lineups,
    "schedule": ingest_schedule,
}


# ---------------------------------------------------------------------------
# Test doubles.
#
# Handwritten, per the stated preference in ``tests/conftest.py`` for
# hand-rolled spies over ``MagicMock`` at the seam of the code under test,
# so that interface drift surfaces as a loud ``TypeError`` instead of
# being absorbed by attribute-access magic. ``MagicMock`` is reserved in
# this repository for the HTTP transport seam, which does not appear here.
# ---------------------------------------------------------------------------


class _InjectedPipelineFailure(RuntimeError):
    """Distinctive exception injected into a pipeline to drive the except path.

    A bespoke class — rather than a bare :class:`RuntimeError` — is what
    lets the assertions state ``type(result.exception) is
    _InjectedPipelineFailure``. If ``run.py`` ever wrapped the original
    exception (for example in a :class:`click.ClickException`, or in a
    generic ``RuntimeError("pipeline failed")``) that identity check
    would fail even though the exit code stayed ``1``.
    """


def _make_recorder(recorder: List[str], domain: str) -> Callable[..., None]:
    """Return a spy that appends ``domain`` to ``recorder`` and returns ``None``.

    Produced by a FACTORY so that ``domain`` is captured by value.
    Installing the closures directly inside a ``for`` loop would make
    every spy share the loop variable's final value — the classic
    late-binding bug — and every ordering assertion in this module would
    then be meaningless while still appearing to pass.

    ``**kwargs`` is accepted because ``run.py`` dispatches with the four
    keywords ``client``, ``writer``, ``checkpoint`` and ``season``
    (L248-L253 and siblings). A narrower signature would raise
    ``TypeError`` and mask the contract under test behind a plumbing
    error. Returning ``None`` mirrors the production pipelines.
    """

    def _run(**kwargs: Any) -> None:
        recorder.append(domain)

    return _run


def _make_failing_recorder(recorder: List[str], domain: str) -> Callable[..., None]:
    """Return a spy that records ``domain`` and THEN raises the injected failure.

    Recording *before* raising is deliberate: it proves the failing
    pipeline actually started, so a later ``recorder == [...]`` assertion
    distinguishes "dispatched and then failed" from "never dispatched at
    all". Without the pre-raise append, an ``all`` fail-fast assertion
    could not tell a correct short-circuit apart from a broken dispatch
    that never invoked anything.
    """

    def _run(**kwargs: Any) -> None:
        recorder.append(domain)
        raise _InjectedPipelineFailure(INJECTED_FAILURE_MESSAGE)

    return _run


def _install_recorders(monkeypatch: pytest.MonkeyPatch, recorder: List[str]) -> None:
    """Install a recording spy on all five ``pipelines.ingest_*.run`` callables.

    Every test installs spies on *all five* pipelines even when it
    invokes a single subcommand. That is what makes the shared
    ``recorder`` list a complete dispatch census: an exact-list assertion
    then simultaneously proves the right pipeline ran, that it ran
    exactly once, and that no sibling pipeline was reached. It also keeps
    the test network-free under a cross-wiring mutation, because a
    mis-dispatched call lands on a spy rather than on a production
    pipeline that would try to reach the NBA Stats API.
    """
    for domain, module in PIPELINE_MODULES.items():
        monkeypatch.setattr(module, "run", _make_recorder(recorder, domain))


def _make_readiness_probe(result: Dict[str, Any]) -> Callable[[], Dict[str, Any]]:
    """Return a zero-argument stand-in for :func:`utils.health.check_readiness`.

    ``run.py`` L512 calls ``health.check_readiness()`` with no arguments,
    L513 hands the value to :func:`json.dumps`, and L514 indexes it with
    ``["status"]``. The stand-in therefore must be zero-argument, must be
    JSON-serializable, and must carry a ``"status"`` key. Substituting a
    deterministic verdict is what removes the environmental variability
    that forces the frozen ``test_cli.py`` readiness test to accept
    either exit code.
    """

    def _check_readiness() -> Dict[str, Any]:
        return result

    return _check_readiness


def _runs_counter(pipeline: str, outcome: str) -> float:
    """Read ``pipeline_runs_total`` for one exact ``(pipeline, outcome)`` pair.

    Reads the REAL registry singleton that ``run.py`` writes to
    (``utils/metrics.py`` L1134) rather than a substitute, because the
    contract under test is the value and the label set that production
    code actually emits. A mock registry would happily record whatever
    labels the CLI passed and prove nothing about their correctness.
    """
    return metrics.registry.get_counter_value(
        RUNS_COUNTER,
        labels={"pipeline": pipeline, "outcome": outcome},
    )


# ---------------------------------------------------------------------------
# Section A — the ``except Exception`` path of the five data subcommands.
#
# ``run.py`` L259-L265 (and its four siblings) log the failure and then
# re-raise. Nothing in the pre-existing suite drives that branch, so the
# tests below are the first to enter it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("subcommand,pipeline_label", DATA_SUBCOMMAND_LABELS)
def test_data_subcommand_exits_one_and_propagates_the_original_exception(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    subcommand: str,
    pipeline_label: str,
) -> None:
    """A raising pipeline must exit 1 with the original exception intact.

    Mutation detected: deleting the bare ``raise`` that closes each
    ``except Exception`` block (``run.py`` L265, L297, L338, L370, L402).
    Without it the callback would return normally, Click would exit
    ``0``, and ``result.exception`` would be ``None`` — so a total
    pipeline failure would be reported to the operator as a success.
    Replacing the propagated exception with a wrapper (for example
    ``raise RuntimeError("pipeline failed") from exc``) is caught by the
    type-identity and message assertions.
    """
    # Arrange — census spies on all five pipelines, then override the one
    # under test with a recording raiser.
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[subcommand],
        "run",
        _make_failing_recorder(recorder, subcommand),
    )

    # Act — the DEFAULT ``catch_exceptions=True`` is essential: it is what
    # records the propagated exception instead of aborting this test.
    result = cli_runner.invoke(cli, [subcommand, "--season", config.DEFAULT_SEASON])

    # Assert
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli {subcommand}` must exit {EXIT_FAILURE} when "
        f"{pipeline_label}.run raises; got {result.exit_code}. "
        f"stderr={result.stderr!r}"
    )
    assert type(result.exception) is _InjectedPipelineFailure, (
        f"`cli {subcommand}` must propagate the original exception; got "
        f"{type(result.exception).__name__} ({result.exception!r}), "
        f"expected _InjectedPipelineFailure"
    )
    assert str(result.exception) == INJECTED_FAILURE_MESSAGE, (
        f"`cli {subcommand}` replaced or re-wrapped the exception "
        f"instance; message={str(result.exception)!r} expected "
        f"{INJECTED_FAILURE_MESSAGE!r}"
    )
    assert recorder == [subcommand], (
        f"`cli {subcommand}` must dispatch to its own pipeline exactly "
        f"once and to no other; recorder={recorder!r} expected "
        f"{[subcommand]!r}"
    )


@pytest.mark.parametrize("subcommand,pipeline_label", DATA_SUBCOMMAND_LABELS)
def test_data_subcommand_increments_only_the_error_outcome_counter_on_failure(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    subcommand: str,
    pipeline_label: str,
) -> None:
    """A raising pipeline records ``outcome="error"`` once and ``success`` never.

    Mutation detected: labelling the failure path ``outcome="success"``,
    incrementing both series, incrementing the error series twice, or
    misspelling the ``pipeline`` label (``run.py`` L260-L263 and its four
    siblings). Because :meth:`get_counter_value` returns a well-defined
    ``0.0`` for a never-incremented label set, the
    :data:`EXPECTED_COUNTER_MISS` assertion is exact rather than vacuous
    — and a typo'd label would leave the asserted series at ``0.0`` and
    fail the first assertion rather than passing silently.
    """
    # Arrange
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[subcommand],
        "run",
        _make_failing_recorder(recorder, subcommand),
    )

    # Act
    result = cli_runner.invoke(cli, [subcommand, "--season", config.DEFAULT_SEASON])

    # Assert
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli {subcommand}` must exit {EXIT_FAILURE} when "
        f"{pipeline_label}.run raises; got {result.exit_code}. "
        f"stderr={result.stderr!r}"
    )
    observed_error = _runs_counter(pipeline_label, OUTCOME_ERROR)
    observed_success = _runs_counter(pipeline_label, OUTCOME_SUCCESS)
    assert observed_error == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{pipeline_label}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_HIT} after exactly one failed `cli "
        f"{subcommand}` invocation"
    )
    assert observed_success == EXPECTED_COUNTER_MISS, (
        f'{RUNS_COUNTER}{{pipeline="{pipeline_label}",'
        f'outcome="{OUTCOME_SUCCESS}"}} is {observed_success}; expected '
        f"{EXPECTED_COUNTER_MISS} because the pipeline raised and no "
        f"success may be recorded"
    )


# ---------------------------------------------------------------------------
# Section B — the success side of the same counter.
#
# The pre-existing suite proves that a subcommand DISPATCHES to its
# pipeline (Gate 13) but never inspects the counter it emits afterwards.
# These tests pin the exact value and label set of the success series so
# the failure-side assertions above have a calibrated counterpart.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("subcommand,pipeline_label", DATA_SUBCOMMAND_LABELS)
def test_data_subcommand_increments_only_the_success_outcome_counter_on_a_clean_run(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    subcommand: str,
    pipeline_label: str,
) -> None:
    """A returning pipeline exits 0 and records ``outcome="success"`` exactly once.

    Mutation detected: incrementing the success counter twice, moving the
    ``inc`` call inside a loop, emitting the wrong ``pipeline`` label, or
    also touching the ``error`` series on a clean run (``run.py``
    L254-L257 and its four siblings). ``== 1.0`` is asserted rather than
    ``> 0`` precisely so a double increment is a failure.
    """
    # Arrange — every pipeline is a no-op recorder; nothing raises.
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)

    # Act — ``catch_exceptions=False`` matches the neighbouring happy-path
    # tests: on this path no exception is expected, so letting one escape
    # gives a faster diagnosis than a recorded traceback would.
    result = cli_runner.invoke(
        cli,
        [subcommand, "--season", config.DEFAULT_SEASON],
        catch_exceptions=False,
    )

    # Assert
    assert result.exit_code == EXIT_SUCCESS, (
        f"`cli {subcommand}` must exit {EXIT_SUCCESS} when its pipeline "
        f"returns; got {result.exit_code}. stderr={result.stderr!r}"
    )
    assert recorder == [subcommand], (
        f"`cli {subcommand}` must dispatch to its own pipeline exactly "
        f"once and to no other; recorder={recorder!r} expected "
        f"{[subcommand]!r}"
    )
    observed_success = _runs_counter(pipeline_label, OUTCOME_SUCCESS)
    observed_error = _runs_counter(pipeline_label, OUTCOME_ERROR)
    assert observed_success == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{pipeline_label}",'
        f'outcome="{OUTCOME_SUCCESS}"}} is {observed_success}; expected '
        f"exactly {EXPECTED_COUNTER_HIT} after one clean `cli "
        f"{subcommand}` invocation"
    )
    assert observed_error == EXPECTED_COUNTER_MISS, (
        f'{RUNS_COUNTER}{{pipeline="{pipeline_label}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_MISS} because nothing raised"
    )


# ---------------------------------------------------------------------------
# Section C — the ``all`` fail-fast guarantee (the headline contract).
#
# ``all_cmd`` wraps the ENTIRE dispatch loop in one ``try`` (``run.py``
# L437-L458). A mutation that moved the ``try``/``except`` inside the loop
# would run all five pipelines and exit 0 after a failure, and every one
# of the 698 pre-existing tests would still pass.
# ---------------------------------------------------------------------------


def test_all_subcommand_is_fail_fast_and_never_reaches_later_pipelines(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """``cli all`` must abandon the remaining pipelines once the first one raises.

    Mutation detected: moving ``all_cmd``'s ``try``/``except`` *inside*
    the ``for`` loop (``run.py`` L437-L458). That version would swallow
    the schedule failure per-iteration, continue through games, teams,
    players and lineups, and exit ``0`` — silently ingesting Games from a
    Schedule artifact that was never written. The whole-list assertion
    below is what makes that impossible: it proves not merely that
    ``schedule`` ran, but that the four downstream pipelines did NOT.
    """
    # Arrange — census spies on all five, then fail the FIRST entry of the
    # documented order so that every other pipeline is downstream of it.
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[FIRST_ALL_PIPELINE],
        "run",
        _make_failing_recorder(recorder, FIRST_ALL_PIPELINE),
    )

    # Act — default ``catch_exceptions=True`` records the propagated error.
    result = cli_runner.invoke(cli, ["all", "--season", config.DEFAULT_SEASON])

    # Assert — the WHOLE ordered census, not a membership check.
    assert recorder == EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE, (
        f"`cli all` must stop after the first failing pipeline; dispatch "
        f"census={recorder!r} expected "
        f"{EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE!r}. Any extra entry "
        f"means the failure did not abort the loop"
    )
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli all` must exit {EXIT_FAILURE} when a pipeline raises; got "
        f"{result.exit_code}. stderr={result.stderr!r}"
    )
    assert type(result.exception) is _InjectedPipelineFailure, (
        f"`cli all` must propagate the original exception; got "
        f"{type(result.exception).__name__} ({result.exception!r}), "
        f"expected _InjectedPipelineFailure"
    )


def test_all_subcommand_increments_only_the_aggregate_error_counter_on_failure(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """A failed ``cli all`` records ``pipeline="all"`` error and nothing else.

    Mutation detected: emitting the per-domain label (for example
    ``pipeline="ingest_schedule"``) from ``all_cmd``'s except block, or
    recording a success alongside the error (``run.py`` L453-L456). The
    third assertion is deliberate negative space: the aggregate handler
    must NOT touch the per-pipeline series, because the operator
    dashboard sums the two label families independently.
    """
    # Arrange
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[FIRST_ALL_PIPELINE],
        "run",
        _make_failing_recorder(recorder, FIRST_ALL_PIPELINE),
    )

    # Act
    result = cli_runner.invoke(cli, ["all", "--season", config.DEFAULT_SEASON])

    # Assert
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli all` must exit {EXIT_FAILURE} when a pipeline raises; got "
        f"{result.exit_code}. stderr={result.stderr!r}"
    )
    observed_error = _runs_counter(ALL_PIPELINE_LABEL, OUTCOME_ERROR)
    observed_success = _runs_counter(ALL_PIPELINE_LABEL, OUTCOME_SUCCESS)
    observed_domain_error = _runs_counter(
        dict(DATA_SUBCOMMAND_LABELS)[FIRST_ALL_PIPELINE], OUTCOME_ERROR
    )
    assert observed_error == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{ALL_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_HIT} after one failed `cli all` invocation"
    )
    assert observed_success == EXPECTED_COUNTER_MISS, (
        f'{RUNS_COUNTER}{{pipeline="{ALL_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_SUCCESS}"}} is {observed_success}; expected '
        f"{EXPECTED_COUNTER_MISS} because the run failed"
    )
    assert observed_domain_error == EXPECTED_COUNTER_MISS, (
        f"`cli all` must record its outcome under the aggregate label "
        f'only; {RUNS_COUNTER}{{pipeline='
        f'"{dict(DATA_SUBCOMMAND_LABELS)[FIRST_ALL_PIPELINE]}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_domain_error}; '
        f"expected {EXPECTED_COUNTER_MISS}"
    )


# ---------------------------------------------------------------------------
# Section D — the ``all`` success counter and its exact label.
#
# ``tests/unit/test_cli.py`` already pins the ``all`` dispatch ORDER
# (L460) and exactly-once dispatch (L499); those tests are untouched. The
# NEW information here is the counter value and its exact label set,
# which nothing in the suite asserted. The ordering assertion is repeated
# only because it is what makes the counter assertion interpretable — a
# counter of 1.0 means little without knowing what actually ran.
# ---------------------------------------------------------------------------


def test_all_subcommand_increments_the_aggregate_success_counter_under_the_documented_label(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """A clean ``cli all`` records ``pipeline="all"`` success exactly once.

    Mutation detected: relabelling the aggregate series (the plausible
    typo is ``"ingest_all"``, matching the per-domain naming pattern),
    incrementing once per loop iteration instead of once per invocation,
    or also emitting an error outcome. The ``ALL_PIPELINE_LABEL_TYPO``
    assertion is what makes a label rename impossible to land silently:
    :meth:`get_counter_value` answers ``0.0`` for an unseen series, so a
    test that only checked the typo'd name would pass vacuously — here it
    is the correct name that must be ``1.0`` and the typo that must stay
    ``0.0``.
    """
    # Arrange
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)

    # Act
    result = cli_runner.invoke(
        cli,
        ["all", "--season", config.DEFAULT_SEASON],
        catch_exceptions=False,
    )

    # Assert
    assert result.exit_code == EXIT_SUCCESS, (
        f"`cli all` must exit {EXIT_SUCCESS} when every pipeline returns; "
        f"got {result.exit_code}. stderr={result.stderr!r}"
    )
    assert recorder == EXPECTED_ALL_ORDER, (
        f"`cli all` dispatch census={recorder!r}; expected "
        f"{EXPECTED_ALL_ORDER!r} (the ``order`` table at run.py L429-L435)"
    )
    observed_success = _runs_counter(ALL_PIPELINE_LABEL, OUTCOME_SUCCESS)
    observed_error = _runs_counter(ALL_PIPELINE_LABEL, OUTCOME_ERROR)
    observed_typo = _runs_counter(ALL_PIPELINE_LABEL_TYPO, OUTCOME_SUCCESS)
    assert observed_success == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{ALL_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_SUCCESS}"}} is {observed_success}; expected '
        f"exactly {EXPECTED_COUNTER_HIT} — one increment per invocation, "
        f"not one per pipeline in the order table"
    )
    assert observed_error == EXPECTED_COUNTER_MISS, (
        f'{RUNS_COUNTER}{{pipeline="{ALL_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_MISS} because nothing raised"
    )
    assert observed_typo == EXPECTED_COUNTER_MISS, (
        f"the aggregate label must be {ALL_PIPELINE_LABEL!r}, never "
        f"{ALL_PIPELINE_LABEL_TYPO!r}; "
        f'{RUNS_COUNTER}{{pipeline="{ALL_PIPELINE_LABEL_TYPO}",'
        f'outcome="{OUTCOME_SUCCESS}"}} is {observed_typo}; expected '
        f"{EXPECTED_COUNTER_MISS}"
    )


# ---------------------------------------------------------------------------
# Section E — ``ready`` translates the probe verdict into an exit code.
#
# These two tests SUPPLEMENT ``tests/unit/test_cli.py``
# ``test_ready_subcommand_when_configured`` (L612), which deliberately
# accepts ``exit_code in (0, 1)`` so a partial CI image cannot make it
# flake. That test is frozen and unmodified; determinism is obtained here
# by substituting the readiness probe, which lets BOTH directions of the
# single ``!= "ready"`` branch be pinned exactly.
# ---------------------------------------------------------------------------


def test_ready_subcommand_exits_one_and_still_echoes_the_body_when_not_ready(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """``cli ready`` echoes the probe body and THEN exits 1 when not ready.

    Mutations detected: dropping ``sys.exit(1)`` (``run.py`` L515) or
    inverting the ``!= "ready"`` comparison (L514) — either would exit
    ``0`` on a failed probe and tell systemd, Docker healthchecks and
    shell pipelines that a broken process is serviceable. Also detected:
    moving ``sys.exit`` ABOVE ``click.echo`` (L513), which would suppress
    the diagnostic body operators pipe into ``jq``; and switching the
    echo to ``err=True``, which would move it off stdout.
    """
    # Arrange — a deterministic not-ready verdict.
    monkeypatch.setattr(
        run_module.health,
        "check_readiness",
        _make_readiness_probe(NOT_READY_PROBE_RESULT),
    )

    # Act — default ``catch_exceptions=True``; Click records the
    # ``SystemExit`` raised by ``sys.exit`` either way.
    result = cli_runner.invoke(cli, ["ready"])

    # Assert
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli ready` must exit {EXIT_FAILURE} when the probe reports "
        f"{NOT_READY_PROBE_RESULT['status']!r}; got {result.exit_code}. "
        f"stdout={result.stdout!r}"
    )
    assert type(result.exception) is SystemExit, (
        f"`cli ready` must fail via sys.exit, not by letting an exception "
        f"escape; recorded {type(result.exception).__name__} "
        f"({result.exception!r}), expected SystemExit"
    )
    assert result.exception.code == EXIT_FAILURE, (
        f"`cli ready` must pass {EXIT_FAILURE} to sys.exit; got "
        f"{result.exception.code!r}"
    )
    assert json.loads(result.stdout) == NOT_READY_PROBE_RESULT, (
        f"`cli ready` must echo the probe body verbatim BEFORE exiting; "
        f"parsed stdout={json.loads(result.stdout)!r} expected "
        f"{NOT_READY_PROBE_RESULT!r}"
    )
    assert result.stderr == "", (
        f"`cli ready` must emit the probe body on stdout, not stderr; "
        f"stderr={result.stderr!r}"
    )


def test_ready_subcommand_exits_zero_and_echoes_the_body_when_ready(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """``cli ready`` exits 0 and echoes the body verbatim when the probe is ready.

    Mutation detected: inverting the ``!= "ready"`` comparison at
    ``run.py`` L514, which would exit ``1`` on a healthy process and make
    every orchestrator treat the service as permanently unready. Paired
    with the not-ready test above, this pins BOTH directions of that
    single branch — which is strictly stronger than the frozen
    ``exit_code in (0, 1)`` assertion it supplements, while leaving that
    assertion byte-for-byte intact.
    """
    # Arrange — a deterministic ready verdict.
    monkeypatch.setattr(
        run_module.health,
        "check_readiness",
        _make_readiness_probe(READY_PROBE_RESULT),
    )

    # Act
    result = cli_runner.invoke(cli, ["ready"], catch_exceptions=False)

    # Assert
    assert result.exit_code == EXIT_SUCCESS, (
        f"`cli ready` must exit {EXIT_SUCCESS} when the probe reports "
        f"{READY_PROBE_RESULT['status']!r}; got {result.exit_code}. "
        f"stdout={result.stdout!r}"
    )
    assert json.loads(result.stdout) == READY_PROBE_RESULT, (
        f"`cli ready` must echo the probe body verbatim; parsed "
        f"stdout={json.loads(result.stdout)!r} expected "
        f"{READY_PROBE_RESULT!r}"
    )
    assert result.stderr == "", (
        f"`cli ready` must emit the probe body on stdout, not stderr; "
        f"stderr={result.stderr!r}"
    )
