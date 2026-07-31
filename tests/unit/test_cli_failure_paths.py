"""CLI failure-path contracts — the ``except Exception`` boundary of ``run.py``.

What this module pins
---------------------
1. **Non-zero exit on failure.** Each of the five data subcommands
   (``players``, ``teams``, ``games``, ``lineups``, ``schedule``) exits
   ``1`` when its pipeline raises, because every ``except Exception``
   block in ``run.py`` ends with a bare ``raise``.
2. **Exception fidelity, asserted as OBJECT IDENTITY.** Each failure test
   constructs the exception it injects and requires the CLI to hand back
   that very object (``result.exception is injected_failure``). Identity
   is strictly stronger than a class-and-message match: it rejects
   swallowing, re-wrapping in another class, AND re-raising a same-class
   look-alike built from the original's message. The companion
   ``type(result.exception)`` and ``str(result.exception)`` assertions
   are diagnostics, so a failure report names what actually arrived.
3. **Metric label sets and values.** ``pipeline_runs_total`` is
   incremented exactly once with ``outcome="error"`` on the failure path
   and exactly once with ``outcome="success"`` on the happy path, under
   the ``pipeline`` label value belonging to the invoked subcommand.
4. **``all`` is fail-fast.** ``all_cmd`` wraps its *whole* dispatch loop
   in ONE ``try``, so the first failing pipeline prevents every later
   pipeline from running.
5. **``ready`` translates status into an exit code.** ``ready_cmd``
   echoes the probe body and *then* calls ``sys.exit(1)`` when
   ``status != "ready"``.

``_build_collaborators`` is exercised *implicitly*: it is the first
statement of every data subcommand, so each invocation here constructs
the real ``RateLimiter``/``NBAClient``/``CSVWriter``/``CheckpointManager``
graph against the ``tmp_path``-rooted config. It is never imported,
promoted, or given a test-only hook.

``catch_exceptions`` posture
----------------------------
Failure-path invocations use :meth:`CliRunner.invoke`'s **default**
``catch_exceptions=True`` so the propagated exception is recorded on
``result.exception`` where it can be asserted; ``catch_exceptions=False``
would let the injected exception escape ``invoke()`` and abort the test
instead of being measured. Happy-path invocations pass
``catch_exceptions=False``, because no exception is expected there.

Isolation
---------
Every test consumes ``tmp_output_dir`` and ``tmp_log_dir`` so no operator
directory is touched, and relies on the three autouse fixtures in
``tests/conftest.py`` that reset the correlation ID, the metrics
registry, and the logger handlers before AND after every test. That
registry reset is what makes the exact ``== 0.0`` assertions meaningful
rather than vacuous: a label set that was never incremented reads
``0.0`` by the Prometheus convention documented on
``MetricsRegistry.get_counter_value``.

Every test below names the specific mutation it detects.
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
# Structural constants derived from ``run.py``'s documented contract.
# Each derivation is spelled out beside the literal it produces.
# ---------------------------------------------------------------------------

#: The counter ``run.py`` increments on both the success and the failure
#: path of every subcommand. It is pre-registered in
#: :mod:`utils.metrics`, which is why :meth:`get_counter_value` returns a
#: well-defined ``0.0`` — rather than raising — for a label set that was
#: never incremented.
RUNS_COUNTER: str = "pipeline_runs_total"

#: Pairs of (CLI subcommand name, metric ``pipeline`` label) for all five
#: data subcommands. The two strings DIFFER — the CLI name is ``teams``
#: while the label is ``ingest_teams`` — so each pair is spelled out
#: literally instead of being derived with an ``f"ingest_{name}"``
#: expression. A literal table turns a label rename in ``run.py`` into a
#: failure here; a derived one would silently follow the rename.
DATA_SUBCOMMAND_LABELS: tuple = (
    ("players", "ingest_players"),
    ("teams", "ingest_teams"),
    ("games", "ingest_games"),
    ("lineups", "ingest_lineups"),
    ("schedule", "ingest_schedule"),
)

#: The aggregate subcommand's ``pipeline`` label. ``run.py`` uses the
#: bare marker ``"all"`` — NOT ``"ingest_all"`` — to distinguish
#: whole-run outcomes from per-pipeline outcomes.
ALL_PIPELINE_LABEL: str = "all"

#: The label the aggregate subcommand must NEVER emit. Asserting that
#: this series stays at zero converts a silent label typo into a test
#: failure, because :meth:`get_counter_value` reports ``0.0`` for an
#: unseen series instead of raising.
ALL_PIPELINE_LABEL_TYPO: str = "ingest_all"

OUTCOME_SUCCESS: str = "success"
OUTCOME_ERROR: str = "error"

#: ``run.py`` increments ``pipeline_runs_total`` EXACTLY once per
#: invocation — every ``metrics.registry.inc`` call sits outside any loop
#: — so one invocation moves the observed series to ``1.0`` and leaves
#: every other series at ``0.0``.
EXPECTED_COUNTER_HIT: float = 1.0
EXPECTED_COUNTER_MISS: float = 0.0

#: Exit code Click reports when a command callback lets an exception
#: escape, which is what ``run.py``'s bare ``raise`` statements cause. It
#: is also the literal argument ``ready_cmd`` hands to ``sys.exit``.
EXIT_FAILURE: int = 1

EXIT_SUCCESS: int = 0

#: The ordered dispatch table of ``run.py``'s ``all_cmd``: the binding
#: run order Schedule, Games, Teams, Players, Lineups. That order is a
#: run-sequencing contract, not a file-coupling one — Games enumerates
#: ``GAME_ID`` values by calling the Schedule endpoint helper directly
#: and never reads ``schedule.csv``.
EXPECTED_ALL_ORDER: List[str] = ["schedule", "games", "teams", "players", "lineups"]

FIRST_ALL_PIPELINE: str = "schedule"

#: The complete dispatch record ``all`` may show once its FIRST pipeline
#: fails. Derived structurally rather than observed: ``all_cmd`` wraps
#: the entire ``for`` loop in ONE ``try``, so the raise unwinds the loop
#: instead of advancing to the next entry.
EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE: List[str] = ["schedule"]

#: Message carried by the injected exception. Each failure test builds
#: its own instance with this text and then asserts OBJECT IDENTITY
#: against it; the message equality assertion is kept as a secondary
#: diagnostic so a failure report shows *what* arrived, not merely that
#: the wrong object did.
INJECTED_FAILURE_MESSAGE: str = "injected pipeline failure for CLI failure-path testing"

#: Deterministic stand-in results for :func:`utils.health.check_readiness`.
#: ``ready_cmd`` feeds the value straight to :func:`json.dumps` and then
#: indexes it with ``["status"]``, so each must be JSON-serializable and
#: must carry a ``"status"`` key or the callback would raise ``KeyError``
#: before reaching the branch under test. The nested sub-probe shape
#: mirrors :mod:`utils.health`. Neither dict is ever mutated, by this
#: module or by the production code.
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
#: ingest_<domain>`` and resolves ``.run`` at CALL time, so patching the
#: module object here is what the production dispatch actually sees.
#: Patching a name local to this test file, or patching
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
# The pipeline dispatch doubles below are handwritten so that they carry
# the real ``run(client, writer, checkpoint, season, logger=None,
# metrics=None)`` signature and no catch-all ``**kwargs``: interface
# drift then surfaces as a loud ``TypeError`` instead of being absorbed
# by attribute-access magic.
# ---------------------------------------------------------------------------


class _InjectedPipelineFailure(RuntimeError):
    """Distinctive exception injected into a pipeline to drive the except path.

    Each failure test constructs ONE instance of this class, hands it to
    :func:`_make_failing_recorder`, and afterwards asserts
    ``result.exception is <that instance>``. Object identity is the real
    contract — ``run.py`` closes every ``except Exception`` block with a
    bare ``raise``, so the very object the pipeline raised must arrive at
    the caller with its ``__traceback__``, ``__cause__`` and any custom
    attributes intact.

    The bespoke class is retained alongside that identity check as a
    diagnostic: ``type(result.exception) is _InjectedPipelineFailure``
    names the wrapper class in a failure report when something arrives
    wrapped, for example in a :class:`click.ClickException`.
    """


def _make_recorder(recorder: List[str], domain: str) -> Callable[..., None]:
    """Return a spy that appends ``domain`` to ``recorder`` and returns ``None``.

    Produced by a FACTORY so that ``domain`` is captured by value.
    Installing the closures directly inside a ``for`` loop would make
    every spy share the loop variable's final value — the classic
    late-binding bug — and every ordering assertion in this module would
    then be meaningless while still appearing to pass.

    The spy reproduces the REAL collaborator signature exactly and
    accepts NO catch-all ``**kwargs``. Every production pipeline is
    declared ``run(client, writer, checkpoint, season, logger=None,
    metrics=None)``, so this double stands in for that interface without
    widening it: the four leading parameters carry no defaults because
    ``run.py`` supplies all four at every dispatch site, while ``logger``
    and ``metrics`` default to ``None`` because production defaults them
    and ``run.py`` never passes them. Returning ``None`` mirrors the
    production pipelines.

    Because the signature is exact, a misspelled dispatch keyword
    (``checkpont=checkpoint``), a dropped required keyword, or an argument
    the real pipelines do not accept raises ``TypeError`` inside
    ``run.py``'s ``try`` block instead of being absorbed silently.
    """

    def _run(
        client: Any,
        writer: Any,
        checkpoint: Any,
        season: str,
        logger: Any = None,
        metrics: Any = None,
    ) -> None:
        recorder.append(domain)

    return _run


def _make_failing_recorder(
    recorder: List[str],
    domain: str,
    failure: BaseException,
) -> Callable[..., None]:
    """Return a spy that records ``domain`` and THEN raises ``failure`` itself.

    Recording *before* raising is deliberate: it proves the failing
    pipeline actually started, so a later ``recorder == [...]`` assertion
    distinguishes "dispatched and then failed" from "never dispatched at
    all".

    The signature is the real ``pipelines.ingest_<domain>.run`` interface
    with no catch-all ``**kwargs``, for the interface-drift reason spelled
    out on :func:`_make_recorder`. Keeping both doubles
    signature-identical matters here: under a dispatch-keyword mutation
    this spy raises ``TypeError`` *instead of* the injected failure, so
    the exception assertions fail loudly rather than passing on a
    coincidentally non-zero exit code, and ``recorder`` stays honest
    because a drifted call never reaches the ``append``.

    ``failure`` is the exception **instance** the caller wants raised,
    and it is a REQUIRED parameter with no default on purpose: if this
    factory constructed the exception itself, the calling test would hold
    no reference to the object that crossed the CLI boundary and could
    only compare its class and message. Injecting the instance is what
    lets each caller assert ``result.exception is <that instance>`` and so
    reject a re-raised look-alike.
    """

    def _run(
        client: Any,
        writer: Any,
        checkpoint: Any,
        season: str,
        logger: Any = None,
        metrics: Any = None,
    ) -> None:
        recorder.append(domain)
        raise failure

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

    ``ready_cmd`` calls ``health.check_readiness()`` with no arguments,
    hands the value to :func:`json.dumps`, and then indexes it with
    ``["status"]``. The stand-in must therefore be zero-argument, must
    return a JSON-serializable object, and must carry a ``"status"`` key.
    Substituting a deterministic verdict is what makes the readiness exit
    code independent of the environment the suite runs in.
    """

    def _check_readiness() -> Dict[str, Any]:
        return result

    return _check_readiness


def _runs_counter(pipeline: str, outcome: str) -> float:
    """Read ``pipeline_runs_total`` for one exact ``(pipeline, outcome)`` pair.

    Reads the REAL registry singleton that ``run.py`` writes to rather
    than a substitute, because the contract under test is the value and
    the label set that production code actually emits. A mock registry
    would happily record whatever labels the CLI passed and prove nothing
    about their correctness.
    """
    return metrics.registry.get_counter_value(
        RUNS_COUNTER,
        labels={"pipeline": pipeline, "outcome": outcome},
    )


# ---------------------------------------------------------------------------
# Section A — the ``except Exception`` path of the five data subcommands.
#
# Each block logs the failure, records the error outcome, and ends with a
# bare ``raise``: the very exception object the pipeline raised reaches
# the caller, the process exits non-zero, and no sibling pipeline is
# dispatched.
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
    """A raising pipeline exits 1, returns the SAME exception object, records error only.

    Mutations detected:

    * Deleting the bare ``raise`` that closes each ``except Exception``
      block — the callback would return normally, Click would exit ``0``
      and ``result.exception`` would be ``None``, reporting a total
      pipeline failure as a success.
    * Wrapping the failure in a different class, for example ``raise
      RuntimeError("pipeline failed") from exc``.
    * **Re-raising a same-class look-alike** — ``except Exception as
      exc: raise type(exc)(str(exc))``. It preserves the class AND the
      message, so it is invisible to a class-plus-message check; only the
      ``is`` assertion below rejects it, and the rebuilt object has lost
      ``__cause__``, the original ``__traceback__`` and every custom
      attribute an operator's handler may read.
    * Labelling the failure path ``outcome="success"``, incrementing both
      series, incrementing the error series twice, or misspelling the
      ``pipeline`` label. Because :meth:`get_counter_value` returns a
      well-defined ``0.0`` for a never-incremented label set, the
      :data:`EXPECTED_COUNTER_MISS` assertion is exact rather than
      vacuous, and a typo'd label leaves the asserted series at ``0.0``.
    """
    # Arrange — census spies on all five pipelines, then override the one
    # under test with a recording raiser. The exception is built HERE, so
    # this test holds the only reference the assertions need in order to
    # compare identity rather than resemblance.
    injected_failure = _InjectedPipelineFailure(INJECTED_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[subcommand],
        "run",
        _make_failing_recorder(recorder, subcommand, injected_failure),
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
    assert result.exception is injected_failure, (
        f"`cli {subcommand}` must propagate the ORIGINAL exception "
        f"object, not a copy of it; got id={id(result.exception)} "
        f"({result.exception!r}) expected id={id(injected_failure)} "
        f"({injected_failure!r}). A same-class, same-message object at a "
        f"different id means the failure was re-wrapped instead of "
        f"re-raised"
    )
    # Diagnostic companions to the identity assertion above: on failure
    # they say *how* the object differs — wrong class versus wrong
    # instance of the right class.
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
# A subcommand whose pipeline returns must record exactly one increment
# under ``outcome="success"`` for its own ``pipeline`` label and leave the
# ``error`` series at zero.
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
    also touching the ``error`` series on a clean run. Each of the five
    data subcommands' success handlers increments exactly once, so
    ``== 1.0`` is asserted rather than ``> 0``.
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
# Section C — the ``all`` fail-fast guarantee.
#
# ``all_cmd`` wraps the ENTIRE dispatch loop in one ``try`` whose handler
# ends in a bare ``raise``, so the first failure unwinds the loop.
#
# The mutation these tests rule out is CATCH-AND-CONTINUE: an ``except``
# placed per-iteration that records the error and moves on, or one that
# drops the ``raise``. That version runs all five pipelines and exits
# ``0`` despite a failed dependency. Relocating the ``try``/``except``
# inside the loop while KEEPING the bare ``raise`` still aborts, so it is
# the swallowing — not the placement — that the ordered dispatch census
# below detects.
# ---------------------------------------------------------------------------


def test_all_subcommand_is_fail_fast_and_never_reaches_later_pipelines(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """``cli all`` abandons the remaining pipelines and records aggregate error only.

    Mutations detected:

    * Catching each iteration's failure inside the ``for`` loop and
      continuing to the next entry — equivalently, dropping the bare
      ``raise`` from the handler — which would run all five pipelines and
      exit ``0`` despite the schedule failure. The whole-list assertion
      below proves not merely that ``schedule`` ran, but that the four
      later pipelines did NOT.
    * Re-wrapping the propagated failure in ``all_cmd``'s ``except``
      block, including the same-class form ``raise type(exc)(str(exc))``
      that survives a class-and-message check: the aggregate command must
      hand back the very object the failing pipeline raised.
    * Emitting the per-domain label (for example
      ``pipeline="ingest_schedule"``) from ``all_cmd``'s except block, or
      recording a success alongside the error. The final assertion is
      deliberate negative space: the aggregate handler must not touch the
      per-pipeline series, because the two label families are summed
      independently.
    """
    # Arrange — census spies on all five, then fail the FIRST entry of the
    # documented order so that every other pipeline is downstream of it.
    # The exception instance is built here so identity, not resemblance,
    # can be asserted after the aggregate command unwinds.
    injected_failure = _InjectedPipelineFailure(INJECTED_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[FIRST_ALL_PIPELINE],
        "run",
        _make_failing_recorder(recorder, FIRST_ALL_PIPELINE, injected_failure),
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
    assert result.exception is injected_failure, (
        f"`cli all` must propagate the ORIGINAL exception object raised "
        f"by {FIRST_ALL_PIPELINE}, not a copy of it; got "
        f"id={id(result.exception)} ({result.exception!r}) expected "
        f"id={id(injected_failure)} ({injected_failure!r}). A same-class, "
        f"same-message object at a different id means the aggregate "
        f"handler re-wrapped the failure instead of re-raising it"
    )
    # Diagnostic companion: distinguishes a wrong CLASS from a wrong
    # INSTANCE of the right class in the failure report.
    assert type(result.exception) is _InjectedPipelineFailure, (
        f"`cli all` must propagate the original exception; got "
        f"{type(result.exception).__name__} ({result.exception!r}), "
        f"expected _InjectedPipelineFailure"
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
# A clean aggregate run must record exactly one increment under
# ``pipeline="all"``, ``outcome="success"``. The dispatch order is
# asserted alongside it: ``1.0`` means "one increment per invocation"
# only once the ordered list confirms five pipelines executed.
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
    or also emitting an error outcome. Because
    :meth:`get_counter_value` answers ``0.0`` for an unseen series, the
    correct name must read ``1.0`` while
    :data:`ALL_PIPELINE_LABEL_TYPO` must stay ``0.0`` — a test that
    checked only the typo'd name would pass vacuously.
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
        f"{EXPECTED_ALL_ORDER!r} (the ``order`` table inside ``all_cmd``)"
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
# ``ready_cmd`` has exactly one branch: it echoes the probe body, then
# exits ``1`` when ``status != "ready"``. Substituting a deterministic
# probe removes the environmental variability that would otherwise decide
# the exit code, so both directions of that branch are pinned exactly —
# a not-ready verdict exits ``1``, a ready verdict exits ``0``, and the
# JSON body is emitted on stdout either way.
# ---------------------------------------------------------------------------


def test_ready_subcommand_exits_one_and_still_echoes_the_body_when_not_ready(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """``cli ready`` echoes the probe body and THEN exits 1 when not ready.

    Mutations detected: dropping ``sys.exit(1)`` or inverting the
    ``!= "ready"`` comparison — either exits ``0`` on a failed probe and
    tells an orchestrator that a broken process is serviceable. Also
    detected: moving ``sys.exit`` ABOVE ``click.echo``, which suppresses
    the diagnostic body; and switching the echo to ``err=True``, which
    moves it off stdout.
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

    Mutation detected: inverting the ``!= "ready"`` comparison, which
    would exit ``1`` on a healthy process and make an orchestrator treat
    the service as permanently unready.
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
