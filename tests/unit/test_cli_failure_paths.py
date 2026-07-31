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

# ---------------------------------------------------------------------------
# Constants for Section F — the failure-disclosure CHARACTERIZATION.
#
# Every literal below is DERIVED from ``run.py`` and ``utils/logger.py``
# and the derivation is written out beside it. None of them was obtained
# by capturing output and pasting it back.
# ---------------------------------------------------------------------------

#: Header :mod:`traceback` writes before the first frame of a rendered
#: stack. Its presence in a published record is the whole substance of
#: the disclosure being characterised.
TRACEBACK_HEADER: str = "Traceback (most recent call last)"

#: Prefix :mod:`traceback` writes before each frame's ABSOLUTE source
#: path. Counting it counts disclosed filesystem paths.
TRACEBACK_FRAME_PREFIX: str = 'File "'

#: Exactly ONE rendered stack reaches each sink per failed invocation.
#: Derivation: every ``except Exception`` block in ``run.py`` calls
#: ``log.exception`` exactly once, ``log.exception`` is
#: ``log.error(..., exc_info=True)``, and the injected exception is
#: raised bare — no ``from`` clause and no nested handler — so the
#: formatter emits one stack with no "During handling of the above
#: exception" or "The above exception was the direct cause" continuation.
EXPECTED_TRACEBACK_HEADERS: int = 1

#: Exactly TWO frames are disclosed. Derivation: the rendered stack spans
#: the frames between the ``try`` that caught the exception and the
#: ``raise``. That is the dispatch statement inside ``run.py`` plus the
#: ``_run`` body of this module's own spy — one call, therefore two
#: frames. One of them is ``run.py``'s absolute path; the other is this
#: test module's.
EXPECTED_TRACEBACK_FRAMES: int = 2

#: Each ``log.exception`` call renders its format string once.
EXPECTED_FAILURE_EVENT_OCCURRENCES: int = 1

#: The injected message surfaces exactly once — on the stack's terminal
#: ``<qualified class>: <message>`` line. The intermediate frame lines
#: quote source text (``raise failure``), not the message, so no second
#: occurrence exists.
EXPECTED_INJECTED_MESSAGE_OCCURRENCES: int = 1

#: (CLI subcommand, pipeline module key to break) for all SIX
#: ``except Exception`` handlers in ``run.py`` — the five data
#: subcommands plus the aggregate. ``all`` is driven by breaking
#: ``schedule`` because that is the FIRST entry of
#: :data:`EXPECTED_ALL_ORDER`, so the aggregate handler is reached on the
#: first dispatch.
FAILURE_HANDLER_TARGETS: tuple = (
    ("players", "players"),
    ("teams", "teams"),
    ("games", "games"),
    ("lineups", "lineups"),
    ("schedule", "schedule"),
    ("all", FIRST_ALL_PIPELINE),
)

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


# ---------------------------------------------------------------------------
# Section F — CHARACTERIZATION of a KNOWN DEFECT in the failure handlers.
#
# * **This section documents behaviour that is WRONG, and asserts it on
#   purpose.** It is the remedy the Agent Action Plan prescribes in
#   §0.4.5 for a defect whose fix lies outside the authorised scope:
#   "leave the source untouched and instead add a test documenting the
#   current behavior with a comment naming the defect."
#
# * **The defect.** Each of the six ``except Exception`` blocks in
#   ``run.py`` calls ``log.exception(...)``, which is
#   ``log.error(..., exc_info=True)``. ``utils/logger.py::_configure``
#   attaches BOTH a ``logging.StreamHandler(sys.stdout)`` AND a
#   ``logging.handlers.RotatingFileHandler`` to the root logger at
#   ``config.LOG_LEVEL`` (default ``INFO``), so that one ERROR record is
#   rendered — traceback and all — into the operator's console AND into
#   the durable log file. The rendered stack carries absolute filesystem
#   paths and the exception's message. Classified CWE-209 (generation of
#   error message containing sensitive information), CWE-497 (exposure of
#   system data to an unauthorised control sphere) and CWE-532
#   (insertion of sensitive information into a log file).
#
# * **Why it is not fixed here.** ``run.py`` is named out of scope by AAP
#   §0.8.2, which additionally states that "no logging statement is
#   altered anywhere"; §0.10.2 makes the ``endpoints/schedule.py``
#   ``GAME_ID`` padding fix "the single exception exercised" and states
#   that "no other source change is permitted". Constraint C2 outranks
#   the optional fix, so the AAP's own fallback applies and this
#   characterization is the sanctioned response.
#
# * **The minimal fix, for whoever is authorised to apply it.** In each
#   of the six handlers replace ``log.exception(...)`` with
#   ``log.error(...)`` carrying no ``exc_info`` — emitting only the
#   event, the subcommand, the season, the exception CLASS name and
#   ``detail=suppressed`` — and move the full ``exc_info`` render to a
#   separate ``log.debug`` record that the default ``INFO`` level
#   discards. Separately, add a ``main(argv=None)`` process boundary that
#   delegates to ``cli.main(standalone_mode=True)`` and converts an
#   escaped ``Exception`` into ``SystemExit(1) from None``, then dispatch
#   ``if __name__ == "__main__":`` through it; without that boundary
#   CPython renders a SECOND stack on stderr that no in-process test can
#   observe, because ``CliRunner`` intercepts the exception first.
#
# * **Measured evidence for the stderr half, recorded here because no
#   in-process test can reach it.** Running the real module as a child
#   process — ``run.py teams --season 2025-26`` against a non-routable
#   ``NBA_API_BASE_URL`` with the log and output directories redirected
#   into a sandbox — exits ``1`` and produces THREE renders of the same
#   failure: stdout carries 4 ``Traceback`` headers / 31 ``File "``
#   frames / 13 distinct absolute paths, the durable log is
#   BYTE-IDENTICAL to stdout at the same 4 / 31 / 13, and stderr carries
#   4 / 37 / 14. Those 6 extra stderr frames and the 14th path are the
#   top-level CPython render that the ``main(argv=None)`` boundary above
#   would suppress; the in-process assertions below pin the stdout and
#   durable-log halves only. Both sinks additionally spell out the full
#   27-parameter outbound ``leaguedashteamstats`` query string. The
#   demotion is safe for operators because the same run already emits the
#   actionable structured line ``NBAClient request exhausted retries``,
#   so triage never depended on the stack.
#
# * **When that fix lands, THIS TEST MUST FAIL — that is its purpose.**
#   Replace the four disclosure assertions below with their redacted
#   counterparts: ``log_text.count(TRACEBACK_HEADER) == 0``,
#   ``log_text.count(TRACEBACK_FRAME_PREFIX) == 0``,
#   ``str(run_module.__file__) not in log_text`` and
#   ``log_text.count(INJECTED_FAILURE_MESSAGE) == 0``, while keeping the
#   ``run.failed`` event assertion at ``1`` so the operator still gets an
#   actionable single line.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("subcommand,pipeline_key", FAILURE_HANDLER_TARGETS)
def test_failure_handler_publishes_the_whole_traceback_to_console_and_durable_log(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    subcommand: str,
    pipeline_key: str,
) -> None:
    """CHARACTERIZATION — a failed subcommand leaks its stack to BOTH sinks.

    Pins, for all six ``except Exception`` handlers at once, that the
    rendered stack is published to the console handler AND to the durable
    ``config.LOG_FILE``, and that the two renderings are byte-identical
    because a single ``LogRecord`` is formatted by two handlers holding
    equivalent formatters at the same level.

    Mutations detected:

    * Detaching either handler in ``utils/logger.py::_configure``, or
      giving one of them a different level, breaks the dual-sink identity
      assertion — the console and the durable log would no longer carry
      the same bytes.
    * Raising ``config.LOG_LEVEL`` above ``ERROR``, or filtering the
      ``run.failed`` event out, drops the event-occurrence assertion from
      ``1`` to ``0``.
    * Applying the redaction fix described in this section's banner turns
      every disclosure assertion below red, which is exactly the intended
      tripwire: the fixer is then routed to the replacement assertions
      spelled out there.
    * Widening the dispatch depth between ``run.py``'s ``try`` and the
      raising callee changes the disclosed frame count away from
      :data:`EXPECTED_TRACEBACK_FRAMES`.
    """
    # Arrange — census spies on all five pipelines so a mis-dispatch lands
    # on a spy and never on a production pipeline that would try to reach
    # the NBA Stats API, then break the one pipeline this case targets.
    injected_failure = _InjectedPipelineFailure(INJECTED_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[pipeline_key],
        "run",
        _make_failing_recorder(recorder, pipeline_key, injected_failure),
    )
    expected_event = (
        f"run.failed subcommand={subcommand} season={config.DEFAULT_SEASON}"
    )

    # Act
    result = cli_runner.invoke(cli, [subcommand, "--season", config.DEFAULT_SEASON])

    # Assert — the invocation really did take the failure path.
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli {subcommand}` must exit {EXIT_FAILURE} when "
        f"{pipeline_key} raises; got {result.exit_code}. "
        f"stderr={result.stderr!r}"
    )

    log_text = (tmp_log_dir / "pipeline.log").read_text(encoding="utf-8")

    # The event line itself — this assertion stays green after the fix.
    assert log_text.count(expected_event) == EXPECTED_FAILURE_EVENT_OCCURRENCES, (
        f"`cli {subcommand}` must log {expected_event!r} exactly "
        f"{EXPECTED_FAILURE_EVENT_OCCURRENCES} time in "
        f"{config.LOG_FILE}; counted "
        f"{log_text.count(expected_event)}"
    )

    # The four disclosure assertions. Each documents a leak, not a
    # desired property; see this section's banner.
    assert log_text.count(TRACEBACK_HEADER) == EXPECTED_TRACEBACK_HEADERS, (
        f"CHARACTERIZATION drift: `cli {subcommand}` renders "
        f"{log_text.count(TRACEBACK_HEADER)} stack(s) into the durable "
        f"log; this defect currently produces exactly "
        f"{EXPECTED_TRACEBACK_HEADERS}. If the redaction fix has landed, "
        f"update this section's assertions to the redacted counterparts"
    )
    assert log_text.count(TRACEBACK_FRAME_PREFIX) == EXPECTED_TRACEBACK_FRAMES, (
        f"CHARACTERIZATION drift: `cli {subcommand}` discloses "
        f"{log_text.count(TRACEBACK_FRAME_PREFIX)} absolute source "
        f"path(s); this defect currently discloses exactly "
        f"{EXPECTED_TRACEBACK_FRAMES}"
    )
    assert str(run_module.__file__) in log_text, (
        f"CHARACTERIZATION drift: the absolute path of run.py "
        f"({run_module.__file__}) is no longer disclosed by `cli "
        f"{subcommand}`. If the redaction fix has landed, invert this "
        f"assertion to `not in`"
    )
    assert (
        log_text.count(INJECTED_FAILURE_MESSAGE)
        == EXPECTED_INJECTED_MESSAGE_OCCURRENCES
    ), (
        f"CHARACTERIZATION drift: the raised exception's message appears "
        f"{log_text.count(INJECTED_FAILURE_MESSAGE)} time(s) in the "
        f"durable log; this defect currently surfaces it exactly "
        f"{EXPECTED_INJECTED_MESSAGE_OCCURRENCES} time"
    )

    # Dual-sink identity: ONE record, TWO handlers, byte-identical
    # renderings. This is what makes the durable log as sensitive as the
    # console, and it is the structural reason redacting only one sink
    # would be insufficient.
    assert result.stdout == log_text, (
        f"`cli {subcommand}` must render the SAME bytes to the console "
        f"handler and to {config.LOG_FILE}, because one LogRecord is "
        f"formatted by two handlers at the same level; console carries "
        f"{len(result.stdout)} bytes and the durable log carries "
        f"{len(log_text)}"
    )


# ---------------------------------------------------------------------------
# Section G — CHARACTERIZATION of the CWE-117 AMPLIFICATION path, end to end.
#
# * **This section asserts behaviour that is WRONG, on purpose**, for the
#   same reason Section F does: AAP §0.4.5 prescribes that a defect whose
#   fix lies outside the authorised scope be met by "a test documenting
#   the current behavior with a comment naming the defect."
#
# * **The defect, and why it needs a SECOND test.** Two separate
#   weaknesses compose into one exploitable outcome, and neither existing
#   test observes the composition:
#
#   1. ``utils/schema_normalizer.py::_build_dataframe`` interpolates the
#      upstream-controlled result-set name into both of its ``ValueError``
#      templates with a BARE ``{name}`` and no neutralisation, and
#      ``_snake_case`` re-punctuates the value without stripping control
#      characters. A newline inside an upstream ``name`` therefore
#      survives into the exception message. **This half is already
#      characterised** — at the message level — by
#      ``test_newline_in_result_set_name_forges_a_second_log_line`` in
#      ``tests/unit/utils/test_schema_normalizer_malformed_input.py``.
#   2. Every ``except Exception`` handler in ``run.py`` publishes that
#      exception through ``log.exception``, and ``utils/logger.py`` sends
#      the record to BOTH a console ``StreamHandler`` and a durable
#      ``RotatingFileHandler``. **This half is characterised** by
#      Section F above.
#
#   What NEITHER pins is the composition: driven end to end, the embedded
#   newline terminates the genuine record early and the remainder is
#   emitted as a STANDALONE PHYSICAL LOG LINE carrying no
#   ``config.LOG_FORMAT`` prefix at all — no timestamp, no level, no
#   ``corr=`` field, no logger name. It is therefore indistinguishable
#   from a record the logging subsystem itself wrote, in the operator's
#   console AND in the durable forensic log. That is CWE-117, improper
#   output neutralization for logs, and a forged audit line is materially
#   worse than a split message, so it earns its own test at the seam
#   where it actually becomes reachable.
#
# * **Reachability, stated honestly.** Result-set names arrive from the
#   NBA Stats envelope, not from an end user, and ``session.verify=True``
#   blocks a MITM rewrite, so this is a hardening gap rather than a
#   directly attacker-reachable vulnerability. It is characterised
#   because an untrusted-input-shaped defect that no test names is one
#   nobody finds later.
#
# * **Why it is not fixed here.** AAP §0.8.2 places ALL of ``utils/*.py``
#   AND ``run.py`` out of scope and states that "no logging statement is
#   altered anywhere"; §0.10.2 makes the ``endpoints/schedule.py``
#   ``GAME_ID`` padding fix "the single exception exercised" and states
#   that "no other source change is permitted". Constraint C2 outranks
#   the optional fix.
#
# * **The minimal fix, for whoever is authorised to apply it.** Render the
#   name through ``repr`` in both ``_build_dataframe`` templates —
#   ``f"Result set {name!r} row ..."`` — or escape explicitly with
#   ``name.replace("\n", "\\n").replace("\r", "\\r")``. The ``repr`` form
#   is provably compatible with the seven exact messages AAP §0.4.2.3
#   pins, because every one of those fixtures uses the plain name ``"t"``
#   and ``repr("t")`` is ``"'t'"`` — the same characters the current
#   ``'{name}'`` template already emits. Neutralisation therefore
#   activates only for names that really contain a quote or a control
#   character. Fixing only ``run.py``'s disclosure would NOT close this:
#   the newline would still split the message wherever that message is
#   surfaced.
#
# * **When that fix lands, THIS TEST MUST FAIL — that is its purpose.**
#   Replace the expectations below with the neutralised counterparts:
#   ``len(str(result.exception).splitlines()) == 1``; the durable log
#   contains ZERO occurrences of :data:`FORGED_LOG_LINE`; every physical
#   line of the log carries the :data:`LOG_RECORD_PREFIX_MARKER`; and the
#   message contains the escaped two-character sequence ``\\n`` rather
#   than a real line break. Keep the exit-code and counter assertions
#   green — the failure must still be reported and still be counted.
# ---------------------------------------------------------------------------

#: An upstream result-set name carrying an embedded newline followed by a
#: forged audit payload. Chosen so ``utils.schema_normalizer._snake_case``
#: is the IDENTITY on it: the value is already lower-case and contains no
#: CamelCase boundary and no letter/digit boundary, so the helper returns
#: it unchanged and every expectation below follows from the message
#: template alone rather than from any observed output.
LOG_INJECTION_TABLE_NAME: str = "legit\nforged_admin_login_success"

#: The exception message the normalizer's row-type guard produces for that
#: name. DERIVED from the template in
#: ``utils/schema_normalizer.py::_build_dataframe``::
#:
#:     f"Result set '{name}' row {idx} is {type(row).__name__}, "
#:     f"expected list/tuple"
#:
#: substituting ``name`` = :data:`LOG_INJECTION_TABLE_NAME`, ``idx`` = 0
#: (the payload carries a single row) and ``type(row).__name__`` = ``dict``
#: (the row is supplied as a mapping, which is what trips that guard).
EXPECTED_INJECTED_VALUE_ERROR_MESSAGE: str = (
    "Result set 'legit\nforged_admin_login_success' row 0 is dict, "
    "expected list/tuple"
)

#: The message spans exactly TWO physical lines, because the substituted
#: name contributes exactly one newline and the template contributes none.
EXPECTED_INJECTED_MESSAGE_LINE_COUNT: int = 2

#: The genuine record's terminal line, truncated at the injected newline.
#: DERIVED as :mod:`traceback`'s ``"<class>: <message>"`` rendering —
#: ``"ValueError: "`` — followed by the message text up to that newline,
#: which is ``"Result set '"`` plus the name's pre-newline part
#: ``"legit"``.
EXPECTED_TRUNCATED_GENUINE_LINE: str = "ValueError: Result set 'legit"

#: The forged line: everything after the injected newline. DERIVED as the
#: name's post-newline part ``"forged_admin_login_success"`` followed by
#: the template's closing quote and tail. It begins with
#: attacker-supplied text and carries no formatter prefix whatsoever.
FORGED_LOG_LINE: str = (
    "forged_admin_login_success' row 0 is dict, expected list/tuple"
)

#: Exactly ONE forged line reaches each sink. Derivation: the schedule
#: handler calls ``log.exception`` once, that renders one stack, and the
#: stack's single terminal line splits into exactly one genuine remnant
#: plus one forged line.
EXPECTED_FORGED_LINE_OCCURRENCES: int = 1

#: The substring every GENUINE record line carries, read off
#: ``config.LOG_FORMAT`` =
#: ``"%(asctime)s %(levelname)s corr=%(correlation_id)s %(name)s
#: %(message)s"``. Its ABSENCE from a physical line is what makes that
#: line forgeable: an operator, and any log shipper doing line-oriented
#: parsing, has nothing left to distinguish it from real output. A
#: leading space is included so the marker cannot match inside a message
#: body that merely mentions ``corr=``.
LOG_RECORD_PREFIX_MARKER: str = " corr="

#: The subcommand driven end to end, and its metric ``pipeline`` label.
#: ``schedule`` is chosen because ``pipelines.ingest_schedule.run`` calls
#: ``normalize_result_sets`` directly on the fetched envelope — the
#: shortest real path from a malformed upstream payload to ``run.py``'s
#: failure handler, with no per-game Rule 6 wrapper in between.
INJECTION_SUBCOMMAND: str = "schedule"
INJECTION_PIPELINE_LABEL: str = "ingest_schedule"

#: The malformed envelope. A single result set whose NAME carries the
#: injected newline and whose single row is a ``dict``, so
#: ``_build_dataframe``'s row-type guard raises before any DataFrame is
#: built. ``headers`` is a well-formed one-element list precisely so that
#: no EARLIER guard fires: the test must reach the template that
#: interpolates the name.
LOG_INJECTION_PAYLOAD: Dict[str, Any] = {
    "resultSets": [
        {
            "name": LOG_INJECTION_TABLE_NAME,
            "headers": ["A"],
            "rowSet": [{"A": 1}],
        }
    ]
}


def _install_recorders_except(
    monkeypatch: pytest.MonkeyPatch,
    recorder: List[str],
    keep_real: str,
) -> None:
    """Install recording spies on every pipeline EXCEPT ``keep_real``.

    The sibling :func:`_install_recorders` replaces all five, which is right
    for a pure dispatch test. This variant exists for the end-to-end
    characterization below, which needs ONE production pipeline to actually
    execute while still keeping the other four unreachable: a cross-wiring
    mutation then lands on a spy instead of on a pipeline that would try to
    reach the NBA Stats API, so the test stays network-free either way.

    ``keep_real`` is looked up against :data:`PIPELINE_MODULES` so a typo
    cannot silently leave every pipeline spied (which would make the
    end-to-end assertions unreachable) or every pipeline live.
    """
    assert keep_real in PIPELINE_MODULES, (
        f"keep_real must name one of {sorted(PIPELINE_MODULES)}; "
        f"got {keep_real!r}"
    )
    for domain, module in PIPELINE_MODULES.items():
        if domain == keep_real:
            continue
        monkeypatch.setattr(module, "run", _make_recorder(recorder, domain))


def _make_malformed_schedule_fetch(
    payload: Dict[str, Any],
) -> Callable[..., Dict[str, Any]]:
    """Return a stand-in for ``endpoints.schedule.fetch_leaguegamefinder``.

    Patched onto the ``pipelines.ingest_schedule`` module object, which is
    where the production name is bound: that module does ``from
    endpoints.schedule import fetch_leaguegamefinder`` at import time, so
    patching ``endpoints.schedule`` instead would silently no-op.

    Replacing the FETCH rather than the pipeline is what makes this an
    end-to-end characterization: ``ingest_schedule.run``, the real
    ``normalize_result_sets`` validation gate, the real ``ValueError`` it
    raises and the real ``run.py`` failure handler all stay in the loop.
    Only the HTTP round trip is removed, so no test performs network I/O.

    The signature mirrors the real helper's ``(client, season)`` exactly,
    with no catch-all ``**kwargs``, so a change to how the pipeline calls
    it raises ``TypeError`` inside ``run.py``'s ``try`` instead of being
    absorbed silently.
    """

    def _fetch_leaguegamefinder(client: Any, season: str) -> Dict[str, Any]:
        return payload

    return _fetch_leaguegamefinder


def test_malformed_table_name_forges_an_unprefixed_line_in_both_sinks(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """CHARACTERIZATION — an injected newline forges a whole audit line.

    Drives a result-set name containing ``"\\nforged_admin_login_success"``
    through the REAL ``pipelines.ingest_schedule.run``, the REAL
    ``utils.schema_normalizer`` validation gate and the REAL ``run.py``
    ``schedule`` failure handler, then reads the durable log back
    line-by-line. Pins that the payload's post-newline remainder becomes a
    STANDALONE physical line carrying none of ``config.LOG_FORMAT``'s
    fields, in the durable log and mirrored on the console — the composed
    outcome that neither the message-level normalizer characterization nor
    Section F's dual-sink characterization observes on its own.

    Mutations detected:

    * Applying the ``repr``/escaping fix to either ``_build_dataframe``
      template — the message collapses to one line, the forged line
      disappears, and the log's final line becomes a prefixed record. Four
      assertions turn red and route the fixer to this section's banner.
    * Applying the ``run.py`` redaction fix ALONE — the traceback stops
      being published, so the forged line disappears from both sinks while
      the two-line exception message survives. The split assertions stay
      green and the sink assertions turn red, which is exactly the signal
      that the disclosure was closed but the neutralisation gap was not.
    * Making ``_snake_case`` strip or replace control characters — the same
      collapse as the ``repr`` fix, detected the same way.
    * Dropping the result-set name from either template — the forged text
      disappears and the diagnostic that names the offending table is
      lost.
    * Swallowing the exception in ``run.py``'s handler instead of
      re-raising — the exit-code and error-counter assertions turn red.
    """
    # Arrange — census spies on the other four pipelines so a mis-dispatch
    # lands on a spy rather than on a production pipeline that would try
    # to reach the NBA Stats API; the schedule pipeline stays REAL and
    # only its fetch is starved of the network.
    recorder: List[str] = []
    _install_recorders_except(monkeypatch, recorder, INJECTION_SUBCOMMAND)
    monkeypatch.setattr(
        ingest_schedule,
        "fetch_leaguegamefinder",
        _make_malformed_schedule_fetch(LOG_INJECTION_PAYLOAD),
    )

    # Act
    result = cli_runner.invoke(
        cli, [INJECTION_SUBCOMMAND, "--season", config.DEFAULT_SEASON]
    )

    # Assert — the real validation gate raised, and the real handler ran.
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli {INJECTION_SUBCOMMAND}` must exit {EXIT_FAILURE} when the "
        f"normalizer rejects the envelope; got {result.exit_code}. "
        f"stderr={result.stderr!r}"
    )
    assert type(result.exception) is ValueError, (
        f"the propagated exception must be the normalizer's own "
        f"ValueError, so this test characterises the production message "
        f"template rather than an injected stand-in; got "
        f"{type(result.exception).__name__}: {result.exception}"
    )
    assert recorder == [], (
        f"`cli {INJECTION_SUBCOMMAND}` must dispatch to the schedule "
        f"pipeline and to nothing else; the spies on the other four "
        f"recorded {recorder!r}, which means the CLI cross-wired a "
        f"subcommand to the wrong pipeline"
    )

    # Assert — the message is exactly the template's output, and the
    # injected newline really did survive into it.
    assert str(result.exception) == EXPECTED_INJECTED_VALUE_ERROR_MESSAGE, (
        f"the message must match the _build_dataframe row-type template "
        f"with the name substituted verbatim; expected "
        f"{EXPECTED_INJECTED_VALUE_ERROR_MESSAGE!r}, got "
        f"{str(result.exception)!r}"
    )
    message_lines = str(result.exception).splitlines()
    assert len(message_lines) == EXPECTED_INJECTED_MESSAGE_LINE_COUNT, (
        f"CHARACTERIZATION drift: the unneutralised name splits the "
        f"message across exactly {EXPECTED_INJECTED_MESSAGE_LINE_COUNT} "
        f"physical lines; got {len(message_lines)}. If this reads 1, the "
        f"repr()/escaping fix has landed and this section's assertions "
        f"must be replaced with the neutralised counterparts"
    )

    # Assert — the forged line reaches the DURABLE log as a standalone
    # physical line, exactly once.
    log_lines = (tmp_log_dir / "pipeline.log").read_text(
        encoding="utf-8"
    ).splitlines()
    assert log_lines.count(FORGED_LOG_LINE) == EXPECTED_FORGED_LINE_OCCURRENCES, (
        f"CHARACTERIZATION drift: the durable log holds "
        f"{log_lines.count(FORGED_LOG_LINE)} standalone forged line(s); "
        f"this defect currently produces exactly "
        f"{EXPECTED_FORGED_LINE_OCCURRENCES}. Expected line: "
        f"{FORGED_LOG_LINE!r}"
    )

    # Assert — the split point, from both sides: the genuine record is
    # truncated mid-message and the forged remainder is the final line.
    # Asserting the pair is what proves ONE record became TWO lines rather
    # than the forged text merely appearing somewhere.
    assert log_lines[-2:] == [
        EXPECTED_TRUNCATED_GENUINE_LINE,
        FORGED_LOG_LINE,
    ], (
        f"the durable log must end with the truncated genuine line "
        f"followed by the forged one; expected "
        f"{[EXPECTED_TRUNCATED_GENUINE_LINE, FORGED_LOG_LINE]!r}, got "
        f"{log_lines[-2:]!r}"
    )

    # Assert — and the forged line carries NO formatter prefix, which is
    # the whole substance of CWE-117 here: nothing on that line marks it
    # as machine-generated.
    assert LOG_RECORD_PREFIX_MARKER not in log_lines[-1], (
        f"CHARACTERIZATION drift: the forged line now carries the record "
        f"prefix marker {LOG_RECORD_PREFIX_MARKER!r}, so it is no longer "
        f"indistinguishable from attacker-authored content. Line: "
        f"{log_lines[-1]!r}"
    )

    # Assert — and the console sink is forged identically, so redacting
    # only one sink would leave the audit trail compromised.
    stdout_lines = result.stdout.splitlines()
    assert (
        stdout_lines.count(FORGED_LOG_LINE) == EXPECTED_FORGED_LINE_OCCURRENCES
    ), (
        f"CHARACTERIZATION drift: the console holds "
        f"{stdout_lines.count(FORGED_LOG_LINE)} standalone forged "
        f"line(s); one LogRecord is formatted by two handlers at the same "
        f"level, so the console must carry exactly "
        f"{EXPECTED_FORGED_LINE_OCCURRENCES}"
    )

    # Assert — the failure was still counted under the real label set, so
    # the forgery happened on the genuine failure path and not on some
    # short-circuit that never reached the handler.
    assert (
        _runs_counter(INJECTION_PIPELINE_LABEL, OUTCOME_ERROR)
        == EXPECTED_COUNTER_HIT
    ), (
        f"{RUNS_COUNTER}{{pipeline={INJECTION_PIPELINE_LABEL!r}, "
        f"outcome={OUTCOME_ERROR!r}}} must be exactly "
        f"{EXPECTED_COUNTER_HIT}; got "
        f"{_runs_counter(INJECTION_PIPELINE_LABEL, OUTCOME_ERROR)}"
    )
    assert (
        _runs_counter(INJECTION_PIPELINE_LABEL, OUTCOME_SUCCESS)
        == EXPECTED_COUNTER_MISS
    ), (
        f"{RUNS_COUNTER}{{pipeline={INJECTION_PIPELINE_LABEL!r}, "
        f"outcome={OUTCOME_SUCCESS!r}}} must remain "
        f"{EXPECTED_COUNTER_MISS} on the failure path; got "
        f"{_runs_counter(INJECTION_PIPELINE_LABEL, OUTCOME_SUCCESS)}"
    )
