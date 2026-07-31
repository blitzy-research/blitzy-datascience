"""CLI failure-path contracts — the ``except Exception`` boundary of ``run.py``.

What this module pins
---------------------
1. **Non-zero exit on failure.** Each of the five data subcommands
   (``players``, ``teams``, ``games``, ``lineups``, ``schedule``) exits
   ``1`` when its pipeline raises, because every ``except Exception``
   block in ``run.py`` ends with a bare ``raise``.
2. **Exception fidelity.** The *original* exception instance reaches the
   caller un-swallowed and un-rewrapped. This is asserted as OBJECT
   IDENTITY (``result.exception is injected_failure``) rather than as a
   class-and-message match, because ``run.py``'s bare ``raise`` promises
   the very object — traceback, ``__cause__`` and attributes included —
   and a class-and-message match cannot tell that object apart from a
   freshly built look-alike.
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
6. **Failure output is confidential (CWE-209, CWE-532).** Every
   ``except Exception`` block routes through ``run.py``'s
   ``_log_failed_run``, so the two channels
   :func:`utils.logger._configure` attaches — the ``StreamHandler`` bound
   to ``sys.stdout`` and the ``RotatingFileHandler`` writing
   :data:`config.LOG_FILE` — carry only ``run.failed subcommand=<d>
   season=<s> error_type=<ClassName> detail=suppressed``. The exception
   message and its traceback are emitted on a separate ``run.failed
   .detail`` record at DEBUG, which the default
   :data:`config.LOG_LEVEL` of ``"INFO"`` discards, so detail is GATED
   rather than destroyed. Section F pins both halves.
7. **The PROCESS boundary is confidential too.** Redacting the log sinks
   is only half of the disclosure surface: the bare ``raise`` of item 1
   deliberately propagates the exception, so ``python run.py <domain>``
   used to hand it to CPython's top-level handler, which printed the
   message, ``Traceback (most recent call last)`` and one absolute
   ``File "<path>", line <n>`` frame per stack level onto ``stderr``.
   ``run.py``'s ``if __name__ == "__main__"`` block therefore dispatches
   through ``run.py::main``, which converts a propagated failure into a
   silent ``SystemExit(1)`` (``from None``) after a redacted
   ``run.aborted`` record, while leaving every deliberate exit status
   (0 on success, ``ready``'s 1, Click's usage 2) untouched. **Section G
   pins that boundary, and it does so WITHOUT ``CliRunner``** — the
   runner catches the exception itself, which is precisely why Sections
   A-F could all pass while the real process disclosed everything.

What "exception fidelity" claims, and with which assertion
---------------------------------------------------------
Each failure test constructs the exception it injects and then compares
``result.exception`` with that object using ``is``. That single
assertion is the strongest of the three: it rejects swallowing (nothing
arrives), re-wrapping in another class, AND re-raising a same-class
look-alike built from the original's message. The companion
``type(result.exception)`` and ``str(result.exception)`` assertions are
retained as diagnostics, so a failure report names what actually
arrived instead of reporting a bare identity mismatch.

The mutations this module detects
--------------------------------
* Deleting the bare ``raise`` that closes an ``except`` block — the
  callback would return normally, Click would exit ``0``, and a total
  pipeline failure would be reported as a success.
* Labelling a failure ``outcome="success"``, double-incrementing either
  series, or misspelling a ``pipeline`` label.
* Making ``all_cmd`` catch a per-iteration failure and **continue** to
  the next entry instead of aborting — every pipeline would run and the
  command would exit ``0`` despite a failed dependency. Note that merely
  relocating the ``try``/``except`` inside the loop while keeping the
  bare ``raise`` still aborts; it is the catch-and-continue behaviour
  that this module's ordered dispatch census rules out.
* Dropping ``ready``'s ``sys.exit(1)``, or inverting its
  ``!= "ready"`` comparison.
* Reverting any handler to ``log.exception`` — or adding
  ``exc_info=True`` back to the redacted ERROR record, or interpolating
  ``str(exc)`` into its message — which republishes upstream-controlled
  exception text, the traceback and this deployment's absolute source
  paths to the console AND to the durable log.
* Deleting the redacted record instead of redacting it (silent
  suppression), deleting the DEBUG detail record or its ``exc_info``
  (diagnostics destroyed rather than gated), or promoting that detail
  record to INFO or above (which restores the disclosure, because both
  configured handlers render INFO).
* Reverting the ``if __name__ == "__main__"`` block to call ``cli()``
  instead of ``run.py::main`` — the propagated exception would again be
  rendered by CPython with its message, traceback and absolute source
  paths on ``stderr``, on the one path every operator actually uses.
* Dropping ``main``'s ``from None``, which would let the interpreter
  print the original failure as the ``SystemExit``'s ``__context__``;
  re-raising the original exception from ``main`` instead of
  ``SystemExit``; or downgrading the exit status to ``0`` so a failed run
  reports success.
* Deleting ``main``'s last-resort ``run.aborted`` record, which would
  turn a failure raised *before* a subcommand's ``try`` — an
  :exc:`OSError` from ``_build_collaborators``, say — into a completely
  silent non-zero exit with no diagnostic anywhere.
* Deleting that record's ``run.aborted.detail`` companion or its
  ``exc_info``, which would destroy the boundary's traceback instead of
  gating it on DEBUG — and for a pre-``try`` failure would destroy the
  only copy of it — or promoting that companion to INFO or above, which
  would re-create on both sinks exactly the disclosure the silent
  ``SystemExit`` removed from ``stderr``.
* Replacing ``main``'s ``try``/``finally`` with plain sequential
  statements, so a failure *inside* the last-resort logging call escapes
  and is rendered by CPython — with its own filesystem path — instead of
  the process exiting quietly with the intended status.
* Widening ``main``'s ``except`` to :exc:`BaseException`, or catching
  :exc:`SystemExit`, which would swallow ``ready``'s deliberate exit
  status and Click's usage exit ``2``.

``_build_collaborators`` is exercised *implicitly*: it is the first
statement of every data subcommand, so each invocation here constructs
the real ``RateLimiter``/``NBAClient``/``CSVWriter``/``CheckpointManager``
graph against the ``tmp_path``-rooted config. It is never imported,
promoted, or given a test-only hook.

No captured output
------------------
Every expected value below is a **structural constant derived from
``run.py``'s documented contract** — an exit code, an exception class, a
float counter value, a label dict, an ordered list of domain names, or a
field name read off ``_log_failed_run``'s own format string. The
confidentiality assertions in Section F compare against a token this
module itself injects and against CPython's documented traceback layout,
never against recorded output.

The one non-constant expectation, the injected exception instance
asserted by identity, is likewise never captured: each failure test
*constructs* that object during its Arrange step and then requires the
CLI to hand back the same one. Nothing here records the CLI's current
output and asserts equality against it, and no expectation would change
if the production code were rewritten while keeping its contract.

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
``catch_exceptions=False`` would let the injected exception escape
``invoke()`` and abort the test instead of being measured. The
happy-path tests in this module do pass ``catch_exceptions=False``,
precisely because no exception is expected there.

**Section G deliberately uses no ``CliRunner`` at all.** That is the
whole point of it: the runner catches the propagated exception itself, so
it can never observe what CPython's top-level handler would have printed.
Section G therefore calls ``run.py::main`` directly, executes ``run.py``
under ``run_name="__main__"``, and runs one real child process — the
three places the process contract actually lives.

Isolation
---------
Every in-process test consumes ``tmp_output_dir`` and ``tmp_log_dir`` so
no operator directory is touched, and relies on the three autouse
fixtures in ``tests/conftest.py`` that reset the correlation ID, the
metrics registry, and the logger handlers before AND after every test.
That registry reset is what makes the exact ``== 0.0`` assertions
meaningful rather than vacuous: a label set that was never incremented
reads ``0.0`` by the Prometheus convention documented on
``MetricsRegistry.get_counter_value``.

Section G's child-process test is the one exception, and deliberately so:
a subprocess cannot inherit a ``monkeypatch``-ed :mod:`config`, so it is
redirected with the four ``NBA_*`` path environment variables — every one
of them pointing inside ``tmp_path`` — while the parent itself writes no
artifact and emits no record, and therefore needs neither fixture.

Import scope
------------
This module imports only stdlib primitives (``__future__``, ``json``,
``logging`` — for the ``DEBUG`` level constant handed to ``caplog``,
``os``, ``pathlib``, ``runpy`` and ``subprocess`` — used by Section G's
process-boundary tests, ``sys`` for the child interpreter path, and
``typing``), ``pytest``, :mod:`config`, :mod:`run` (the ``cli`` group
plus the module object used as the readiness patch target), the five
:mod:`pipelines.ingest_<domain>` modules that ``run.py`` itself imports,
and :mod:`utils.metrics` for the counter read-back. :mod:`requests` is
never imported (Rule 1) and no third-party test library is introduced.
"""
from __future__ import annotations

import json
import logging
import os
import runpy
import subprocess
import sys
from pathlib import Path
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
# Every value in this block is derived independently from ``run.py``'s
# documented contract rather than captured from a run, which is what
# makes the module snapshot-free.
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

#: The ordered dispatch table of ``run.py``'s ``all_cmd``. The sequence
#: is the binding dependency order (AAP §0.4.5): Schedule runs first
#: because it establishes the season's game set, and Lineups runs last so
#: that a failure in the richer domains cannot mask its outcome. The
#: order is a run-sequencing contract, not a file-coupling one — Games
#: enumerates ``GAME_ID`` values by calling the Schedule endpoint helper
#: directly and never reads ``schedule.csv``.
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
# Failure-output confidentiality constants (Section F).
#
# Every value below is derived STRUCTURALLY — from ``run.py``'s redacted
# format string, from CPython's documented traceback layout, or from a
# token this module itself injects. None was captured from a run (C1).
# ---------------------------------------------------------------------------

#: A single unbroken, highly distinctive token planted inside the injected
#: exception's message. Because the TEST supplies it, finding it anywhere
#: in an operator-visible channel proves that channel echoed
#: attacker-/upstream-controlled exception text verbatim. It is written as
#: one word with no spaces so a substring search cannot be defeated by
#: line wrapping in the log formatter.
DISCLOSURE_SENTINEL: str = "SECRET_MARKER_query_token_A1B2C3"

#: The full message of the injected exception. Its shape mirrors the real
#: threat model rather than being abstract: a transport failure whose text
#: carries an endpoint URL and its query string, which is precisely how a
#: bearer token, a signed URL parameter or a payload value reaches an
#: exception message in this system (``requests`` puts the request URL in
#: :exc:`~requests.HTTPError`; :exc:`OSError` puts the path in
#: ``strerror``/``filename``; a normalizer error puts cell values in its
#: ``ValueError``).
SENSITIVE_FAILURE_MESSAGE: str = (
    "upstream request failed: "
    f"https://stats.nba.com/stats/leaguedashplayerstats?{DISCLOSURE_SENTINEL}"
)

#: First line CPython's :mod:`traceback` machinery writes for a chained or
#: unchained exception, and therefore the first line
#: :meth:`logging.Formatter.formatException` appends when a record carries
#: ``exc_info``. Its presence on a normal channel is the unambiguous
#: signature of an unredacted ``log.exception``.
TRACEBACK_HEADER: str = "Traceback (most recent call last)"

#: Prefix of every stack-frame line in a formatted traceback — the full
#: shape is ``File "<absolute path>", line <n>, in <function>``. Asserting
#: this prefix's absence covers source paths AND line numbers in one
#: check, because a line number never appears without its frame line.
TRACEBACK_FRAME_PREFIX: str = 'File "'

#: Absolute source paths that a formatted traceback of an injected
#: pipeline failure is guaranteed to print: the CLI frame that called the
#: pipeline, and this module's frame that raised. ``__file__`` is absolute
#: on CPython 3.12 for both an imported module and a collected test
#: module, which is exactly why a leaked traceback publishes the
#: deployment's directory layout.
CLI_SOURCE_PATH: str = run_module.__file__
TEST_SOURCE_PATH: str = __file__

#: What must NEVER reach a normal operator channel after a failed run,
#: paired with a human-readable description used in the failure message.
#: Asserted as a complete tuple against all three channels rather than
#: spot-checked, so a partial redaction cannot pass.
FORBIDDEN_DISCLOSURES: tuple = (
    ("exception message text", DISCLOSURE_SENTINEL),
    ("traceback header", TRACEBACK_HEADER),
    ("traceback stack frame (source path and line number)", TRACEBACK_FRAME_PREFIX),
    ("absolute path of the CLI module", CLI_SOURCE_PATH),
    ("absolute path of the raising module", TEST_SOURCE_PATH),
)

#: Event name of the redacted ERROR record, read from ``run.py``'s
#: ``_log_failed_run`` format string. Asserting it PRESENT is what stops
#: the fix degenerating into silent suppression: an operator must still
#: learn that this run failed, and under which correlation ID.
FAILURE_EVENT: str = "run.failed"

#: Event name of the DEBUG-gated detail record, from the same helper.
FAILURE_DETAIL_EVENT: str = "run.failed.detail"

#: Field prefix carrying the exception's CLASS NAME on the redacted
#: record. The class name is a static identifier from project source —
#: never upstream- or attacker-controlled — and is what keeps the record
#: triage-able after the message is withheld.
ERROR_TYPE_FIELD_PREFIX: str = "error_type="

#: Literal token on the redacted record stating that detail exists but is
#: withheld, so the ERROR line cannot be mistaken for the whole story.
REDACTION_MARKER: str = "detail=suppressed"

#: Levels the two records must carry. ``_log_failed_run`` calls
#: ``log.error`` then ``log.debug``, and the split is the whole point of
#: the boundary: ERROR is always rendered, DEBUG is discarded by the
#: default :data:`config.LOG_LEVEL` of ``"INFO"`` (``config.py`` L252),
#: which ``utils/logger._configure`` applies to the root logger AND to
#: both handlers.
REDACTED_RECORD_LEVEL: str = "ERROR"
DETAIL_RECORD_LEVEL: str = "DEBUG"

#: Logger name ``run.py`` passes to ``_build_collaborators`` for one
#: subcommand, spelled as a template. Raising THIS logger's level to
#: DEBUG is how the detail record is made to exist without touching
#: :data:`config.LOG_LEVEL` — the handlers stay level-filtered at INFO,
#: so the record is emitted and capturable while never being rendered
#: into ``result.stdout`` or the durable :data:`config.LOG_FILE`.
CLI_LOGGER_NAME_TEMPLATE: str = "cli.{subcommand}"

#: Subcommand used by the DEBUG-gating test. Any one of the five would
#: do — they share a single helper — so one is chosen and named rather
#: than parametrizing a contract that has no per-domain variation.
DEBUG_GATE_SUBCOMMAND: str = "teams"


# ---------------------------------------------------------------------------
# Process-boundary constants (Section G).
#
# Same derivation discipline as Section F: every value is read off
# ``run.py``'s own contract, off CPython's documented behaviour for
# ``SystemExit``, or off a token this module injects. None is captured
# from a run (C1).
# ---------------------------------------------------------------------------

#: Exit status Click reports for a usage error such as an unknown
#: subcommand. Click raises ``UsageError``, whose ``exit_code`` is ``2``,
#: and handles it INSIDE ``Group.main`` while still in standalone mode —
#: so it becomes a ``SystemExit(2)`` that ``run.py::main``'s
#: ``except Exception`` must not intercept.
EXIT_USAGE_ERROR: int = 2

#: A subcommand name ``run.py`` does not register. Spelled to be obviously
#: absent rather than plausibly future-registered, so this test cannot
#: start passing vacuously if a new subcommand is added.
UNKNOWN_SUBCOMMAND: str = "definitely-not-a-registered-subcommand"

#: The operator-facing sentence Click's ``UsageError`` prints for an
#: unregistered subcommand (``click.Group.resolve_command``). It is
#: generated from this project's own command registry — never from
#: payload or upstream data — so it must SURVIVE the process boundary;
#: suppressing it would cost usability and buy no confidentiality.
CLICK_UNKNOWN_COMMAND_MESSAGE: str = f"No such command '{UNKNOWN_SUBCOMMAND}'."

#: Sentinel planted in the message of a deliberately failing last-resort
#: logging call. Distinct from :data:`DISCLOSURE_SENTINEL` so a leak can
#: be attributed to the right exception, and shaped like a private
#: filesystem path because that is what a real log-sink failure carries
#: (:exc:`OSError` puts the path in ``strerror``/``filename``).
LAST_RESORT_SENTINEL: str = "SECRET_MARKER_log_sink_D4E5F6"

LAST_RESORT_FAILURE_MESSAGE: str = (
    f"log sink unavailable: /srv/app/private/{LAST_RESORT_SENTINEL}"
)

#: Event name of the last-resort record ``run.py::_log_aborted_process``
#: writes at ERROR, read off that helper's format string. Asserting it
#: PRESENT is what stops the silent ``SystemExit`` degenerating into a
#: silent failure; asserting it ABSENT on the success, ``ready`` and
#: usage-error paths is what proves only genuine aborts are converted.
PROCESS_ABORT_EVENT: str = "run.aborted"

#: Event name of that helper's DEBUG-gated companion record. It shares the
#: ERROR record's prefix, so every ``PROCESS_ABORT_EVENT`` presence check
#: is written against the fuller ``run.aborted exit_status=`` form to keep
#: the two records distinguishable by a text filter.
PROCESS_ABORT_DETAIL_EVENT: str = "run.aborted.detail"

#: Logger name the process boundary writes under — the PARENT of the
#: ``"cli.<subcommand>"`` names, so raising it to DEBUG also raises the
#: subcommand loggers that inherit from it. Spelled literally for the same
#: reason :data:`CLI_LOGGER_NAME_TEMPLATE` is: a rename in ``run.py`` must
#: fail here rather than be followed silently.
PROCESS_LOGGER_NAME: str = "cli"

#: Field prefix carrying the translated exit status on the last-resort
#: record. Combined with :data:`EXIT_FAILURE` it yields the exact token
#: ``exit_status=1`` that the record must contain.
EXIT_STATUS_FIELD_PREFIX: str = "exit_status="

#: Repository root, derived from the CLI module's own ``__file__`` rather
#: than from a relative literal, so the child-process probe locates the
#: package under test no matter what directory pytest was started from.
REPO_ROOT: Path = Path(CLI_SOURCE_PATH).resolve().parent

#: Filename of the driver script the child-process probe writes into
#: ``tmp_path``. It lives outside ``tests/`` on purpose — pytest's
#: ``testpaths = tests`` means it is never collected — and it is deleted
#: with the temporary directory.
SUBPROCESS_DRIVER_NAME: str = "standalone_cli_probe.py"

#: Hard ceiling for the child process. The child performs no network I/O
#: (its pipeline is replaced before dispatch) and no sleeps, so a run that
#: takes longer than this is hung, not slow, and must fail rather than
#: stall the suite.
SUBPROCESS_TIMEOUT_SECONDS: float = 120.0

#: ``config.py`` L238-241 reads these four ``NBA_*`` overrides ONCE at
#: import time (``config._env_path``), which is exactly why the child is
#: redirected through the environment rather than through
#: ``monkeypatch.setattr`` — a child process cannot inherit the parent's
#: monkeypatched module attributes.
CHILD_OUTPUT_DIR_ENV: str = "NBA_OUTPUT_DIR"
CHILD_CHECKPOINT_PATH_ENV: str = "NBA_CHECKPOINT_PATH"
CHILD_LOG_DIR_ENV: str = "NBA_LOG_DIR"
CHILD_LOG_FILE_ENV: str = "NBA_LOG_FILE"

#: ``config.py`` L252 reads this override for :data:`config.LOG_LEVEL`.
#: The probe REMOVES it from the child's environment so the child runs at
#: the default ``"INFO"`` and the DEBUG detail record is level-filtered —
#: the state an operator gets unless they deliberately opt in.
CHILD_LOG_LEVEL_ENV: str = "NBA_LOG_LEVEL"

#: Variables the driver script itself reads. Passing the repository root,
#: the failure message and the pipeline attribute through the environment
#: keeps :data:`SUBPROCESS_DRIVER_SOURCE` a fixed constant with no string
#: interpolation, so what the child executes is auditable by reading it.
CHILD_REPO_ROOT_ENV: str = "PROBE_REPO_ROOT"
CHILD_FAILURE_MESSAGE_ENV: str = "PROBE_FAILURE_MESSAGE"
CHILD_PIPELINE_ATTR_ENV: str = "PROBE_PIPELINE_ATTR"

#: ``pipeline`` label of :data:`DEBUG_GATE_SUBCOMMAND`, spelled literally
#: for the same reason :data:`DATA_SUBCOMMAND_LABELS` is: a label rename
#: in ``run.py`` must fail here rather than be followed silently by a
#: derived ``f"ingest_{name}"`` expression.
DEBUG_GATE_PIPELINE_LABEL: str = "ingest_teams"

#: Subcommand the child probe drives, and the attribute of :mod:`run`
#: holding the pipeline module it dispatches to. The two differ, exactly
#: as in :data:`DATA_SUBCOMMAND_LABELS`, so both are spelled literally.
CHILD_PROBE_SUBCOMMAND: str = "teams"
CHILD_PROBE_PIPELINE_ATTR: str = "ingest_teams"

#: Class name the driver gives its injected exception. The child's
#: redacted records must name it in ``error_type=``, which proves the
#: child really failed the way the probe intended instead of dying of
#: something incidental such as an import error.
CHILD_FAILURE_CLASS_NAME: str = "_ChildPipelineFailure"

#: The driver script. It is a FIXED string — no ``format``, no f-string —
#: so a reader can see exactly what the child runs. It reproduces
#: ``python run.py <subcommand>`` faithfully in the one respect that
#: matters here: control reaches ``run.py``'s real process entry point,
#: ``run.main``, with a pipeline that raises. Injecting the failure is
#: unavoidable — every genuine pipeline failure needs the network, and
#: this tier is offline — so the driver replaces exactly one attribute and
#: changes nothing else.
SUBPROCESS_DRIVER_SOURCE: str = '''"""Drive run.py's process entry point with one failing pipeline.

Written to a temporary directory by the child-process probe in
tests/unit/test_cli_failure_paths.py and executed as a real child
process, so the interpreter's top-level exception handling is the
genuine article rather than a harness's imitation.
"""
import os
import sys

sys.path.insert(0, os.environ["PROBE_REPO_ROOT"])

import run

FAILURE_MESSAGE = os.environ["PROBE_FAILURE_MESSAGE"]


class _ChildPipelineFailure(RuntimeError):
    """Distinct class so the child's redacted record names it exactly."""


def _raise_failure(client, writer, checkpoint, season, logger=None, metrics=None):
    raise _ChildPipelineFailure(FAILURE_MESSAGE)


setattr(getattr(run, os.environ["PROBE_PIPELINE_ATTR"]), "run", _raise_failure)
run.main(sys.argv[1:])
'''


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

    Each failure test constructs ONE instance of this class, hands it to
    :func:`_make_failing_recorder`, and afterwards asserts
    ``result.exception is <that instance>``. Object identity is the real
    contract — ``run.py`` closes every ``except Exception`` block with a
    bare ``raise``, so the very object the pipeline raised must arrive at
    the caller with its ``__traceback__``, ``__cause__`` and any custom
    attributes intact.

    The bespoke class is retained *alongside* that identity check as a
    diagnostic: ``type(result.exception) is _InjectedPipelineFailure``
    reads clearly in a failure report and immediately distinguishes
    "wrapped in something else" (for example a
    :class:`click.ClickException` or a generic
    ``RuntimeError("pipeline failed")``) from "a look-alike of the right
    class". Only the ``is`` assertion can tell a look-alike apart from
    the original, which is why it is the primary one.
    """


class _SensitivePipelineFailure(RuntimeError):
    """Exception whose message deliberately carries a confidential token.

    Distinct from :class:`_InjectedPipelineFailure` on purpose. The two
    families answer different questions and must not be merged:

    * :class:`_InjectedPipelineFailure` proves the exception OBJECT
      survives the CLI boundary intact (identity, exit code, metrics).
    * this class proves the exception's TEXT does **not** survive into
      any operator-visible channel.

    Its class name is also load-bearing: the redacted ERROR record must
    carry ``error_type=_SensitivePipelineFailure``, so a bespoke name
    makes that assertion specific rather than satisfiable by any generic
    ``RuntimeError`` a mutation might substitute.
    """


def _read_operator_log() -> str:
    """Return the full text of the durable operator log.

    Reads :data:`config.LOG_FILE` symbolically rather than rebuilding the
    path from a literal filename, so the ``tmp_log_dir`` redirection is
    honoured automatically and a future rename of the artifact cannot make
    this helper silently inspect the wrong file.

    The file is guaranteed to exist and to be complete by the time a test
    calls this: ``run.py`` emits ``run.start`` at INFO before invoking any
    pipeline, and :meth:`logging.StreamHandler.emit` — the base of
    :class:`~logging.handlers.RotatingFileHandler` — flushes after every
    record, so no explicit handler close or flush is required.

    This is the second of the two sinks :func:`utils.logger._configure`
    attaches. Inspecting it separately from ``result.stdout`` matters
    because the two have different lifetimes: console output is
    ephemeral, whereas this file is the durable forensic artifact an
    operator archives and ships to a log aggregator, which is exactly the
    CWE-532 surface.
    """
    return config.LOG_FILE.read_text(encoding="utf-8")


def _assert_no_sensitive_disclosure(channel: str, text: str, subcommand: str) -> None:
    """Assert the COMPLETE forbidden-disclosure tuple is absent from ``text``.

    Iterating :data:`FORBIDDEN_DISCLOSURES` rather than spot-checking one
    marker is what makes a partial redaction fail: suppressing the
    exception message while still attaching ``exc_info`` would leave the
    traceback header and the frame lines behind, and each is asserted
    independently with a message naming which disclosure leaked and on
    which channel.
    """
    for description, forbidden in FORBIDDEN_DISCLOSURES:
        assert forbidden not in text, (
            f"`cli {subcommand}` disclosed the {description} on {channel}: "
            f"{forbidden!r} must never appear there. Exception text and "
            f"tracebacks are arbitrary, frequently upstream-controlled data "
            f"(CWE-209, CWE-532) and belong only on the DEBUG-gated "
            f"diagnostic channel. {channel}={text!r}"
        )


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

    That exactness is why a handwritten spy is preferred to a
    ``MagicMock`` here: interface drift must surface as a loud
    ``TypeError`` rather than being absorbed silently. A catch-all would
    accept a misspelled dispatch keyword (``checkpont=checkpoint``), a
    dropped required keyword, or an argument the real pipelines do not
    accept — leaving all three invisible. With this signature each of
    them raises ``TypeError`` inside ``run.py``'s ``try`` block instead,
    which breaks the exception-class assertions on the failure paths and
    both the exit-code and success-counter assertions on the happy paths.
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
    all". Without the pre-raise append, an ``all`` fail-fast assertion
    could not tell a correct short-circuit apart from a broken dispatch
    that never invoked anything.

    The signature is the real ``pipelines.ingest_<domain>.run`` interface
    with no catch-all ``**kwargs``, for the interface-drift reason spelled
    out on :func:`_make_recorder`. Keeping both doubles
    signature-identical matters specifically here: under a
    dispatch-keyword mutation this spy raises ``TypeError`` *instead of*
    the injected failure, so the exception assertions fail loudly rather
    than passing on a coincidentally non-zero exit code. It also keeps
    ``recorder`` honest — a drifted call never reaches the ``append``, so
    the ``recorder == [...]`` fail-fast assertions break too.

    ``failure`` is the exception **instance** the caller wants raised,
    and it is a REQUIRED parameter with no default on purpose. If this
    factory constructed the exception itself, the calling test would hold
    no reference to the object that crossed the CLI boundary and could
    only compare its class and message. A mutation that caught the
    pipeline error and re-raised a fresh look-alike —
    ``except Exception as exc: raise type(exc)(str(exc))`` in place of
    ``run.py``'s bare ``raise`` — would then satisfy every assertion
    while destroying the ``__cause__`` chain, the ``__traceback__`` and
    any attribute an operator's error handler reads off the original
    object. Injecting the instance is what lets each caller assert
    ``result.exception is <that instance>`` and make that mutation fail.
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
# Each data subcommand's ``except Exception`` block logs the failure,
# records the error outcome, and ends with a bare ``raise``. The contract
# is therefore threefold: the very exception object the pipeline raised
# reaches the caller, the process exits non-zero, and no sibling pipeline
# is dispatched.
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
      block. Without it the callback would return normally, Click would
      exit ``0``, and ``result.exception`` would be ``None`` — so a total
      pipeline failure would be reported to the operator as a success.
    * Wrapping the failure in a different class (for example ``raise
      RuntimeError("pipeline failed") from exc``), caught by the
      object-identity assertion and by its class/message companions.
    * **Re-raising a same-class look-alike** — ``except Exception as
      exc: raise type(exc)(str(exc))``. This mutation preserves the
      class AND the message, so it is invisible to a class-plus-message
      check; only the ``is`` assertion below rejects it. It matters
      because the rebuilt object silently drops ``__cause__``, the
      original ``__traceback__`` and every custom attribute an
      operator's handler may read.
    * Labelling the failure path ``outcome="success"``, incrementing both
      series, incrementing the error series twice, or misspelling the
      ``pipeline`` label. Because :meth:`get_counter_value` returns a
      well-defined ``0.0`` for a never-incremented label set, the
      :data:`EXPECTED_COUNTER_MISS` assertion is exact rather than
      vacuous — and a typo'd label would leave the asserted series at
      ``0.0`` and fail the error-counter assertion rather than passing
      silently.
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
# ``error`` series at zero. Pinning the success series exactly is what
# calibrates the failure-side assertions above: both directions of the
# same counter are then specified, so neither can drift alone.
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
# ``all_cmd`` wraps the ENTIRE dispatch loop in one ``try`` whose handler
# ends in a bare ``raise``, so the first failure unwinds the loop.
#
# The mutation these tests rule out is CATCH-AND-CONTINUE: an ``except``
# placed per-iteration that records the error and moves on to the next
# entry (or one that simply drops the ``raise``). That version would run
# all five pipelines and exit ``0`` despite a failed dependency. Merely
# relocating the ``try``/``except`` inside the loop while KEEPING the bare
# ``raise`` still aborts, so it is the swallowing — not the placement —
# that the ordered dispatch census below detects.
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
      ``raise`` from the handler. That version would record the schedule
      failure, proceed through games, teams, players and lineups, and
      exit ``0``, so the operator would be told a run succeeded when its
      first pipeline never completed. The whole-list assertion below is
      what makes that impossible: it proves not merely that ``schedule``
      ran, but that the four later pipelines did NOT.
    * Re-wrapping the propagated failure in ``all_cmd``'s ``except``
      block — including the same-class form ``raise type(exc)(str(exc))``
      that survives a class-and-message check. The aggregate command must
      hand back the very object the failing pipeline raised, so the
      identity assertion below is asserted here exactly as it is for the
      individual subcommands.
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
# asserted alongside it because the counter is only interpretable next to
# the census of what actually ran: ``1.0`` proves "one increment per
# invocation" only once the ordered list confirms five pipelines executed
# rather than one.
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
# ``ready_cmd`` has exactly one branch: it echoes the probe body, then
# exits ``1`` when ``status != "ready"``. Substituting a deterministic
# probe removes the environmental variability that would otherwise decide
# the exit code, so BOTH directions of that single branch can be pinned
# exactly — a not-ready verdict must exit ``1`` and a ready verdict must
# exit ``0``, with the JSON body emitted on stdout either way.
# ---------------------------------------------------------------------------


def test_ready_subcommand_exits_one_and_still_echoes_the_body_when_not_ready(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """``cli ready`` echoes the probe body and THEN exits 1 when not ready.

    Mutations detected: dropping ``sys.exit(1)`` or inverting the
    ``!= "ready"`` comparison — either would exit ``0`` on a failed probe
    and tell systemd, Docker healthchecks and shell pipelines that a
    broken process is serviceable. Also detected: moving ``sys.exit``
    ABOVE ``click.echo``, which would suppress the diagnostic body
    operators pipe into ``jq``; and switching the echo to ``err=True``,
    which would move it off stdout.
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
    would exit ``1`` on a healthy process and make every orchestrator
    treat the service as permanently unready. Paired with the not-ready
    test above, this pins BOTH directions of the branch, so neither
    verdict can be reported with the other's exit code.
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
# Section F — failure-output CONFIDENTIALITY (CWE-209, CWE-532).
#
# Sections A-E certify what the failure boundary must DO: exit non-zero,
# propagate the original object, meter the outcome, fail fast, translate a
# readiness verdict. This section certifies what it must not SAY.
#
# CD-3 — THE DEFECT THESE TESTS MOTIVATED, with its failing case:
#
# * Behaviour before the fix: every ``except Exception`` block called
#   ``log.exception(...)``, which is ``log.error(..., exc_info=True)``.
#   ``utils/logger._configure`` attaches a ``StreamHandler`` bound to
#   ``sys.stdout`` AND a ``RotatingFileHandler`` writing
#   ``config.LOG_FILE``, both at ``config.LOG_LEVEL``, so an ERROR record
#   carrying ``exc_info`` was rendered into two NORMAL operator channels
#   at once. Injecting a pipeline failure whose message is
#   ``"upstream request failed: https://stats.nba.com/stats/
#   leaguedashplayerstats?<token>"`` proved it: the token, ``Traceback
#   (most recent call last)``, the absolute path of ``run.py`` and its
#   exact line numbers all appeared in ``result.stdout`` AND in
#   ``logs/pipeline.log``. Any upstream, filesystem or payload-derived
#   exception message — a signed URL, a query token, a private path, a
#   data value — was therefore disclosed to the console, to CI output and
#   to every downstream log consumer.
# * Behaviour after the fix: those channels carry only
#   ``run.failed subcommand=<d> season=<s> error_type=<ClassName>
#   detail=suppressed``. The message and traceback move to a DEBUG record
#   that the default ``config.LOG_LEVEL="INFO"`` discards outright.
# * The change: one new private helper, ``run.py::_log_failed_run``, and
#   one changed statement per handler. Every ``metrics.registry.inc`` and
#   every bare ``raise`` is byte-identical, so Sections A-E keep passing
#   unmodified — which is the point, and which the assertions below
#   re-prove in the same breath as the confidentiality property.
# * Deliberately NOT changed: the bare ``raise``. AAP §0.5.2.1 mandates
#   log-and-re-raise, so the original exception must still reach the
#   caller; redaction governs what this process writes to its OWN log
#   sinks, not what it propagates — which is why every assertion below
#   re-proves propagation in the same breath as confidentiality.
# * The ``if __name__ == "__main__"`` block, by contrast, IS changed, and
#   Section G owns that half. Leaving it on ``cli()`` meant the exception
#   this section keeps out of the log sinks was still rendered in full by
#   CPython onto ``stderr`` — the same disclosure on a channel this
#   section cannot observe, because ``CliRunner`` catches the exception
#   before the interpreter ever sees it. See Section G (defect CD-4).
# * Scope: exercised under Constraint C2, which permits a non-test source
#   change "to fix a genuine bug found" provided it is minimal and called
#   out explicitly with the failing case that motivated it (AAP §0.1.4).
#   The failing case is the injected sensitive message below.
#
# How the DEBUG half is verified WITHOUT persisting the detail: the third
# test raises the level of the SUBCOMMAND's logger only, so the record is
# created and captured in-process while both configured handlers remain
# level-filtered at INFO and render nothing. Nothing in this section
# requires a token or a traceback to be written into a durable log file.
#
# Every expected value here is structural (C1): a token this module
# injects, ``run.py``'s own redacted format string, or CPython's
# documented traceback layout. Nothing is captured from a run.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("subcommand,pipeline_label", DATA_SUBCOMMAND_LABELS)
def test_data_subcommand_failure_discloses_no_exception_text_traceback_or_source_path(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    subcommand: str,
    pipeline_label: str,
) -> None:
    """A failed subcommand redacts exception text, tracebacks and paths everywhere.

    All three channels ``run.py`` can write are held to the identical
    standard: ``result.stdout`` (the ``StreamHandler`` bound to
    ``sys.stdout``), ``result.stderr``, and the durable
    ``config.LOG_FILE`` written by the ``RotatingFileHandler``. The
    complete :data:`FORBIDDEN_DISCLOSURES` tuple is checked against each,
    so a partial redaction — sanitising the console while still writing
    the traceback to the archived log, or vice versa — fails here.

    The record must still be USEFUL, so the same test asserts the
    redacted line is present with the event name, the subcommand, the
    exception's class name and the ``detail=suppressed`` marker. That is
    what separates redaction from silent suppression: deleting the
    logging call altogether would satisfy every absence assertion and is
    caught by the presence assertions.

    Finally it re-proves the contracts redaction must not have cost —
    exit code, original-exception identity, and the exact error counter —
    because a "fix" that swallowed the exception, or converted it to a
    :class:`click.ClickException`, would also stop the traceback being
    printed while silently destroying failure propagation.

    Mutation detected: restoring ``log.exception`` (or adding
    ``exc_info=True`` to the ERROR record), interpolating ``str(exc)`` or
    ``repr(exc)`` into the message, promoting the DEBUG detail record to
    INFO/WARNING/ERROR, or deleting the redacted record entirely.
    """
    # Arrange — a failure whose MESSAGE is the thing under test.
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[subcommand],
        "run",
        _make_failing_recorder(recorder, subcommand, sensitive_failure),
    )

    # Act — default ``catch_exceptions=True`` so the propagated exception
    # is recorded rather than aborting the test, and so Click itself never
    # prints a traceback that would confound the channel assertions.
    result = cli_runner.invoke(cli, [subcommand, "--season", config.DEFAULT_SEASON])
    operator_log = _read_operator_log()

    # Assert — nothing confidential on any of the three normal channels.
    _assert_no_sensitive_disclosure("result.stdout", result.stdout, subcommand)
    _assert_no_sensitive_disclosure("result.stderr", result.stderr, subcommand)
    _assert_no_sensitive_disclosure("the operator log file", operator_log, subcommand)

    # Assert — the redacted record IS emitted, and carries enough to
    # triage: event, subcommand, exception class, suppression marker.
    expected_error_type = (
        f"{ERROR_TYPE_FIELD_PREFIX}{_SensitivePipelineFailure.__name__}"
    )
    for channel, text in (
        ("result.stdout", result.stdout),
        ("the operator log file", operator_log),
    ):
        assert f"{FAILURE_EVENT} subcommand={subcommand}" in text, (
            f"`cli {subcommand}` must still record the failure event "
            f"{FAILURE_EVENT!r} for this subcommand on {channel}; redaction "
            f"must not become silent suppression. {channel}={text!r}"
        )
        assert expected_error_type in text, (
            f"`cli {subcommand}` must record the exception CLASS name on "
            f"{channel} — {expected_error_type!r} — because the class name is "
            f"a static project identifier that keeps the redacted record "
            f"triage-able. {channel}={text!r}"
        )
        assert REDACTION_MARKER in text, (
            f"`cli {subcommand}` must mark the record as redacted with "
            f"{REDACTION_MARKER!r} on {channel} so an operator knows detail "
            f"exists and is withheld rather than absent. {channel}={text!r}"
        )

    # Assert — confidentiality did not cost any Section A-C contract.
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli {subcommand}` must still exit {EXIT_FAILURE} after redacting "
        f"its failure output; got {result.exit_code}. A sanitised boundary "
        f"that also swallowed the failure would be a worse defect than the "
        f"disclosure it fixed"
    )
    assert result.exception is sensitive_failure, (
        f"`cli {subcommand}` must still propagate the ORIGINAL exception "
        f"object; got id={id(result.exception)} "
        f"({type(result.exception).__name__}), expected "
        f"id={id(sensitive_failure)}. Redaction governs what run.py WRITES, "
        f"never what it RAISES"
    )
    observed_error = _runs_counter(pipeline_label, OUTCOME_ERROR)
    assert observed_error == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{pipeline_label}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_HIT} — the redacted boundary must keep emitting "
        f"the exact same metric it did before"
    )


def test_all_subcommand_failure_discloses_no_exception_text_traceback_or_source_path(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
) -> None:
    """A failed ``cli all`` redacts its failure output on every normal channel.

    ``all_cmd``'s handler is a separate ``except`` block from the five
    data subcommands, so it needs its own coverage: a fix applied to the
    five and missed on the aggregate would leave the most commonly
    scheduled command — the one an operator wires into cron or CI, where
    output is captured and retained — still disclosing.

    Mutation detected: leaving ``all_cmd`` on ``log.exception`` while the
    five data subcommands are redacted.
    """
    # Arrange — fail the FIRST pipeline of the documented order.
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[FIRST_ALL_PIPELINE],
        "run",
        _make_failing_recorder(recorder, FIRST_ALL_PIPELINE, sensitive_failure),
    )

    # Act
    result = cli_runner.invoke(cli, ["all", "--season", config.DEFAULT_SEASON])
    operator_log = _read_operator_log()

    # Assert — nothing confidential on any of the three normal channels.
    _assert_no_sensitive_disclosure("result.stdout", result.stdout, "all")
    _assert_no_sensitive_disclosure("result.stderr", result.stderr, "all")
    _assert_no_sensitive_disclosure("the operator log file", operator_log, "all")

    # Assert — the aggregate's own redacted record is present and useful.
    expected_error_type = (
        f"{ERROR_TYPE_FIELD_PREFIX}{_SensitivePipelineFailure.__name__}"
    )
    assert f"{FAILURE_EVENT} subcommand=all" in operator_log, (
        f"`cli all` must still record {FAILURE_EVENT!r} for the aggregate "
        f"subcommand; operator log={operator_log!r}"
    )
    assert expected_error_type in operator_log, (
        f"`cli all` must record the exception class name "
        f"{expected_error_type!r}; operator log={operator_log!r}"
    )
    assert REDACTION_MARKER in operator_log, (
        f"`cli all` must mark the record as redacted with "
        f"{REDACTION_MARKER!r}; operator log={operator_log!r}"
    )

    # Assert — fail-fast, propagation and metrics all survive redaction.
    assert recorder == EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE, (
        f"`cli all` must still stop after the first failing pipeline; "
        f"dispatch census={recorder!r} expected "
        f"{EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE!r}"
    )
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli all` must still exit {EXIT_FAILURE} after redacting its "
        f"failure output; got {result.exit_code}"
    )
    assert result.exception is sensitive_failure, (
        f"`cli all` must still propagate the ORIGINAL exception object; got "
        f"id={id(result.exception)} ({type(result.exception).__name__}), "
        f"expected id={id(sensitive_failure)}"
    )
    observed_error = _runs_counter(ALL_PIPELINE_LABEL, OUTCOME_ERROR)
    assert observed_error == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{ALL_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_HIT}"
    )


def test_failure_detail_is_emitted_only_on_the_debug_gated_channel(
    cli_runner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    caplog,
) -> None:
    """The withheld traceback is GATED on DEBUG, not destroyed — and stays unrendered.

    This is the other half of the confidentiality contract, and without it
    the fix would be indistinguishable from throwing diagnostics away.
    ``run.py::_log_failed_run`` emits two records per failure: the
    redacted ERROR record and a ``run.failed.detail`` DEBUG record
    carrying the full ``exc_info``.

    The two levels are asserted separately because they have opposite
    obligations. The ERROR record must carry NO ``exc_info`` — that is
    precisely what ``log.exception`` did wrong. The DEBUG record must
    carry the ORIGINAL exception object, so an operator who opts in
    recovers the true causality rather than a reconstruction.

    How the DEBUG record is made to exist without persisting it: only the
    subcommand's own logger (``cli.teams``) is raised to DEBUG, via
    ``caplog.at_level(..., logger=...)``. ``utils/logger._configure``
    applies ``config.LOG_LEVEL`` — ``"INFO"`` by default — to the root
    logger AND to both handlers, and a handler drops any record below its
    own level, so the DEBUG record is emitted and captured in-process
    while ``result.stdout`` and the durable ``config.LOG_FILE`` render
    nothing of it. That is asserted here too: raising a logger's level
    must not turn the durable sink into a disclosure channel, and this
    test never requires the sentinel or a traceback to be written to a
    file.

    Mutation detected: deleting the DEBUG detail record (diagnostics lost
    with no way to recover them); dropping its ``exc_info=True`` (the
    record survives but carries no causality); promoting it to INFO or
    above (which would restore the very disclosure the redaction removed,
    because both handlers render INFO); and adding ``exc_info`` back to
    the ERROR record.
    """
    # Arrange — raise ONLY the subcommand logger's level, then fail.
    logger_name = CLI_LOGGER_NAME_TEMPLATE.format(subcommand=DEBUG_GATE_SUBCOMMAND)
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[DEBUG_GATE_SUBCOMMAND],
        "run",
        _make_failing_recorder(recorder, DEBUG_GATE_SUBCOMMAND, sensitive_failure),
    )

    # Act
    with caplog.at_level(logging.DEBUG, logger=logger_name):
        result = cli_runner.invoke(
            cli, [DEBUG_GATE_SUBCOMMAND, "--season", config.DEFAULT_SEASON]
        )
    operator_log = _read_operator_log()

    # Assert — exactly one detail record, at DEBUG, carrying the ORIGINAL
    # exception object rather than a copy of its text.
    detail_records = [
        record
        for record in caplog.records
        if record.getMessage().startswith(FAILURE_DETAIL_EVENT)
    ]
    assert len(detail_records) == 1, (
        f"the boundary must emit exactly one {FAILURE_DETAIL_EVENT!r} record "
        f"per failure so causality is recoverable; captured "
        f"{[record.getMessage() for record in caplog.records]!r}"
    )
    detail_record = detail_records[0]
    assert detail_record.levelname == DETAIL_RECORD_LEVEL, (
        f"the {FAILURE_DETAIL_EVENT!r} record must be emitted at "
        f"{DETAIL_RECORD_LEVEL} so the default INFO handlers discard it; got "
        f"{detail_record.levelname}"
    )
    # ``or (None, None, None)`` keeps the comparison below well defined
    # under the mutation it exists to catch: when ``exc_info=True`` is
    # dropped, ``record.exc_info`` is ``None``, the pair becomes
    # ``(None, None)``, and the assertion reports a readable diagnosis
    # instead of raising ``TypeError`` on a subscript. Comparing the
    # ``(class, value)`` pair — rather than asking merely whether
    # ``exc_info`` exists — additionally pins the exception CLASS the
    # record must carry, which a bare existence check cannot do.
    detail_exc_info = detail_record.exc_info or (None, None, None)
    assert detail_exc_info[:2] == (_SensitivePipelineFailure, sensitive_failure), (
        f"the {FAILURE_DETAIL_EVENT!r} record must carry exc_info whose class "
        f"is {_SensitivePipelineFailure.__name__} and whose value is the "
        f"raised object, or the withheld traceback is destroyed rather than "
        f"gated; got {detail_exc_info[:2]!r}, expected "
        f"{(_SensitivePipelineFailure, sensitive_failure)!r}"
    )
    assert detail_record.exc_info[1] is sensitive_failure, (
        f"the {FAILURE_DETAIL_EVENT!r} record must carry the ORIGINAL "
        f"exception object; got id={id(detail_record.exc_info[1])} "
        f"({type(detail_record.exc_info[1]).__name__}), expected "
        f"id={id(sensitive_failure)}"
    )

    # Assert — the redacted ERROR record is emitted alongside it and
    # carries NO exc_info, which is the difference from log.exception.
    redacted_records = [
        record
        for record in caplog.records
        if record.getMessage().startswith(
            f"{FAILURE_EVENT} subcommand={DEBUG_GATE_SUBCOMMAND}"
        )
    ]
    assert len(redacted_records) == 1, (
        f"the redacted {FAILURE_EVENT!r} record must still be emitted "
        f"alongside the detail record — the detail SUPPLEMENTS it and never "
        f"replaces it, so log-based alerting keeps working at every level; "
        f"captured {[record.getMessage() for record in caplog.records]!r}"
    )
    redacted_record = redacted_records[0]
    assert redacted_record.levelname == REDACTED_RECORD_LEVEL, (
        f"the redacted record must be emitted at {REDACTED_RECORD_LEVEL}; got "
        f"{redacted_record.levelname}"
    )
    assert redacted_record.exc_info is None, (
        f"the {REDACTED_RECORD_LEVEL} record must carry NO exc_info — "
        f"attaching it is exactly what log.exception did and what published "
        f"the traceback to both normal sinks; got "
        f"{redacted_record.exc_info!r}"
    )
    assert REDACTION_MARKER in redacted_record.getMessage(), (
        f"the redacted record must carry {REDACTION_MARKER!r}; got "
        f"{redacted_record.getMessage()!r}"
    )

    # Assert — the level-filtered handlers rendered none of it, so the
    # gated detail never reached a normal operator channel.
    _assert_no_sensitive_disclosure(
        "result.stdout", result.stdout, DEBUG_GATE_SUBCOMMAND
    )
    _assert_no_sensitive_disclosure(
        "the operator log file", operator_log, DEBUG_GATE_SUBCOMMAND
    )
    assert FAILURE_DETAIL_EVENT not in operator_log, (
        f"the {DETAIL_RECORD_LEVEL} detail record must not be rendered into "
        f"the durable log while {REDACTED_RECORD_LEVEL}-level handlers are "
        f"configured; operator log={operator_log!r}"
    )

    # Assert — raising a logger's level changes nothing about the outcome.
    assert result.exit_code == EXIT_FAILURE, (
        f"`cli {DEBUG_GATE_SUBCOMMAND}` must exit {EXIT_FAILURE} regardless "
        f"of log level; got {result.exit_code}"
    )
    assert result.exception is sensitive_failure, (
        f"`cli {DEBUG_GATE_SUBCOMMAND}` must propagate the ORIGINAL exception "
        f"object regardless of log level; got id={id(result.exception)} "
        f"({type(result.exception).__name__}), expected "
        f"id={id(sensitive_failure)}"
    )


# ---------------------------------------------------------------------------
# Section G — the PROCESS boundary (CWE-209, CWE-532).
#
# Section F proved the redaction of run.py's own log sinks. It could not
# prove anything about the process's error stream, because every one of
# its invocations goes through ``CliRunner``, which catches the propagated
# exception before the interpreter can render it. That blind spot is not
# hypothetical: with Section F fully green, ``python run.py teams`` still
# exited 1 while printing, on stderr,
#
#     Traceback (most recent call last)
#     ...
#       File "<abs>/run.py", line 420, in teams
#       File "<abs>/<driver>.py", line 12, in _boom
#     _Boom: upstream request failed: https://stats.nba.com/x?<token>
#
# — the token, the traceback header, eight absolute source paths and their
# line numbers. Everything ``_log_failed_run`` withheld from the log sinks
# was republished on the one channel it does not govern.
#
# CD-4 — THE DEFECT THESE TESTS MOTIVATED, with its failing case:
#
# * Behaviour before the fix: ``if __name__ == "__main__": cli()``. The
#   bare ``raise`` closing each handler is mandatory (AAP §0.5.2.1
#   log-and-re-raise) and Click does not catch non-Click exceptions, so
#   the exception reached CPython's top-level handler and was rendered in
#   full. The failing case is the stderr transcript above, reproduced with
#   a pipeline raising ``SENSITIVE_FAILURE_MESSAGE``.
# * Behaviour after the fix: the entry point is ``run.py::main``, which
#   catches ``Exception`` — never ``BaseException`` — writes a redacted
#   ``run.aborted`` last-resort record, and raises ``SystemExit(1) from
#   None``. CPython prints nothing for a ``SystemExit`` carrying an int,
#   and ``from None`` suppresses the context, so stderr is EMPTY while the
#   exit status and both redacted records survive.
# * The change: ``main``, ``_log_aborted_process``, and one statement in
#   the ``__main__`` block. Every callback — bare ``raise``,
#   ``metrics.registry.inc``, logging — is byte-identical, which is why
#   Sections A-F keep passing unmodified.
# * Scope: Constraint C2 permits a non-test source change "to fix a
#   genuine bug found" when it is minimal and called out explicitly with
#   the failing case that motivated it (AAP §0.1.4). This is that
#   call-out; the reproduced transcript above is that failing case.
#
# Why these tests use NO CliRunner, and what each mechanism buys:
#
# * ``run_module.main([...])`` — the exact callable the ``__main__`` block
#   invokes, so the translation itself (status, ``from None``, records) is
#   asserted in-process against the real metrics registry and log sinks.
# * ``runpy.run_path(..., run_name="__main__")`` — executes run.py's
#   entry-point block, so a revert to ``cli()`` is caught even though
#   ``main`` itself would still be correct.
# * one real child process — the interpreter's genuine top-level handling,
#   which no in-process harness can imitate. It is the only test here
#   that spends ~0.5 s, and it is the only one that can observe what an
#   operator's terminal, CI log or cron mail would actually receive.
#
# Every expectation is structural (C1): an exit status, an exception
# class, a float counter value, an ordered dispatch census, a token this
# module injects, a field name read off ``run.py``'s own format strings,
# or CPython's documented ``SystemExit`` behaviour. Nothing is captured
# from a run.
# ---------------------------------------------------------------------------


class _LastResortLoggingFailure(RuntimeError):
    """Exception raised by a deliberately broken last-resort logging call.

    Used by exactly one test, to prove that ``run.py::main``'s
    ``try``/``finally`` keeps its promise when the log sink itself is the
    thing that is broken — a full disk, a revoked directory, a closed
    handler. Its message carries its own sentinel so a leak can be
    attributed to this exception rather than to the injected pipeline
    failure.
    """


def _assert_redacted_failure_record(channel: str, text: str, subcommand: str) -> None:
    """Assert the subcommand's redacted ``run.failed`` record is present and useful.

    Presence is asserted with the same rigour as absence: a "fix" that
    deleted the logging call would satisfy every confidentiality
    assertion in this section while leaving the operator with no record
    at all. Each of the three fields is checked separately so a failure
    names which one went missing.
    """
    expected_error_type = (
        f"{ERROR_TYPE_FIELD_PREFIX}{_SensitivePipelineFailure.__name__}"
    )
    assert f"{FAILURE_EVENT} subcommand={subcommand}" in text, (
        f"the subcommand's redacted {FAILURE_EVENT!r} record must survive the "
        f"process boundary on {channel}; redaction must not become silent "
        f"suppression. {channel}={text!r}"
    )
    assert expected_error_type in text, (
        f"the redacted record on {channel} must name the exception CLASS "
        f"{expected_error_type!r}, which is the static identifier that keeps "
        f"it triage-able. {channel}={text!r}"
    )
    assert REDACTION_MARKER in text, (
        f"the redacted record on {channel} must carry {REDACTION_MARKER!r} so "
        f"an operator knows detail exists and is withheld rather than absent. "
        f"{channel}={text!r}"
    )


def _assert_redacted_abort_record(
    channel: str,
    text: str,
    error_class_name: str,
) -> None:
    """Assert the process boundary's redacted ``run.aborted`` record is present.

    This is the record that makes ``main``'s silent ``SystemExit`` a
    *silent output*, not a *silent failure*: it states that the process
    gave up, with which status, and of what exception class. For a
    failure raised before a subcommand's ``try`` — an :exc:`OSError` from
    ``_build_collaborators``, for instance — it is the ONLY record
    written, so its three fields are asserted individually.
    """
    expected_event = (
        f"{PROCESS_ABORT_EVENT} {EXIT_STATUS_FIELD_PREFIX}{EXIT_FAILURE}"
    )
    assert expected_event in text, (
        f"the process boundary must record {expected_event!r} on {channel} so "
        f"the silent exit is still diagnosable. {channel}={text!r}"
    )
    assert f"{ERROR_TYPE_FIELD_PREFIX}{error_class_name}" in text, (
        f"the {PROCESS_ABORT_EVENT!r} record on {channel} must name the "
        f"exception class {error_class_name!r}. {channel}={text!r}"
    )
    assert REDACTION_MARKER in text, (
        f"the {PROCESS_ABORT_EVENT!r} record on {channel} must carry "
        f"{REDACTION_MARKER!r}. {channel}={text!r}"
    )


def _assert_no_abort_record(channel: str, text: str, invocation: str) -> None:
    """Assert no ``run.aborted`` record was written for ``invocation``.

    The counterpart to :func:`_assert_redacted_abort_record`. A boundary
    that logged an abort for a successful run, for ``ready``'s deliberate
    ``sys.exit(1)``, or for a Click usage error would be over-broad — it
    would be catching :exc:`SystemExit` — and would flood operators with
    false failures.
    """
    assert PROCESS_ABORT_EVENT not in text, (
        f"`{invocation}` must NOT write a {PROCESS_ABORT_EVENT!r} record on "
        f"{channel}: only a genuine escaped exception is an abort, and "
        f"SystemExit must pass through untouched. {channel}={text!r}"
    )


def _write_standalone_driver(tmp_path: Path) -> Path:
    """Write the child-process driver into ``tmp_path`` and return its path.

    The driver lives outside ``tests/`` so ``testpaths = tests`` can never
    collect it, and inside ``tmp_path`` so pytest deletes it. Its source is
    the fixed :data:`SUBPROCESS_DRIVER_SOURCE` constant — no interpolation
    — so what the child executes is auditable by reading this module.
    """
    driver = tmp_path / SUBPROCESS_DRIVER_NAME
    driver.write_text(SUBPROCESS_DRIVER_SOURCE, encoding="utf-8")
    return driver


def _child_environment(tmp_path: Path) -> Dict[str, str]:
    """Build the child's environment: every artifact path under ``tmp_path``.

    A child process cannot inherit the parent's ``monkeypatch.setattr`` on
    :mod:`config`, and ``config.py`` reads its ``NBA_*`` overrides once at
    import time, so the environment is the only way to redirect the child.
    All four artifact paths point inside ``tmp_path``, and
    ``NBA_LOG_LEVEL`` is REMOVED so the child runs at the default
    ``"INFO"`` — the state in which the DEBUG detail record must stay
    unrendered.
    """
    child_output = tmp_path / "child_output"
    child_logs = tmp_path / "child_logs"

    environment = dict(os.environ)
    environment.pop(CHILD_LOG_LEVEL_ENV, None)
    environment[CHILD_OUTPUT_DIR_ENV] = str(child_output)
    environment[CHILD_CHECKPOINT_PATH_ENV] = str(child_output / "checkpoint.json")
    environment[CHILD_LOG_DIR_ENV] = str(child_logs)
    environment[CHILD_LOG_FILE_ENV] = str(child_logs / "pipeline.log")
    environment[CHILD_REPO_ROOT_ENV] = str(REPO_ROOT)
    environment[CHILD_FAILURE_MESSAGE_ENV] = SENSITIVE_FAILURE_MESSAGE
    environment[CHILD_PIPELINE_ATTR_ENV] = CHILD_PROBE_PIPELINE_ATTR
    return environment


@pytest.mark.parametrize("subcommand,pipeline_label", DATA_SUBCOMMAND_LABELS)
def test_process_entry_point_exits_one_and_renders_nothing_for_a_failed_subcommand(
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
    subcommand: str,
    pipeline_label: str,
) -> None:
    """`run.py::main` turns a failed subcommand into a silent, redacted exit 1.

    Invoked WITHOUT ``CliRunner``, so nothing stands between the
    propagated exception and the boundary under test. Three properties
    are asserted together because only their conjunction is the contract:

    * the status is exactly ``EXIT_FAILURE`` — a failure is never
      reported as success, and never as some other status;
    * the raised object is a ``SystemExit`` whose ``__cause__`` is
      ``None`` and whose ``__suppress_context__`` is ``True``, which is
      what ``raise ... from None`` means and what stops CPython printing
      the original exception as this one's context;
    * the original failure is nevertheless retained as
      ``__context__`` — the boundary redacts and translates, it does not
      erase causality.

    Then every channel is checked: ``stderr`` must be EXACTLY empty, and
    the complete :data:`FORBIDDEN_DISCLOSURES` tuple must be absent from
    stdout, stderr and the durable log. Both redacted records must be
    present and useful, the error counter must still read exactly
    ``1.0``, and the dispatch census must show only this subcommand ran.

    Mutation detected: reverting the ``__main__`` block or ``main`` to let
    the exception escape (stderr stops being empty and the sentinel,
    traceback header and absolute paths all reappear); dropping ``from
    None`` (``__suppress_context__`` flips and CPython prints the
    original); re-raising the original instead of ``SystemExit``;
    exiting ``0``; or deleting either redacted record.
    """
    # Arrange — a failure whose message is the confidential payload.
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[subcommand],
        "run",
        _make_failing_recorder(recorder, subcommand, sensitive_failure),
    )

    # Act — the real process entry point, no runner in the way.
    with pytest.raises(SystemExit) as exit_info:
        run_module.main([subcommand, "--season", config.DEFAULT_SEASON])
    captured = capsys.readouterr()
    operator_log = _read_operator_log()

    # Assert — the exact exit status, and the exact exception structure
    # that keeps the interpreter silent.
    assert exit_info.value.code == EXIT_FAILURE, (
        f"`run.py {subcommand}` must exit {EXIT_FAILURE} when its pipeline "
        f"raises; got {exit_info.value.code!r}"
    )
    assert exit_info.value.__cause__ is None, (
        f"the SystemExit must be raised `from None`, so its __cause__ stays "
        f"None and the interpreter has no chained exception to render; got "
        f"{exit_info.value.__cause__!r}"
    )
    assert exit_info.value.__suppress_context__ is True, (
        f"`from None` must set __suppress_context__, which is what stops "
        f"CPython printing the original exception as this SystemExit's "
        f"context; got {exit_info.value.__suppress_context__!r}"
    )
    assert exit_info.value.__context__ is sensitive_failure, (
        f"the ORIGINAL failure must remain the SystemExit's __context__ — the "
        f"boundary redacts causality, it does not erase it; got "
        f"id={id(exit_info.value.__context__)} "
        f"({type(exit_info.value.__context__).__name__}), expected "
        f"id={id(sensitive_failure)}"
    )

    # Assert — stderr is empty, and nothing confidential reached any of
    # the three channels this process can write.
    assert captured.err == "", (
        f"`run.py {subcommand}` must render NOTHING on stderr after a failed "
        f"run: CPython prints nothing for a SystemExit carrying an int, which "
        f"is the whole point of the translation; got {captured.err!r}"
    )
    _assert_no_sensitive_disclosure("stdout", captured.out, subcommand)
    _assert_no_sensitive_disclosure("stderr", captured.err, subcommand)
    _assert_no_sensitive_disclosure("the operator log file", operator_log, subcommand)

    # Assert — both redacted records survive on both normal channels.
    for channel, text in (
        ("stdout", captured.out),
        ("the operator log file", operator_log),
    ):
        _assert_redacted_failure_record(channel, text, subcommand)
        _assert_redacted_abort_record(
            channel, text, _SensitivePipelineFailure.__name__
        )

    # Assert — translation cost none of the Section A-C contracts.
    assert recorder == [subcommand], (
        f"`run.py {subcommand}` must dispatch exactly its own pipeline and no "
        f"sibling; dispatch census={recorder!r} expected {[subcommand]!r}"
    )
    observed_error = _runs_counter(pipeline_label, OUTCOME_ERROR)
    assert observed_error == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{pipeline_label}",outcome="{OUTCOME_ERROR}"}} '
        f"is {observed_error}; expected {EXPECTED_COUNTER_HIT} — the process "
        f"boundary must not disturb the metric the callback emitted"
    )
    observed_success = _runs_counter(pipeline_label, OUTCOME_SUCCESS)
    assert observed_success == EXPECTED_COUNTER_MISS, (
        f'{RUNS_COUNTER}{{pipeline="{pipeline_label}",'
        f'outcome="{OUTCOME_SUCCESS}"}} is {observed_success}; expected '
        f"{EXPECTED_COUNTER_MISS} — a failed run is never also a success"
    )


def test_process_entry_point_exits_one_and_renders_nothing_for_a_failed_all_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
) -> None:
    """A failed ``all`` run is redacted and silent at the process boundary too.

    ``all_cmd`` has its own ``except`` block and is the command operators
    actually schedule — the one whose stdout and stderr a cron wrapper or
    CI job retains — so it gets its own process-boundary coverage rather
    than being assumed to follow the five data subcommands.

    Fail-fast is re-asserted here because the translation must not change
    *when* the unwinding happens: the census must still show only the
    first pipeline of the documented order.

    Mutation detected: wiring the redacting entry point for the five data
    subcommands but leaving the aggregate on the raw ``cli()`` path; or
    letting the boundary's abort record be written while the aggregate's
    own redacted record is lost.
    """
    # Arrange — fail the FIRST pipeline of the binding order.
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[FIRST_ALL_PIPELINE],
        "run",
        _make_failing_recorder(recorder, FIRST_ALL_PIPELINE, sensitive_failure),
    )

    # Act
    with pytest.raises(SystemExit) as exit_info:
        run_module.main(["all", "--season", config.DEFAULT_SEASON])
    captured = capsys.readouterr()
    operator_log = _read_operator_log()

    # Assert — status and exception structure.
    assert exit_info.value.code == EXIT_FAILURE, (
        f"`run.py all` must exit {EXIT_FAILURE} when a pipeline raises; got "
        f"{exit_info.value.code!r}"
    )
    assert exit_info.value.__suppress_context__ is True, (
        f"`run.py all` must raise its SystemExit `from None`; got "
        f"__suppress_context__={exit_info.value.__suppress_context__!r}"
    )
    assert exit_info.value.__context__ is sensitive_failure, (
        f"`run.py all` must retain the ORIGINAL failure as __context__; got "
        f"id={id(exit_info.value.__context__)} "
        f"({type(exit_info.value.__context__).__name__}), expected "
        f"id={id(sensitive_failure)}"
    )

    # Assert — channels.
    assert captured.err == "", (
        f"`run.py all` must render NOTHING on stderr after a failed run; got "
        f"{captured.err!r}"
    )
    _assert_no_sensitive_disclosure("stdout", captured.out, "all")
    _assert_no_sensitive_disclosure("stderr", captured.err, "all")
    _assert_no_sensitive_disclosure("the operator log file", operator_log, "all")
    for channel, text in (
        ("stdout", captured.out),
        ("the operator log file", operator_log),
    ):
        _assert_redacted_failure_record(channel, text, "all")
        _assert_redacted_abort_record(
            channel, text, _SensitivePipelineFailure.__name__
        )

    # Assert — fail-fast and metrics survive the translation.
    assert recorder == EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE, (
        f"`run.py all` must still stop after the first failing pipeline; "
        f"dispatch census={recorder!r} expected "
        f"{EXPECTED_ALL_ORDER_AFTER_FIRST_FAILURE!r}"
    )
    observed_error = _runs_counter(ALL_PIPELINE_LABEL, OUTCOME_ERROR)
    assert observed_error == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{ALL_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_HIT}"
    )


def test_process_entry_point_preserves_a_successful_exit_status_and_logs_no_abort(
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
) -> None:
    """A successful run still exits 0 through the boundary, with no abort record.

    The boundary must be invisible on the happy path. Click's standalone
    mode ends a successful invocation in ``ctx.exit()``, which becomes
    ``SystemExit(0)`` — a :exc:`BaseException` that ``main``'s
    ``except Exception`` cannot see — so the status passes straight
    through and the success counter is emitted exactly once.

    The absence of a ``run.aborted`` record is asserted on both channels
    because a boundary that logged an abort for every invocation would
    make every successful nightly run look like a failure to log-based
    alerting.

    Mutation detected: widening ``main``'s ``except`` to
    :exc:`BaseException` (or adding ``except SystemExit``), which would
    swallow the success status and convert exit 0 into exit 1; or writing
    the last-resort record unconditionally instead of only on the failure
    path.
    """
    # Arrange — every pipeline succeeds.
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)

    # Act
    with pytest.raises(SystemExit) as exit_info:
        run_module.main([DEBUG_GATE_SUBCOMMAND, "--season", config.DEFAULT_SEASON])
    captured = capsys.readouterr()
    operator_log = _read_operator_log()

    # Assert — status and dispatch.
    assert exit_info.value.code == EXIT_SUCCESS, (
        f"`run.py {DEBUG_GATE_SUBCOMMAND}` must exit {EXIT_SUCCESS} when its "
        f"pipeline succeeds; got {exit_info.value.code!r}"
    )
    assert recorder == [DEBUG_GATE_SUBCOMMAND], (
        f"`run.py {DEBUG_GATE_SUBCOMMAND}` must dispatch exactly its own "
        f"pipeline; dispatch census={recorder!r} expected "
        f"{[DEBUG_GATE_SUBCOMMAND]!r}"
    )

    # Assert — no abort record anywhere, and stderr untouched.
    invocation = f"run.py {DEBUG_GATE_SUBCOMMAND}"
    _assert_no_abort_record("stdout", captured.out, invocation)
    _assert_no_abort_record("the operator log file", operator_log, invocation)
    assert captured.err == "", (
        f"a successful `{invocation}` must write nothing to stderr; got "
        f"{captured.err!r}"
    )

    # Assert — the success metric is emitted exactly once, error stays 0.
    observed_success = _runs_counter(DEBUG_GATE_PIPELINE_LABEL, OUTCOME_SUCCESS)
    assert observed_success == EXPECTED_COUNTER_HIT, (
        f'{RUNS_COUNTER}{{pipeline="{DEBUG_GATE_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_SUCCESS}"}} is {observed_success}; expected '
        f"{EXPECTED_COUNTER_HIT}"
    )
    observed_error = _runs_counter(DEBUG_GATE_PIPELINE_LABEL, OUTCOME_ERROR)
    assert observed_error == EXPECTED_COUNTER_MISS, (
        f'{RUNS_COUNTER}{{pipeline="{DEBUG_GATE_PIPELINE_LABEL}",'
        f'outcome="{OUTCOME_ERROR}"}} is {observed_error}; expected '
        f"{EXPECTED_COUNTER_MISS}"
    )


def test_process_entry_point_preserves_readys_deliberate_nonzero_exit_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
) -> None:
    """``ready``'s deliberate ``sys.exit(1)`` passes through unconverted and unlogged.

    A deliberate non-zero exit and an aborted failure are different
    events that happen to share a status, and the boundary must keep them
    distinguishable: ``ready`` still echoes its probe body first, exits
    ``1`` on its own terms, and produces NO ``run.aborted`` record —
    because ``sys.exit`` raises :exc:`SystemExit`, which
    ``except Exception`` does not catch.

    Mutation detected: catching :exc:`BaseException` (or
    :exc:`SystemExit`) in ``main``, which would relabel every deliberate
    exit as an abort and, worse, could mask ``ready``'s verdict behind a
    generic failure record.
    """
    # Arrange — a deterministic not-ready verdict.
    monkeypatch.setattr(
        run_module.health,
        "check_readiness",
        _make_readiness_probe(NOT_READY_PROBE_RESULT),
    )

    # Act
    with pytest.raises(SystemExit) as exit_info:
        run_module.main(["ready"])
    captured = capsys.readouterr()

    # Assert — the status is ready's own, and the body was echoed first.
    assert exit_info.value.code == EXIT_FAILURE, (
        f"`run.py ready` must exit {EXIT_FAILURE} when the probe reports "
        f"{NOT_READY_PROBE_RESULT['status']!r}; got {exit_info.value.code!r}"
    )
    assert json.loads(captured.out) == NOT_READY_PROBE_RESULT, (
        f"`run.py ready` must echo the probe body verbatim BEFORE exiting; "
        f"parsed stdout={json.loads(captured.out)!r} expected "
        f"{NOT_READY_PROBE_RESULT!r}"
    )
    assert exit_info.value.__context__ is None, (
        f"a deliberate `sys.exit(1)` is not an aborted failure, so the "
        f"SystemExit must carry no exception context; got "
        f"{exit_info.value.__context__!r}"
    )

    # Assert — nothing was treated as an abort.
    _assert_no_abort_record("stdout", captured.out, "run.py ready")
    assert captured.err == "", (
        f"`run.py ready` must emit its verdict on stdout and nothing on "
        f"stderr; got {captured.err!r}"
    )


def test_process_entry_point_preserves_clicks_usage_error_exit_status_and_message(
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
) -> None:
    """A Click usage error keeps exit 2 and keeps Click's own message on stderr.

    The boundary must be narrow in both directions. Click resolves an
    unregistered subcommand into a ``UsageError`` *inside*
    ``Group.main``, which in standalone mode prints the message and calls
    ``sys.exit(2)`` — so ``main`` sees a :exc:`SystemExit` and leaves it
    alone. That matters for usability: this message is generated from
    this project's own command registry, never from payload or upstream
    data, so it is exactly the kind of stderr output that must SURVIVE.

    Mutation detected: catching :exc:`BaseException` in ``main`` (usage
    errors would collapse to exit 1 with no explanation, and ``--help``
    would break the same way); or "hardening" the boundary by silencing
    stderr wholesale rather than by not rendering exceptions.
    """
    # Act — no arrange step: the subcommand simply does not exist.
    with pytest.raises(SystemExit) as exit_info:
        run_module.main([UNKNOWN_SUBCOMMAND])
    captured = capsys.readouterr()

    # Assert — Click's own status and message survive intact.
    assert exit_info.value.code == EXIT_USAGE_ERROR, (
        f"an unknown subcommand must keep Click's usage status "
        f"{EXIT_USAGE_ERROR}; got {exit_info.value.code!r}"
    )
    assert CLICK_UNKNOWN_COMMAND_MESSAGE in captured.err, (
        f"Click's operator-facing usage message "
        f"{CLICK_UNKNOWN_COMMAND_MESSAGE!r} must still reach stderr — the "
        f"boundary suppresses rendered EXCEPTIONS, not all diagnostics; "
        f"stderr={captured.err!r}"
    )
    assert TRACEBACK_HEADER not in captured.err, (
        f"a usage error must never print a traceback; stderr={captured.err!r}"
    )

    # Assert — a usage error is not an abort.
    _assert_no_abort_record("stdout", captured.out, f"run.py {UNKNOWN_SUBCOMMAND}")
    _assert_no_abort_record("stderr", captured.err, f"run.py {UNKNOWN_SUBCOMMAND}")


def test_process_entry_point_exit_status_survives_a_failing_last_resort_record(
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
) -> None:
    """A broken last-resort log call still yields a silent, redacted exit 1.

    ``main`` writes its last-resort record inside a ``try``/``finally``
    precisely because the log sink can be the casualty — a full disk, a
    revoked directory, a closed handler. If that call raises, the
    ``finally`` still raises ``SystemExit(1) from None``, so the second
    exception replaces the first and its context is suppressed: neither
    the injected pipeline failure NOR the logging failure can reach the
    traceback renderer, and the status is never downgraded to ``0``.

    The exception chain is asserted exactly — ``__context__`` is the
    logging failure and ITS ``__context__`` is the original pipeline
    failure — because that chain is what an in-process caller can still
    inspect, and losing it would mean the boundary destroyed causality
    rather than withholding it.

    Mutation detected: replacing the ``try``/``finally`` with a plain
    sequence of statements, so a logging failure escapes ``main`` and
    CPython renders IT — path and all — instead of exiting quietly;
    dropping ``from None`` so the chain is printed; or letting the
    logging failure change the exit status.
    """
    # Arrange — a failing pipeline AND a failing last-resort logger.
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[DEBUG_GATE_SUBCOMMAND],
        "run",
        _make_failing_recorder(recorder, DEBUG_GATE_SUBCOMMAND, sensitive_failure),
    )

    def _raise_logging_failure() -> None:
        raise _LastResortLoggingFailure(LAST_RESORT_FAILURE_MESSAGE)

    monkeypatch.setattr(run_module, "_log_aborted_process", _raise_logging_failure)

    # Act
    with pytest.raises(SystemExit) as exit_info:
        run_module.main([DEBUG_GATE_SUBCOMMAND, "--season", config.DEFAULT_SEASON])
    captured = capsys.readouterr()

    # Assert — the status and the suppression survive the second failure.
    assert exit_info.value.code == EXIT_FAILURE, (
        f"a broken last-resort record must not change the exit status; "
        f"expected {EXIT_FAILURE}, got {exit_info.value.code!r}"
    )
    assert exit_info.value.__suppress_context__ is True, (
        f"the SystemExit must still be raised `from None` when the last-resort "
        f"record fails; got "
        f"__suppress_context__={exit_info.value.__suppress_context__!r}"
    )
    assert type(exit_info.value.__context__) is _LastResortLoggingFailure, (
        f"the exception in flight when the finally-clause runs is the LOGGING "
        f"failure, so it must be the SystemExit's __context__; got "
        f"{type(exit_info.value.__context__).__name__}"
    )
    assert exit_info.value.__context__.__context__ is sensitive_failure, (
        f"the ORIGINAL pipeline failure must remain reachable one link "
        f"further down the chain; got "
        f"id={id(exit_info.value.__context__.__context__)} "
        f"({type(exit_info.value.__context__.__context__).__name__}), expected "
        f"id={id(sensitive_failure)}"
    )

    # Assert — neither exception's text reached a channel.
    assert captured.err == "", (
        f"neither the pipeline failure nor the logging failure may be rendered "
        f"on stderr; got {captured.err!r}"
    )
    _assert_no_sensitive_disclosure("stdout", captured.out, DEBUG_GATE_SUBCOMMAND)
    _assert_no_sensitive_disclosure("stderr", captured.err, DEBUG_GATE_SUBCOMMAND)
    for channel, text in (("stdout", captured.out), ("stderr", captured.err)):
        assert LAST_RESORT_SENTINEL not in text, (
            f"the LOGGING failure's message must not be disclosed on {channel} "
            f"either — a log-sink error carries a private filesystem path; "
            f"{channel}={text!r}"
        )

    # Assert — the callback's own redacted record was still written, so the
    # run is diagnosable even though the boundary's record was lost.
    _assert_redacted_failure_record("stdout", captured.out, DEBUG_GATE_SUBCOMMAND)


def test_module_main_block_dispatches_through_the_redacting_entry_point(
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
) -> None:
    """``run.py``'s ``__main__`` block routes through ``main``, not bare ``cli()``.

    Every other test in this section calls ``run_module.main`` directly,
    so all of them would still pass if the ``if __name__ ==
    "__main__"`` block were reverted to ``cli()`` — the wiring, not the
    wrapper, is what this test pins. ``runpy.run_path(...,
    run_name="__main__")`` executes that block for real, in-process:
    ``run.py``'s body runs again in a fresh namespace, the already
    imported ``pipelines`` modules (and therefore the monkeypatched
    ``run`` attributes) are reused, and the entry-point statement fires.

    Under the correct wiring this raises ``SystemExit(1)`` with a
    suppressed context; under the reverted wiring the injected
    ``_SensitivePipelineFailure`` escapes ``run_path`` instead, and
    ``pytest.raises(SystemExit)`` fails loudly with the disclosure that
    the real process would have printed.

    Mutation detected: reverting the entry point to ``cli()``; guarding
    it with a different dunder check so it never fires; or calling
    ``main`` without letting its ``SystemExit`` propagate.
    """
    # Arrange — the argv a shell would supply, plus a failing pipeline.
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[DEBUG_GATE_SUBCOMMAND],
        "run",
        _make_failing_recorder(recorder, DEBUG_GATE_SUBCOMMAND, sensitive_failure),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            Path(CLI_SOURCE_PATH).name,
            DEBUG_GATE_SUBCOMMAND,
            "--season",
            config.DEFAULT_SEASON,
        ],
    )

    # Act — execute run.py exactly as `python run.py ...` does.
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(CLI_SOURCE_PATH, run_name="__main__")
    captured = capsys.readouterr()
    operator_log = _read_operator_log()

    # Assert — the block produced the redacting boundary's SystemExit.
    assert exit_info.value.code == EXIT_FAILURE, (
        f"executing run.py as __main__ must exit {EXIT_FAILURE} when the "
        f"pipeline raises; got {exit_info.value.code!r}"
    )
    assert exit_info.value.__suppress_context__ is True, (
        f"the __main__ block must reach the boundary that raises `from None`; "
        f"got __suppress_context__={exit_info.value.__suppress_context__!r}"
    )
    assert exit_info.value.__context__ is sensitive_failure, (
        f"the SystemExit produced by the __main__ block must carry the "
        f"ORIGINAL failure as its context; got "
        f"id={id(exit_info.value.__context__)} "
        f"({type(exit_info.value.__context__).__name__}), expected "
        f"id={id(sensitive_failure)}"
    )

    # Assert — the entry point disclosed nothing and recorded the abort.
    assert captured.err == "", (
        f"executing run.py as __main__ must render nothing on stderr; got "
        f"{captured.err!r}"
    )
    _assert_no_sensitive_disclosure("stdout", captured.out, DEBUG_GATE_SUBCOMMAND)
    _assert_no_sensitive_disclosure("stderr", captured.err, DEBUG_GATE_SUBCOMMAND)
    _assert_redacted_abort_record(
        "stdout", captured.out, _SensitivePipelineFailure.__name__
    )
    _assert_redacted_abort_record(
        "the operator log file", operator_log, _SensitivePipelineFailure.__name__
    )
    assert recorder == [DEBUG_GATE_SUBCOMMAND], (
        f"the __main__ block must dispatch exactly the requested subcommand; "
        f"dispatch census={recorder!r} expected {[DEBUG_GATE_SUBCOMMAND]!r}"
    )


def test_standalone_child_process_renders_no_exception_detail_on_stderr(
    tmp_path: Path,
) -> None:
    """A real child process exits 1 with an EMPTY stderr and redacted stdout.

    This is the only test in the repository that observes the genuine
    interpreter top level, and it exists because no in-process harness
    can: ``CliRunner`` catches the exception, and even a direct
    ``main([...])`` call is judged by assertions rather than by CPython's
    own rendering. Here a real ``python`` child runs ``run.py``'s entry
    point with one pipeline replaced by a raiser, and the parent inspects
    exactly what an operator's terminal, a CI log or cron mail would
    receive.

    ``stderr`` is asserted to be EXACTLY empty — the strongest available
    form of "nothing was rendered" — and the complete forbidden-disclosure
    tuple, extended with the driver's own path, is additionally checked
    against stderr, stdout and the child's durable log so a failure names
    precisely what leaked and where. The redacted records must be present
    on stdout and in the child's log, and both DEBUG detail records must
    be absent from that log, which proves the gating holds at the default
    ``config.LOG_LEVEL`` in a real process rather than only under a
    test-raised logger level.

    Isolation: the child writes only inside ``tmp_path`` (all four
    ``NBA_*`` path overrides are redirected there), runs with ``-B`` so it
    leaves no bytecode behind, starts in ``tmp_path`` rather than the
    repository, performs no network I/O because its pipeline never runs,
    and is bounded by an explicit timeout. The parent needs neither
    ``tmp_output_dir`` nor ``tmp_log_dir``: it writes no artifact and
    emits no log record of its own, and ``monkeypatch.setattr`` could not
    reach the child anyway — which is exactly why the redirection is done
    through the environment.

    Mutation detected: reverting the entry point to ``cli()``, or any
    change that lets an exception reach CPython's top level — the
    sentinel, ``Traceback (most recent call last)``, ``File "..."``
    frames and absolute source paths all reappear, and stderr stops being
    empty.
    """
    # Arrange — the driver and a fully redirected child environment.
    driver = _write_standalone_driver(tmp_path)
    environment = _child_environment(tmp_path)
    child_log = Path(environment[CHILD_LOG_FILE_ENV])

    # Act — a real child process, no harness in the way.
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(driver),
            CHILD_PROBE_SUBCOMMAND,
            "--season",
            config.DEFAULT_SEASON,
        ],
        cwd=str(tmp_path),
        env=environment,
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
        check=False,
    )
    child_log_text = child_log.read_text(encoding="utf-8")

    # Assert — the process contract: non-zero status, silent stderr.
    assert completed.returncode == EXIT_FAILURE, (
        f"`python run.py {CHILD_PROBE_SUBCOMMAND}` must exit {EXIT_FAILURE} "
        f"when its pipeline raises; got {completed.returncode}. "
        f"stdout={completed.stdout!r} stderr={completed.stderr!r}"
    )
    assert completed.stderr == "", (
        f"the child process must render NOTHING on stderr: the exception is "
        f"translated into SystemExit({EXIT_FAILURE}) `from None`, and CPython "
        f"prints nothing for a SystemExit carrying an int. This is the exact "
        f"disclosure CD-4 fixed. stderr={completed.stderr!r}"
    )

    # Assert — no forbidden disclosure on any channel the child wrote.
    child_forbidden = FORBIDDEN_DISCLOSURES + (
        ("absolute path of the child driver script", str(driver)),
    )
    for channel, text in (
        ("child stderr", completed.stderr),
        ("child stdout", completed.stdout),
        ("the child's durable log file", child_log_text),
    ):
        for description, forbidden in child_forbidden:
            assert forbidden not in text, (
                f"the child process disclosed the {description} on {channel}: "
                f"{forbidden!r} must never appear there (CWE-209, CWE-532). "
                f"{channel}={text!r}"
            )

    # Assert — the redacted records ARE there, on both channels.
    for channel, text in (
        ("child stdout", completed.stdout),
        ("the child's durable log file", child_log_text),
    ):
        assert f"{FAILURE_EVENT} subcommand={CHILD_PROBE_SUBCOMMAND}" in text, (
            f"the child must still record {FAILURE_EVENT!r} for "
            f"{CHILD_PROBE_SUBCOMMAND!r} on {channel}; {channel}={text!r}"
        )
        _assert_redacted_abort_record(channel, text, CHILD_FAILURE_CLASS_NAME)

    # Assert — both DEBUG detail records stayed unrendered at the default
    # log level, so the gating is real and not a test artefact.
    for detail_event in (FAILURE_DETAIL_EVENT, PROCESS_ABORT_DETAIL_EVENT):
        assert detail_event not in child_log_text, (
            f"{detail_event!r} is emitted at {DETAIL_RECORD_LEVEL} and must be "
            f"discarded by the default config.LOG_LEVEL handlers, so it must "
            f"not reach the child's durable log; log={child_log_text!r}"
        )


def test_process_abort_detail_is_emitted_only_on_the_debug_gated_channel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_output_dir,
    tmp_log_dir,
    capsys: pytest.CaptureFixture,
    caplog,
) -> None:
    """The boundary GATES its traceback on DEBUG rather than destroying it.

    The exact counterpart, for the process boundary's own record, of the
    Section F test that pins ``run.failed.detail``. Without it, "stderr is
    empty" would be satisfiable by simply throwing the diagnostics away —
    and for a failure raised *before* a subcommand's ``try`` (an
    :exc:`OSError` from ``_build_collaborators``, say) that would leave no
    recoverable causality anywhere in the system, because
    ``run.aborted.detail`` is then the only record carrying it.

    The two records are asserted separately because their obligations are
    opposite: the ERROR record must carry NO ``exc_info`` (attaching it is
    exactly what ``log.exception`` did wrong), while the DEBUG record must
    carry the ORIGINAL exception object so an opted-in operator recovers
    the true chain rather than a reconstruction.

    Only the ``"cli"`` logger is raised to DEBUG, via
    ``caplog.at_level(..., logger=...)``. ``utils.logger._configure`` sets
    the level on the ROOT logger and on both handlers — never on a named
    logger — so the record is emitted and captured in-process while both
    normal sinks, being level-filtered, render nothing of it. That is
    asserted too: raising a logger's level must not turn stdout or the
    durable log into a disclosure channel.

    Mutation detected: deleting the ``run.aborted.detail`` record (the
    traceback is destroyed, not gated); dropping its ``exc_info=True``
    (the record survives carrying no causality); promoting it to INFO or
    above (both handlers would render the traceback, re-creating the very
    disclosure CD-4 removed); or attaching ``exc_info`` to the ERROR
    record.
    """
    # Arrange — raise ONLY the boundary logger's level, then fail.
    sensitive_failure = _SensitivePipelineFailure(SENSITIVE_FAILURE_MESSAGE)
    recorder: List[str] = []
    _install_recorders(monkeypatch, recorder)
    monkeypatch.setattr(
        PIPELINE_MODULES[DEBUG_GATE_SUBCOMMAND],
        "run",
        _make_failing_recorder(recorder, DEBUG_GATE_SUBCOMMAND, sensitive_failure),
    )

    # Act
    with caplog.at_level(logging.DEBUG, logger=PROCESS_LOGGER_NAME):
        with pytest.raises(SystemExit) as exit_info:
            run_module.main(
                [DEBUG_GATE_SUBCOMMAND, "--season", config.DEFAULT_SEASON]
            )
    captured = capsys.readouterr()
    operator_log = _read_operator_log()

    # Assert — exactly one detail record, at DEBUG, carrying the ORIGINAL
    # exception object rather than a copy of its text.
    detail_records = [
        record
        for record in caplog.records
        if record.getMessage().startswith(PROCESS_ABORT_DETAIL_EVENT)
    ]
    assert len(detail_records) == 1, (
        f"the process boundary must emit exactly one "
        f"{PROCESS_ABORT_DETAIL_EVENT!r} record per aborted run so causality "
        f"stays recoverable; captured "
        f"{[record.getMessage() for record in caplog.records]!r}"
    )
    detail_record = detail_records[0]
    assert detail_record.levelname == DETAIL_RECORD_LEVEL, (
        f"the {PROCESS_ABORT_DETAIL_EVENT!r} record must be emitted at "
        f"{DETAIL_RECORD_LEVEL} so the default INFO handlers discard it; got "
        f"{detail_record.levelname}"
    )
    # ``or (None, None, None)`` keeps the comparison well defined under the
    # mutation it exists to catch: with ``exc_info=True`` dropped the pair
    # becomes ``(None, None)`` and the assertion reports a readable
    # diagnosis instead of raising ``TypeError`` on a subscript.
    detail_exc_info = detail_record.exc_info or (None, None, None)
    assert detail_exc_info[:2] == (_SensitivePipelineFailure, sensitive_failure), (
        f"the {PROCESS_ABORT_DETAIL_EVENT!r} record must carry exc_info whose "
        f"class is {_SensitivePipelineFailure.__name__} and whose value is the "
        f"raised object; got {detail_exc_info[:2]!r}, expected "
        f"{(_SensitivePipelineFailure, sensitive_failure)!r}"
    )
    assert detail_record.exc_info[1] is sensitive_failure, (
        f"the {PROCESS_ABORT_DETAIL_EVENT!r} record must carry the ORIGINAL "
        f"exception object; got id={id(detail_record.exc_info[1])} "
        f"({type(detail_record.exc_info[1]).__name__}), expected "
        f"id={id(sensitive_failure)}"
    )

    # Assert — the redacted ERROR record accompanies it and carries no
    # exc_info of its own.
    redacted_records = [
        record
        for record in caplog.records
        if record.getMessage().startswith(
            f"{PROCESS_ABORT_EVENT} {EXIT_STATUS_FIELD_PREFIX}"
        )
    ]
    assert len(redacted_records) == 1, (
        f"the redacted {PROCESS_ABORT_EVENT!r} record must still be emitted "
        f"alongside the detail record — the detail SUPPLEMENTS it and never "
        f"replaces it; captured "
        f"{[record.getMessage() for record in caplog.records]!r}"
    )
    redacted_record = redacted_records[0]
    assert redacted_record.levelname == REDACTED_RECORD_LEVEL, (
        f"the redacted {PROCESS_ABORT_EVENT!r} record must be emitted at "
        f"{REDACTED_RECORD_LEVEL}; got {redacted_record.levelname}"
    )
    assert redacted_record.exc_info is None, (
        f"the {REDACTED_RECORD_LEVEL} {PROCESS_ABORT_EVENT!r} record must "
        f"carry NO exc_info — attaching it is what published tracebacks to "
        f"both normal sinks; got {redacted_record.exc_info!r}"
    )
    assert REDACTION_MARKER in redacted_record.getMessage(), (
        f"the redacted {PROCESS_ABORT_EVENT!r} record must carry "
        f"{REDACTION_MARKER!r}; got {redacted_record.getMessage()!r}"
    )

    # Assert — the level-filtered handlers rendered none of the gated
    # detail, and the outcome is unchanged by the raised logger level.
    _assert_no_sensitive_disclosure("stdout", captured.out, DEBUG_GATE_SUBCOMMAND)
    _assert_no_sensitive_disclosure("stderr", captured.err, DEBUG_GATE_SUBCOMMAND)
    _assert_no_sensitive_disclosure(
        "the operator log file", operator_log, DEBUG_GATE_SUBCOMMAND
    )
    assert PROCESS_ABORT_DETAIL_EVENT not in operator_log, (
        f"the {DETAIL_RECORD_LEVEL} record must not be rendered into the "
        f"durable log while {REDACTED_RECORD_LEVEL}-level handlers are "
        f"configured; operator log={operator_log!r}"
    )
    assert exit_info.value.code == EXIT_FAILURE, (
        f"`run.py {DEBUG_GATE_SUBCOMMAND}` must exit {EXIT_FAILURE} regardless "
        f"of log level; got {exit_info.value.code!r}"
    )
