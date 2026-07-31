"""Command-line entry point for the NBA Data Ingestion Pipeline.

This module is the *sole* CLI entry point for the system. It satisfies
Feature F-001 (Command-Line Interface), Validation Gate 9 (Integration
Wiring Verification), Validation Gate 13 (Registration-Invocation
Pairing), and is the canonical read-site for :data:`config.DEFAULT_SEASON`
(Validation Gate 12 — Config Propagation Tracing).

Layout
------
* A single :class:`click.Group` named :data:`cli` exposes nine
  subcommands:

  * Five **domain** subcommands — ``players``, ``teams``, ``games``,
    ``lineups``, ``schedule`` — each dispatches to the corresponding
    :mod:`pipelines.ingest_*` module's ``run()`` function.
  * One **aggregate** subcommand — ``all`` — invokes every pipeline in
    the binding dependency order ``schedule → games → teams → players →
    lineups`` (AAP §0.4.5).
  * Three **diagnostic** subcommands — ``health``, ``ready``,
    ``metrics`` — expose the Observability-rule surface (AAP §0.7.3.1).

* Every data subcommand accepts ``--season STRING`` defaulting to
  :data:`config.DEFAULT_SEASON` (current default: ``"2025-26"``).
* Collaborator objects (``NBAClient``, ``CSVWriter``,
  ``CheckpointManager``, ``RateLimiter``) are composed ONCE per
  subcommand invocation via :func:`_build_collaborators`, which also
  mints and binds a correlation ID to the current execution context via
  :mod:`utils.correlation`. Every downstream log record auto-carries
  that ID through :class:`utils.correlation.CorrelationAdapter`.

Operational-rule posture of this file
-------------------------------------
* **Rule 1 — Single HTTP Client.** This file does NOT import
  :mod:`requests` or call ``requests.*`` directly. All HTTP traffic
  is mediated by the injected :class:`NBAClient` instance. Verified by
  ``tests/invariants/test_rule1_sole_http_client.py``.
* **Rule 7 — Pluggable Storage.** This file does NOT call
  ``DataFrame.to_csv``. The injected :class:`CSVWriter` is the only
  call site in production code. Verified by
  ``tests/invariants/test_rule7_basewriter_only.py``.
* **Rule 6 — Fail-Safe Iteration** is enforced exclusively in
  ``pipelines/ingest_games.py``. This CLI module wraps each pipeline
  invocation in a per-subcommand ``try/except`` that *logs and
  re-raises* (AAP §0.5.2.1) so Click prints a non-zero exit status;
  it never silently swallows exceptions.
* **Failure-output confidentiality.** The log half of that
  "log and re-raise" is deliberately **redacted**: every ``except``
  block calls :func:`_log_failed_run`, which emits the exception's
  *class name* on the normal ERROR channels and routes the message and
  traceback to the DEBUG-gated diagnostic channel. Exception text is
  arbitrary, frequently upstream-controlled data (URLs with query
  strings, filesystem paths, payload cell values) and tracebacks
  publish absolute source paths and line numbers, so neither belongs in
  the console/CI stream or the durable rotating log by default
  (CWE-209, CWE-532). See :func:`_log_failed_run` for the full
  rationale and for what is intentionally *not* changed.
* **Process-boundary confidentiality.** Redacting this module's own log
  sinks does not finish the job. The bare ``raise`` that closes every
  handler *deliberately* propagates the exception, so when this file
  runs as a script CPython's top-level handler renders that exception on
  ``stderr`` — its ``str()``, ``Traceback (most recent call last)``, and
  one absolute ``File "<path>", line <n>, in <function>`` frame per
  stack level — republishing on the process's own error stream exactly
  what :func:`_log_failed_run` withheld from the log sinks. The
  ``if __name__ == "__main__"`` block therefore dispatches through
  :func:`main`, which translates a propagated failure into a silent
  ``SystemExit(1)`` after a redacted last-resort record. See
  :func:`main` and :func:`_log_aborted_process` for the reproduced
  failing case and for what is intentionally *not* changed.

References
----------
* AAP §0.4.1.1 — CLI responsibility and integration wiring.
* AAP §0.4.5 — ``all`` dispatch order ``schedule → games → teams →
  players → lineups``.
* AAP §0.5.1.7 — Group 7 CLI subcommand inventory.
* AAP §0.7.2 — Operational rules 1–8.
* AAP §0.7.3.1 — Observability-rule diagnostic surface.
* README.md "Usage" section — public CLI contract.
"""
from __future__ import annotations

import json
import logging
import sys
from typing import Callable, Sequence

import click

import config
from api.nba_client import NBAClient
from pipelines import (
    ingest_games,
    ingest_lineups,
    ingest_players,
    ingest_schedule,
    ingest_teams,
)
from storage.csv_writer import CSVWriter
from utils import correlation, health, metrics
from utils import logger as logger_module
from utils.checkpoint import CheckpointManager
from utils.rate_limiter import RateLimiter


# ---------------------------------------------------------------------------
# Collaborator composition helper
# ---------------------------------------------------------------------------


def _build_collaborators(
    logger_name: str,
) -> tuple[NBAClient, CSVWriter, CheckpointManager, logging.LoggerAdapter]:
    """Compose the injectable collaborators for a single CLI subcommand.

    This helper is the canonical composition root for every *data*
    subcommand (``players``, ``teams``, ``games``, ``lineups``,
    ``schedule``, ``all``). It is invoked ONCE per subcommand entry and
    performs three responsibilities in this exact order:

    1. Mint a fresh correlation ID via
       :func:`utils.correlation.new_correlation_id` and bind it to the
       current execution context via
       :meth:`contextvars.ContextVar.set`. Every subsequent log record
       issued in this process — whether directly from ``run.py`` or
       transitively from any pipeline, endpoint, utility, or the HTTP
       client — will carry this ID through the
       :class:`utils.correlation.CorrelationAdapter` (Observability
       rule, AAP §0.7.3.1).

    2. Acquire a :class:`logging.LoggerAdapter` via
       :func:`utils.logger.get_logger` for the calling subcommand (e.g.
       ``"cli.players"``). This is the adapter that will be returned to
       the caller for subcommand-scoped logging.

    3. Instantiate the four collaborator objects using **constructor
       injection** (AAP §0.4.1.2):

         * ``RateLimiter()`` — default argument reads
           :data:`config.RATE_LIMIT_SECONDS` internally.
         * ``NBAClient(rate_limiter=..., logger=..., metrics=...)`` —
           keyword-only constructor; all three collaborators are
           injected explicitly.
         * ``CSVWriter(output_dir=config.OUTPUT_DIR)`` — resolves the
           writer's output directory at :data:`config.OUTPUT_DIR`
           (Gate 12 read-site).
         * ``CheckpointManager(path=config.CHECKPOINT_PATH)`` —
           resolves the checkpoint manifest at
           :data:`config.CHECKPOINT_PATH` (Gate 12 read-site).

    Parameters
    ----------
    logger_name : str
        The subcommand-scoped logger name, e.g. ``"cli.players"``,
        ``"cli.all"``. The returned adapter uses this name so log lines
        can be filtered by subcommand at query time.

    Returns
    -------
    tuple[NBAClient, CSVWriter, CheckpointManager, logging.LoggerAdapter]
        A 4-tuple ``(client, writer, checkpoint, logger_adapter)``
        ready for direct handoff to any pipeline's
        ``run(client, writer, checkpoint, season, ...)`` function.

    Notes
    -----
    The :class:`NBAClient` is always given the logger named
    ``"nba_client"`` — never the subcommand-scoped logger — so that HTTP
    transport events retain a stable logger name for filtering,
    independent of which subcommand triggered them. The subcommand's
    own logger (returned from this helper) carries the subcommand name.
    """
    # Step 1 — mint and bind the correlation ID. Every log record and
    # every outbound NBA Stats API request issued in the remainder of
    # this invocation will carry this ID via contextvars propagation.
    cid = correlation.new_correlation_id()
    correlation.correlation_id.set(cid)

    # Step 2 — acquire the subcommand-scoped logger adapter.
    adapter = logger_module.get_logger(logger_name)

    # Step 3 — compose the collaborators. Each constructor is explicit
    # about its Gate-12 read-site so static analysis can trace
    # config propagation from this file.
    rate_limiter = RateLimiter()
    client = NBAClient(
        rate_limiter=rate_limiter,
        logger=logger_module.get_logger("nba_client"),
        metrics=metrics.registry,
    )
    writer = CSVWriter(output_dir=config.OUTPUT_DIR)
    checkpoint = CheckpointManager(path=config.CHECKPOINT_PATH)

    return client, writer, checkpoint, adapter


# ---------------------------------------------------------------------------
# Sanitized failure-logging boundary (CD-3)
# ---------------------------------------------------------------------------


def _log_failed_run(
    log: logging.LoggerAdapter,
    subcommand: str,
    season: str,
) -> None:
    """Record a run failure WITHOUT disclosing the exception's detail.

    Called from the ``except Exception`` block of every data subcommand
    and of ``all``, immediately before the mandatory bare ``raise``. It
    must be invoked while an exception is being handled, because it reads
    the active exception from :func:`sys.exc_info`.

    Why this exists instead of ``log.exception``
    -------------------------------------------
    ``log.exception`` is ``log.error(..., exc_info=True)``, and
    :func:`utils.logger._configure` attaches BOTH a
    :class:`logging.StreamHandler` bound to ``sys.stdout`` AND a
    :class:`logging.handlers.RotatingFileHandler` writing
    :data:`config.LOG_FILE`, each at :data:`config.LOG_LEVEL`. Using it
    here therefore rendered the formatted traceback into **two normal
    operator channels at once** — the interactive/CI console and the
    durable log file — and with it:

    * the exception's ``str()``, which is arbitrary text this process does
      not control. An upstream HTTP error carries the request URL and its
      query string; an :exc:`OSError` carries a filesystem path; a pandas
      or normalizer error can carry cell values drawn straight from the
      payload. Any of those may hold a token, a signed URL, or data an
      operator's log pipeline is not entitled to retain.
    * ``Traceback (most recent call last)`` with one ``File "<absolute
      path>", line <n>, in <function>`` frame per stack level, which
      publishes the deployment's directory layout and exact source
      coordinates to anyone who can read the log.

    That is CWE-209 (information exposure through an error message) and
    CWE-532 (insertion of sensitive information into a log file).

    The failing case that motivated the change: a pipeline raising
    ``RuntimeError("upstream request failed: https://stats.nba.com/stats/"
    "leaguedashplayerstats?<token>")`` published that token, the
    traceback header, this file's absolute path and its line numbers to
    ``stdout`` *and* to ``logs/pipeline.log``. It is exercised by
    ``tests/unit/test_cli_failure_paths.py`` Section F, which fails
    against the unredacted boundary.

    What is emitted instead
    -----------------------
    * At **ERROR**, on the normal channels: the ``run.failed`` event, the
      subcommand, the season, and the exception's **class name**. The
      class name is a static identifier from this project's own source —
      never attacker- or data-controlled — and it is what makes the
      redacted record triage-able (``ConnectionError`` and
      ``PermissionError`` demand very different operator responses).
      ``detail=suppressed`` states plainly that more exists, so the record
      cannot be mistaken for the whole story.
    * At **DEBUG**, on the gated diagnostic channel: the full
      ``exc_info`` traceback. :data:`config.LOG_LEVEL` defaults to
      ``"INFO"`` and both handlers are level-filtered, so this record is
      discarded — never formatted, never written — unless an operator
      deliberately opts in with ``NBA_LOG_LEVEL=DEBUG`` and thereby
      accepts durable retention of the detail. Internal causality is
      preserved and gated, not destroyed.

    What is deliberately unchanged
    ------------------------------
    The caller's bare ``raise`` and the ``metrics.registry.inc`` call that
    precedes it. AAP §0.5.2.1 mandates that this boundary *log and
    re-raise*: the original exception object — traceback, ``__cause__``
    and attributes intact — must still reach the caller so Click exits
    non-zero and no failure is ever silently swallowed. Redaction applies
    to what this process *writes to its own log sinks*, not to what it
    propagates.

    What the propagated exception must NOT be allowed to do, however, is
    reach CPython's top-level handler, which would render its message and
    traceback on ``stderr`` and undo this redaction on a channel this
    helper does not govern. That is the complementary boundary, and it
    lives in :func:`main`: see its docstring for the reproduced failing
    case (defect CD-4).

    Parameters
    ----------
    log : logging.LoggerAdapter
        The subcommand-scoped adapter returned by
        :func:`_build_collaborators`; it carries the correlation ID that
        ties this record to the rest of the run, which is how an operator
        finds the matching DEBUG detail after re-running with
        ``NBA_LOG_LEVEL=DEBUG``.
    subcommand : str
        The Click subcommand name (``"players"``, ``"all"``, ...).
    season : str
        The ``--season`` value, echoed for symmetry with the
        ``run.start`` record. It is a CLI argument, not payload data.
    """
    # ``sys.exc_info()[1]`` rather than an ``except ... as exc`` binding:
    # it keeps each call site a single statement and reads the exception
    # the interpreter is currently handling, which is exactly the one the
    # DEBUG record's ``exc_info=True`` will render.
    active = sys.exc_info()[1]
    error_type = type(active).__name__ if active is not None else "unknown"

    log.error(
        "run.failed subcommand=%s season=%s error_type=%s detail=suppressed "
        "(re-run with NBA_LOG_LEVEL=DEBUG for the traceback)",
        subcommand,
        season,
        error_type,
    )
    log.debug(
        "run.failed.detail subcommand=%s season=%s",
        subcommand,
        season,
        exc_info=True,
    )


# ---------------------------------------------------------------------------
# Click group
# ---------------------------------------------------------------------------


@click.group(
    help=(
        "NBA Data Ingestion Pipeline — pulls, normalizes, and writes CSVs "
        "for players, teams, games, lineups, and schedule."
    ),
)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Root Click group for the NBA Data Ingestion Pipeline CLI.

    The group itself takes no options; ``--season`` is declared
    per-subcommand so that the surface remains minimal and so
    ``--help`` on any subcommand clearly documents the option.

    ``ctx.ensure_object(dict)`` creates a context-scoped dictionary
    that subcommands can use to stash references without resorting to
    module globals. The current subcommands do not require this
    (collaborators are composed locally via :func:`_build_collaborators`
    and passed directly into the pipeline ``run()`` calls), but the
    ensured object is a small, well-known Click idiom that keeps the
    door open for future shared state (e.g., a ``--verbose`` flag).
    """
    ctx.ensure_object(dict)


# ---------------------------------------------------------------------------
# Domain subcommands — one per data domain, plus the aggregate ``all``.
#
# Every domain subcommand follows the identical five-step pattern:
#   1. Compose collaborators via _build_collaborators().
#   2. Emit a structured "run.start" INFO log with the subcommand name
#      and season.
#   3. Invoke the pipeline's run() with keyword arguments
#      client/writer/checkpoint/season.
#   4. On success: increment
#      pipeline_runs_total{pipeline=ingest_<domain>,outcome=success} and
#      emit "run.complete" INFO.
#   5. On failure: increment
#      pipeline_runs_total{pipeline=ingest_<domain>,outcome=error},
#      emit a REDACTED "run.failed" ERROR via _log_failed_run (exception
#      class name only — never its message or traceback, which go to the
#      DEBUG-gated diagnostic channel), and re-raise so Click exits with
#      a non-zero status.
#
# The ``pipeline`` / ``outcome`` labels with values ``ingest_<domain>`` /
# ``success|error`` are the binding contract documented in
# ``docs/OBSERVABILITY.md`` (metrics catalog) and queried by the
# ``operator_dashboard`` (``sum by (outcome) (...)`` chart and the
# ``PipelineErrorOutcome`` alert rule). The ``all`` aggregate subcommand
# uses ``pipeline="all"`` as a special marker distinguishing
# whole-run outcomes from per-pipeline outcomes.
#
# The try/except Exception boundary here is *explicitly sanctioned* by
# AAP §0.5.2.1: it logs-and-reraises, never silently swallows. This is
# distinct from Rule 6's try/except in ``pipelines/ingest_games.py``,
# which is domain-specific and isolates per-game failures.
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--season",
    default=config.DEFAULT_SEASON,
    show_default=True,
    help="Season string, e.g. '2025-26'.",
)
def players(season: str) -> None:
    """Run the Players pipeline (F-009)."""
    client, writer, checkpoint, log = _build_collaborators("cli.players")
    log.info("run.start subcommand=players season=%s", season)
    try:
        ingest_players.run(
            client=client,
            writer=writer,
            checkpoint=checkpoint,
            season=season,
        )
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_players", "outcome": "success"},
        )
        log.info("run.complete subcommand=players season=%s", season)
    except Exception:
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_players", "outcome": "error"},
        )
        _log_failed_run(log, "players", season)
        raise


@cli.command()
@click.option(
    "--season",
    default=config.DEFAULT_SEASON,
    show_default=True,
    help="Season string, e.g. '2025-26'.",
)
def teams(season: str) -> None:
    """Run the Teams pipeline (F-010)."""
    client, writer, checkpoint, log = _build_collaborators("cli.teams")
    log.info("run.start subcommand=teams season=%s", season)
    try:
        ingest_teams.run(
            client=client,
            writer=writer,
            checkpoint=checkpoint,
            season=season,
        )
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_teams", "outcome": "success"},
        )
        log.info("run.complete subcommand=teams season=%s", season)
    except Exception:
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_teams", "outcome": "error"},
        )
        _log_failed_run(log, "teams", season)
        raise


@cli.command()
@click.option(
    "--season",
    default=config.DEFAULT_SEASON,
    show_default=True,
    help="Season string, e.g. '2025-26'.",
)
def games(season: str) -> None:
    """Run the Games pipeline (F-011).

    Note: AAP §0.4.5 specifies that isolated ``games`` invocations
    re-enumerate ``GAME_IDs`` on demand inside
    ``pipelines.ingest_games.run`` via
    ``endpoints.schedule.enumerate_game_ids``. This CLI subcommand
    therefore does NOT auto-invoke the Schedule pipeline — it calls
    ``ingest_games.run(...)`` directly, and the pipeline handles its
    own ``GAME_ID`` enumeration.
    """
    client, writer, checkpoint, log = _build_collaborators("cli.games")
    log.info("run.start subcommand=games season=%s", season)
    try:
        ingest_games.run(
            client=client,
            writer=writer,
            checkpoint=checkpoint,
            season=season,
        )
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_games", "outcome": "success"},
        )
        log.info("run.complete subcommand=games season=%s", season)
    except Exception:
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_games", "outcome": "error"},
        )
        _log_failed_run(log, "games", season)
        raise


@cli.command()
@click.option(
    "--season",
    default=config.DEFAULT_SEASON,
    show_default=True,
    help="Season string, e.g. '2025-26'.",
)
def lineups(season: str) -> None:
    """Run the Lineups pipeline (F-012)."""
    client, writer, checkpoint, log = _build_collaborators("cli.lineups")
    log.info("run.start subcommand=lineups season=%s", season)
    try:
        ingest_lineups.run(
            client=client,
            writer=writer,
            checkpoint=checkpoint,
            season=season,
        )
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_lineups", "outcome": "success"},
        )
        log.info("run.complete subcommand=lineups season=%s", season)
    except Exception:
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_lineups", "outcome": "error"},
        )
        _log_failed_run(log, "lineups", season)
        raise


@cli.command()
@click.option(
    "--season",
    default=config.DEFAULT_SEASON,
    show_default=True,
    help="Season string, e.g. '2025-26'.",
)
def schedule(season: str) -> None:
    """Run the Schedule pipeline (F-013)."""
    client, writer, checkpoint, log = _build_collaborators("cli.schedule")
    log.info("run.start subcommand=schedule season=%s", season)
    try:
        ingest_schedule.run(
            client=client,
            writer=writer,
            checkpoint=checkpoint,
            season=season,
        )
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_schedule", "outcome": "success"},
        )
        log.info("run.complete subcommand=schedule season=%s", season)
    except Exception:
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "ingest_schedule", "outcome": "error"},
        )
        _log_failed_run(log, "schedule", season)
        raise


# ---------------------------------------------------------------------------
# Aggregate ``all`` subcommand
#
# The ordering schedule → games → teams → players → lineups is BINDING
# per AAP §0.4.5. See docs/DECISIONS.md D-008 for the full rationale and
# dependency analysis.
# ---------------------------------------------------------------------------


@cli.command("all")
@click.option(
    "--season",
    default=config.DEFAULT_SEASON,
    show_default=True,
    help="Season string, e.g. '2025-26'.",
)
def all_cmd(season: str) -> None:
    """Run every pipeline in dependency order: schedule, games, teams, players, lineups."""
    client, writer, checkpoint, log = _build_collaborators("cli.all")
    log.info("run.start subcommand=all season=%s", season)

    # The ordered dispatch table is the authoritative expression of
    # AAP §0.4.5. Any future reordering requires a corresponding AAP
    # amendment AND an update to ``docs/TRACEABILITY.md``.
    order: list[tuple[str, Callable]] = [
        ("schedule", ingest_schedule.run),
        ("games", ingest_games.run),
        ("teams", ingest_teams.run),
        ("players", ingest_players.run),
        ("lineups", ingest_lineups.run),
    ]

    try:
        for name, runner in order:
            log.info("pipeline.start name=%s", name)
            runner(
                client=client,
                writer=writer,
                checkpoint=checkpoint,
                season=season,
            )
            log.info("pipeline.complete name=%s", name)
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "all", "outcome": "success"},
        )
        log.info("run.complete subcommand=all season=%s", season)
    except Exception:
        metrics.registry.inc(
            "pipeline_runs_total",
            {"pipeline": "all", "outcome": "error"},
        )
        _log_failed_run(log, "all", season)
        raise


# ---------------------------------------------------------------------------
# Diagnostic subcommands (Observability rule — AAP §0.7.3.1).
#
# These subcommands are stateless — they do NOT mint a correlation ID
# and do NOT call ``_build_collaborators`` because:
#   * ``health`` and ``ready`` inspect process and filesystem state,
#     never issuing HTTP traffic.
#   * ``metrics`` renders the shared in-process registry; minting a
#     correlation ID would add noise without any corresponding log line.
# Keeping them stateless also means operators can run them against a
# fresh install (before any pipeline has populated ``output/`` or
# ``logs/``) without side effects.
# ---------------------------------------------------------------------------


@cli.command("health")
def health_cmd() -> None:
    """Print the liveness probe as JSON and exit 0.

    ``utils.health.check_health`` never raises and always reports
    ``status="ok"`` — its purpose is to prove the interpreter can
    execute code in this process. The output is emitted via
    :func:`click.echo` so it is captured cleanly by
    :class:`click.testing.CliRunner` in Gate-13 tests.

    This callback is registered under the Click name ``"health"`` (via
    the explicit argument to :func:`click.command`) while the Python
    symbol is :func:`health_cmd` to avoid shadowing the imported
    :mod:`utils.health` module within this file.
    """
    result = health.check_health()
    click.echo(json.dumps(result, indent=2))


@cli.command("ready")
def ready_cmd() -> None:
    """Print the readiness probe as JSON and exit 0/1.

    ``utils.health.check_readiness`` aggregates four sub-probes
    (output-dir writability, required headers presence, rate-limit
    floor, checkpoint parseability). The CLI surface translates the
    aggregate ``status`` field into the process exit code:

    * ``status="ready"``  → exit 0.
    * ``status="not_ready"`` → exit 1 so orchestrators (systemd,
      docker healthcheck, shell pipelines) can detect the failure.

    The callback is explicitly named :func:`ready_cmd` and registered
    under the Click name ``"ready"`` to keep the naming convention
    consistent with the other ``*_cmd`` diagnostic subcommands.
    """
    result = health.check_readiness()
    click.echo(json.dumps(result, indent=2))
    if result["status"] != "ready":
        sys.exit(1)


@cli.command("metrics")
def metrics_cmd() -> None:
    """Print the Prometheus-text-format exposition of the metrics registry.

    This subcommand is registered under the Click name ``"metrics"``
    (via the explicit argument to :func:`click.command`) while the
    Python callback is named :func:`metrics_cmd` to avoid shadowing the
    imported :mod:`utils.metrics` module within this file.

    The rendered output conforms to Prometheus text-format 0.0.4 and
    includes ``# HELP`` / ``# TYPE`` preambles plus sample lines for
    every registered counter and histogram. When the registry has not
    yet recorded any observations (fresh process), the exposition may
    consist solely of the header lines or be empty — both are valid.
    """
    click.echo(metrics.registry.render_prometheus(), nl=False)


# ---------------------------------------------------------------------------
# Module entry point and its process-level failure boundary (CD-4)
# ---------------------------------------------------------------------------


#: Process exit status reported when a subcommand fails. It is the status
#: Click already produces for an escaped callback exception and the
#: literal :func:`ready_cmd` hands to :func:`sys.exit`, so translating a
#: propagated failure into it preserves the exit contract operators,
#: orchestrators and CI pipelines already rely on.
_FAILURE_EXIT_STATUS: int = 1

#: Logger name for records emitted by the process boundary itself. It is
#: the parent of the ``"cli.<subcommand>"`` names :func:`_build_collaborators`
#: uses, so a boundary record sorts alongside the subcommand records of the
#: same run and inherits the correlation ID already bound to this context.
_PROCESS_LOGGER_NAME: str = "cli"


def _log_aborted_process() -> None:
    """Record that the process is aborting, WITHOUT disclosing the exception.

    Called from :func:`main`'s ``except`` block while the exception is
    still being handled, because it reads the active exception from
    :func:`sys.exc_info`.

    Why a second record exists alongside :func:`_log_failed_run`
    -----------------------------------------------------------
    The two cover different failure populations, and only their union
    covers every way this process can die non-zero:

    * :func:`_log_failed_run` runs inside a subcommand's ``except``
      block, so it sees only failures raised *within* the ``try`` that
      wraps a pipeline call. Its record names the subcommand and season.
    * this helper runs at the outermost frame, so it also sees failures
      raised *before* a subcommand's ``try`` is entered — for example an
      :exc:`OSError` from ``CSVWriter(output_dir=...)`` inside
      :func:`_build_collaborators`, or a :exc:`KeyError` from a
      diagnostic subcommand. For those, this is the ONLY record written,
      which is what keeps :func:`main`'s silent exit from becoming a
      silent *failure*: the process never dies without saying so.

    The event names are deliberately distinct (``run.aborted`` versus
    ``run.failed``) so log-based alerting can tell "a pipeline raised"
    apart from "the process gave up", and so neither record can be
    mistaken for the other by a text filter.

    What is emitted, and where
    --------------------------
    Exactly the redaction contract :func:`_log_failed_run` establishes,
    for the same CWE-209 / CWE-532 reasons:

    * At **ERROR**, on the normal channels: the ``run.aborted`` event,
      the exit status, the exception's **class name** — a static
      identifier from this project's own source, never attacker- or
      payload-controlled — and ``detail=suppressed`` so the record cannot
      be mistaken for the whole story.
    * At **DEBUG**, on the gated diagnostic channel: the full
      ``exc_info``. :data:`config.LOG_LEVEL` defaults to ``"INFO"`` and
      :func:`utils.logger._configure` applies it to the root logger AND
      to both handlers, so this record is discarded — never formatted,
      never written — unless an operator opts in with
      ``NBA_LOG_LEVEL=DEBUG``.
    """
    # ``sys.exc_info()[1]`` rather than an ``except ... as exc`` binding,
    # mirroring ``_log_failed_run``: it keeps the call site a single
    # statement and reads the exception the interpreter is currently
    # handling, which is exactly the one ``exc_info=True`` will render.
    active = sys.exc_info()[1]
    error_type = type(active).__name__ if active is not None else "unknown"

    log = logger_module.get_logger(_PROCESS_LOGGER_NAME)
    log.error(
        "run.aborted exit_status=%s error_type=%s detail=suppressed "
        "(re-run with NBA_LOG_LEVEL=DEBUG for the traceback)",
        _FAILURE_EXIT_STATUS,
        error_type,
    )
    log.debug("run.aborted.detail", exc_info=True)


def main(argv: Sequence[str] | None = None) -> None:
    """Run the CLI as a process, translating a failure into a silent exit 1.

    This is the function the ``if __name__ == "__main__"`` block below
    invokes, and therefore the real behaviour of ``python run.py
    <subcommand>``. It is a module-level function rather than inline code
    so the process boundary is reachable from a test
    (``run.main(["teams", "--season", ...])``) instead of only from a
    shell.

    The defect this exists to fix (CD-4), with its failing case
    ----------------------------------------------------------
    * **Behaviour before the fix.** The entry point called ``cli()``
      directly. Every subcommand's ``except`` block ends with a bare
      ``raise`` (AAP §0.5.2.1 mandates log-and-re-raise), Click does not
      catch non-Click exceptions, so the exception reached CPython's
      top-level handler and was rendered to ``stderr`` in full. Running
      a ``teams`` pipeline that raised
      ``RuntimeError("upstream request failed: https://stats.nba.com/"
      "stats/leaguedashplayerstats?<token>")`` exited 1 while printing
      the token, ``Traceback (most recent call last)``, and eight
      ``File "<absolute path>", line <n>, in <function>`` frames —
      including this file's absolute path and line number — onto the
      process's error stream. :func:`_log_failed_run` had already
      withheld all of that from the log sinks, so the disclosure was
      republished on the one channel it did not govern: CWE-209
      (information exposure through an error message), and CWE-532 once
      a CI or cron wrapper retains ``stderr``.
    * **Behaviour after the fix.** ``stderr`` carries nothing at all. The
      operator learns the run failed from the redacted ``run.failed``
      and ``run.aborted`` records, and the process still exits
      :data:`_FAILURE_EXIT_STATUS`.
    * **The change.** This function, :func:`_log_aborted_process`, and
      one changed statement in the ``__main__`` block. Every subcommand
      callback — its bare ``raise``, its ``metrics.registry.inc`` calls,
      its logging — is byte-identical, which is why in-process callers
      such as :class:`click.testing.CliRunner` see exactly the behaviour
      they saw before.

    What is deliberately NOT caught
    -------------------------------
    ``except Exception`` never catches :exc:`SystemExit` or
    :exc:`KeyboardInterrupt`, both of which derive from
    :exc:`BaseException`. Consequently:

    * a successful run still exits 0 (Click's standalone mode ends in
      ``ctx.exit()``),
    * ``ready``'s deliberate ``sys.exit(1)`` still exits 1 with its JSON
      body already echoed,
    * a Click usage error still exits 2 with Click's own
      operator-facing message on ``stderr`` — that message is generated
      by this project, not derived from payload or upstream data, so
      suppressing it would destroy usability without any confidentiality
      gain,
    * ``Ctrl-C`` still aborts the way Click documents.

    Only a genuine, already-logged failure is converted, and it is
    converted with ``from None`` so the interpreter cannot render the
    original exception as the new one's context either.

    Parameters
    ----------
    argv : Sequence[str], optional
        Argument vector *excluding* the program name, handed straight to
        :meth:`click.Group.main`. ``None`` — the value the ``__main__``
        block uses — makes Click read ``sys.argv[1:]``, which is the
        production path. A test supplies an explicit list to drive one
        subcommand without touching ``sys.argv``.

    Raises
    ------
    SystemExit
        Always: with :data:`_FAILURE_EXIT_STATUS` when a subcommand let
        an exception escape, and otherwise with whatever status Click or
        a callback chose (0 on success, 1 from ``ready``, 2 for a usage
        error).
    """
    try:
        cli.main(args=argv, standalone_mode=True)
    except Exception:
        # ``try``/``finally`` rather than a nested ``except``: whatever
        # happens while writing the last-resort record — a full disk, a
        # revoked log directory — this process must still exit
        # ``_FAILURE_EXIT_STATUS`` with nothing rendered. A ``raise``
        # inside ``finally`` replaces the in-flight exception, and
        # ``from None`` suppresses its context, so neither the original
        # failure nor a logging failure can reach the traceback
        # renderer. The status is never silently downgraded to 0.
        try:
            _log_aborted_process()
        finally:
            raise SystemExit(_FAILURE_EXIT_STATUS) from None


if __name__ == "__main__":
    main()
