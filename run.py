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
from typing import Callable

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
#      emit "run.failed" ERROR with exception info, and re-raise so
#      Click exits with a non-zero status.
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
        log.exception("run.failed subcommand=players season=%s", season)
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
        log.exception("run.failed subcommand=teams season=%s", season)
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
        log.exception("run.failed subcommand=games season=%s", season)
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
        log.exception("run.failed subcommand=lineups season=%s", season)
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
        log.exception("run.failed subcommand=schedule season=%s", season)
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
        log.exception("run.failed subcommand=all season=%s", season)
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
# Module entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    cli()
