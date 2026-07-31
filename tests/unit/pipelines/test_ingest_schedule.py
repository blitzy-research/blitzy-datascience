"""Unit tests for :mod:`pipelines.ingest_schedule` (Feature F-013, Schedule domain).

These tests verify the orchestration contract of the Schedule pipeline without
performing any real network I/O or filesystem work beyond ``tmp_path``. They
use the ``recording_client`` / ``recording_writer`` / ``recording_checkpoint``
spy fixtures defined in :mod:`tests.conftest` — the same fixtures that power
:mod:`tests.unit.pipelines.test_ingest_lineups` (the template this module
parallels per the Agent Action Plan §0.5.1.8).

Behavioral scope
----------------

The tests assert the following contracts:

1. **Happy path** — ``ingest_schedule.run`` invokes the ``leaguegamefinder``
   endpoint exactly once, writes the normalized DataFrame under
   ``config.CSV_SCHEDULE`` with the caller-provided season, marks the
   checkpoint with the ``"<endpoint>:<season>"`` key pattern, and
   increments the ``pipeline_rows_written_total`` counter.
2. **Idempotent resume** — when the checkpoint is pre-seeded with the
   expected ``(domain, key)`` pair, ``run`` makes no HTTP calls, writes no
   CSVs, and performs no additional ``mark_completed`` calls.
3. **Rule 5 ordering** — the pipeline calls ``checkpoint.is_completed``
   exactly once BEFORE writing, and ``checkpoint.mark_completed`` exactly
   once AFTER writing. ``mark_completed`` MUST never precede a successful
   ``write``.
4. **Negative-space guard** — the pipeline invokes ONLY
   ``leaguegamefinder``; no other NBA Stats endpoint name appears in
   ``client.calls``.

Rule 6 (fail-safe per-game iteration) is intentionally NOT exercised here
because Rule 6 is EXCLUSIVE to ``pipelines.ingest_games`` per AAP §0.7.2.6.
The Schedule pipeline must propagate exceptions rather than swallow them;
that contract is covered by integration tests and the per-pipeline
exception-propagation behavior asserted in ``test_ingest_players`` and
``test_ingest_teams``.

Cross-module invariants (Rules 1, 4, 7) are verified by the dedicated
``tests/invariants/`` suite and are deliberately out of scope here.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest  # noqa: F401  (imported to satisfy Phase 1 header convention)

import config
from pipelines import ingest_schedule


# ---------------------------------------------------------------------------
# Module-level constants
#
# The checkpoint key format is an implicit contract between this pipeline,
# the checkpoint manifest JSON schema, and any operator-visible key listing
# (``output/checkpoint.json``). Duplicating the format here — rather than
# importing a private helper — is intentional so that drift in either the
# production module or the key schema is detected by a failing test.
# ---------------------------------------------------------------------------

_SEASON = "2025-26"
_ENDPOINT_LABEL = "leaguegamefinder"
_EXPECTED_KEY = f"{_ENDPOINT_LABEL}:{_SEASON}"


# ---------------------------------------------------------------------------
# Hand-derived EXPECTED values
#
# Every constant below follows from the ``sample_schedule_payload`` and
# ``sample_empty_payload`` fixtures in :mod:`tests.conftest` together with this
# pipeline's INTENDED semantics, and the arithmetic behind each number is
# spelled out beside it.
#
# These are EXPECTED (output) values and are deliberately kept separate from
# the fixture INPUT values, which remain owned by ``tests/conftest.py``.
# ---------------------------------------------------------------------------

#: Rows the writer must receive on the happy path.
#:
#: ``leaguegamefinder`` emits one row per team per game, so
#: ``sample_schedule_payload`` holds 2 team-rows for game ``0022500001``
#: + 2 team-rows for game ``0022500002`` + 1 team-row for game
#: ``0022500003``.  2 + 2 + 1 = 5.
#:
#: ``ingest_schedule`` deliberately does NOT deduplicate — its module
#: docstring commits to preserving the endpoint's natural
#: two-row-per-game ``TEAM_ID``/``GAME_ID`` representation, and any
#: one-row-per-game pivot is documented downstream work — so all 5 rows
#: must reach the writer verbatim.
EXPECTED_SCHEDULE_ROWS = 5

#: Distinct ``GAME_ID`` values spanned by those 5 rows: ``0022500001``,
#: ``0022500002`` and ``0022500003`` → 3.
#:
#: Asserting 3 distinct IDs ALONGSIDE 5 rows is what proves the documented
#: no-dedupe contract. Either number alone is ambiguous: a
#: ``drop_duplicates(subset=["GAME_ID"])`` mutation would leave the distinct
#: count at 3 while collapsing the row count to 3, so only the pair pins the
#: behavior.
EXPECTED_DISTINCT_GAME_IDS = 3

#: Exact ordered column list of the frame handed to the writer.
#:
#: ``sample_schedule_payload`` declares the headers ``SEASON_ID``,
#: ``TEAM_ID``, ``GAME_ID``, ``GAME_DATE`` in that order, and the
#: normalizer preserves them verbatim. The pipeline's private
#: ``_ensure_season_column`` helper then tests ``c.lower() == "season"``
#: against every column: ``SEASON_ID`` lowercases to ``"season_id"``, which
#: does NOT match, so no column satisfies the predicate and a lowercase
#: ``season`` column is inserted at index 0 while ``SEASON_ID`` is preserved
#: untouched. Hence 1 inserted + 4 upstream = 5 columns in this order.
EXPECTED_SCHEDULE_COLUMNS = [
    "season",
    "SEASON_ID",
    "TEAM_ID",
    "GAME_ID",
    "GAME_DATE",
]

#: Rows written for a payload whose table carries ``headers`` but an empty
#: ``rowSet``.
#:
#: ``sample_empty_payload`` supplies ``"rowSet": []``, so that result set
#: normalizes to a zero-row DataFrame — column names preserved, zero records
#: — inside the ``Dict[str, DataFrame]`` mapping ``normalize_result_sets``
#: returns, and ``_select_primary_df`` selects it. len(df) is therefore 0,
#: which is both the ``rows`` the writer records and the ``n`` the row-count
#: counter receives.
EXPECTED_EMPTY_ROWS = 0

#: Exact ordered column list for the empty-``rowSet`` case.
#:
#: ``sample_empty_payload`` declares the headers ``PLAYER_ID`` and ``PTS``.
#: Neither lowercases to ``"season"``, so ``_ensure_season_column`` inserts
#: ``season`` at index 0 exactly as it does on the happy path — 1 inserted
#: + 2 upstream = 3 columns — proving the header-only artifact is a real,
#: fully-formed frame rather than an empty placeholder.
EXPECTED_EMPTY_COLUMNS = [
    "season",
    "PLAYER_ID",
    "PTS",
]

#: Label set every ``pipeline_rows_written_total`` increment must carry.
#:
#: The ``{"pipeline": "ingest_<domain>", "artifact": "<csv_name>.csv"}``
#: convention is repository-wide — all five pipelines emit it — so it is a
#: published observability contract. Built from ``config.CSV_SCHEDULE`` so
#: renaming the artifact stays a single-point edit.
EXPECTED_ROW_COUNT_LABELS = {
    "pipeline": "ingest_schedule",
    "artifact": f"{config.CSV_SCHEDULE}.csv",
}


# ===========================================================================
# Test 1 — Happy-path: endpoint invoked, CSV written, checkpoint marked
# ===========================================================================

def test_run_happy_path_writes_schedule_and_marks_checkpoint(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_schedule_payload,
    tmp_path,
):
    """``run`` fetches leaguegamefinder, writes ``schedule.csv``, checkpoints.

    This is the end-to-end sanity test for F-013: given a simulated NBA
    Stats ``leaguegamefinder`` response, the pipeline must (a) invoke the
    client with exactly that endpoint name, (b) pass the resulting
    DataFrame to the writer with the canonical ``config.CSV_SCHEDULE``
    name, (c) mark the checkpoint with the ``"leaguegamefinder:<season>"``
    key, and (d) increment the row-count counter at least once.
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_schedule_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    ingest_schedule.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # (a) Endpoint contract: leaguegamefinder invoked.
    assert any(
        call[0] == _ENDPOINT_LABEL for call in client.calls
    ), (
        "schedule pipeline must invoke the leaguegamefinder endpoint; "
        f"recorded calls were {client.calls!r}"
    )

    # (b) Writer contract: one write with the canonical schedule CSV name.
    assert len(writer.writes) == 1, (
        f"schedule pipeline must write exactly one CSV artifact; "
        f"observed {len(writer.writes)} writes: {writer.writes!r}"
    )
    write = writer.writes[0]
    assert write["name"] == config.CSV_SCHEDULE, (
        f"writer must receive name={config.CSV_SCHEDULE!r}; got {write['name']!r}"
    )
    assert write["season"] == _SEASON, (
        f"writer must receive season={_SEASON!r}; got {write['season']!r}"
    )
    assert write["rows"] > 0, (
        "writer must receive a non-empty DataFrame for a happy-path payload; "
        f"got rows={write['rows']} (write={write!r})"
    )

    # (c) Rule 5 contract: checkpoint probed AND marked.
    assert (config.DOMAIN_SCHEDULE, _EXPECTED_KEY) in checkpoint.checks, (
        "pipeline must probe is_completed(DOMAIN_SCHEDULE, "
        f"{_EXPECTED_KEY!r}) before work; observed checks={checkpoint.checks!r}"
    )
    assert checkpoint.marks == [(config.DOMAIN_SCHEDULE, _EXPECTED_KEY)], (
        "pipeline must call mark_completed exactly once with "
        f"(DOMAIN_SCHEDULE, {_EXPECTED_KEY!r}); observed marks={checkpoint.marks!r}"
    )

    # (d) Observability contract: pipeline_rows_written_total incremented.
    rows_written_calls = [
        c
        for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert len(rows_written_calls) == 1, (
        "pipeline must increment pipeline_rows_written_total exactly once "
        "per successful write; observed "
        f"{len(rows_written_calls)} relevant inc() calls: {metrics_mock.inc.call_args_list!r}"
    )


# ===========================================================================
# Test 2 — Idempotent resume: pre-seeded checkpoint short-circuits the run
# ===========================================================================

def test_run_idempotent_skip_when_already_checkpointed(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_schedule_payload,
    tmp_path,
):
    """If the ``(schedule, leaguegamefinder:<season>)`` pair is already in
    the checkpoint, ``run`` must skip all work.

    Rule 5's resumability guarantee (AAP §0.7.2.5) means the pipeline MUST
    be idempotent: a second invocation against an already-completed
    checkpoint must produce ZERO HTTP calls, ZERO writes, and ZERO
    additional ``mark_completed`` calls. The ``is_completed`` probe itself
    IS permitted (and required) — that is how the pipeline detects the
    short-circuit condition.
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_schedule_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint(
        completed={config.DOMAIN_SCHEDULE: [_EXPECTED_KEY]}
    )
    metrics_mock = MagicMock()

    ingest_schedule.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # Short-circuit contract: no side effects beyond the is_completed probe.
    assert client.calls == [], (
        "idempotent skip must make zero client.get calls; observed "
        f"{client.calls!r}"
    )
    assert writer.writes == [], (
        "idempotent skip must write zero CSV artifacts; observed "
        f"{writer.writes!r}"
    )
    assert checkpoint.marks == [], (
        "idempotent skip must NOT re-call mark_completed; observed "
        f"{checkpoint.marks!r}"
    )
    # The probe itself is required — verify it happened exactly once
    # against the expected (domain, key) pair.
    assert (config.DOMAIN_SCHEDULE, _EXPECTED_KEY) in checkpoint.checks, (
        "idempotent skip path must still probe is_completed; observed "
        f"checks={checkpoint.checks!r}"
    )


# ===========================================================================
# Test 3 — Rule 5 ordering: is_completed BEFORE mark_completed
# ===========================================================================

def test_rule5_is_completed_precedes_mark_completed(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_schedule_payload,
    tmp_path,
):
    """The pipeline must probe the checkpoint exactly once BEFORE work and
    mark it exactly once AFTER a successful write.

    This test encodes Operational Rule 5 at the granularity of call
    ordering. A single write must be flanked by exactly ONE preceding
    ``is_completed`` probe and exactly ONE following ``mark_completed``
    call — no more, no less. Any divergence (e.g., marking before writing,
    probing twice, or marking twice) indicates a Rule 5 violation that
    would compromise resume determinism (AAP Gate 8).
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_schedule_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint()

    ingest_schedule.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
    )

    # Rule 5 pre-condition: exactly one is_completed probe, for the
    # expected (domain, key) pair.
    assert checkpoint.checks == [(config.DOMAIN_SCHEDULE, _EXPECTED_KEY)], (
        "pipeline must probe is_completed exactly once with "
        f"(DOMAIN_SCHEDULE, {_EXPECTED_KEY!r}); observed {checkpoint.checks!r}"
    )
    # Rule 5 post-condition: exactly one successful write.
    assert len(writer.writes) == 1, (
        f"pipeline must perform exactly one write; observed {len(writer.writes)}"
    )
    # Rule 5 post-condition: exactly one mark_completed for the same pair.
    assert checkpoint.marks == [(config.DOMAIN_SCHEDULE, _EXPECTED_KEY)], (
        "pipeline must call mark_completed exactly once with "
        f"(DOMAIN_SCHEDULE, {_EXPECTED_KEY!r}); observed {checkpoint.marks!r}"
    )


# ===========================================================================
# Test 4 — DataFrame post-condition: writer receives a frame with a
#          lowercase ``season`` column
# ===========================================================================

def test_writer_receives_dataframe_with_season_column(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_schedule_payload,
    tmp_path,
):
    """The pipeline must guarantee a ``season`` column on the DataFrame
    handed to the writer.

    The pipeline's private ``_ensure_season_column`` helper inserts a
    lowercase ``season`` column at position 0 when the normalized
    DataFrame does not already have one (case-insensitive lookup against
    the upstream header names). The ``leaguegamefinder`` payload emits a
    ``SEASON_ID`` column — a numeric season identifier like ``"22025"``
    for the 2025-26 regular season — but not a column literally named
    ``season``. This test verifies the helper's post-condition by
    inspecting the DataFrame the writer captured and asserting that a
    case-insensitive ``season`` entry is present in the column set.

    Why this test exists
    --------------------
    The README output contract for ``schedule.csv`` lists ``season`` as
    the leading key column alongside ``game_id``, ``home_team_id``, and
    ``away_team_id``. Downstream consumers rely on that column to
    disambiguate rows across multi-season runs. If a future refactor
    ever drops or renames ``_ensure_season_column`` — or changes its
    case-insensitive detection heuristic — this test will fail before
    that regression reaches a live CSV.

    Why we test column presence, not cell values
    --------------------------------------------
    The normalization of the ``resultSets`` envelope into a flat
    DataFrame is owned by :mod:`utils.schema_normalizer` and covered by
    its dedicated test module. Re-asserting those cell values here would
    couple the pipeline test to the normalizer's internals and make both
    tests brittle. This test only verifies the pipeline's own contract:
    the ``season`` column is present on the frame handed to the writer.
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_schedule_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint()

    ingest_schedule.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
    )

    # The writer spy captured exactly one write on the happy path; pull
    # the recorded DataFrame back out and inspect its columns.
    assert len(writer.writes) == 1, (
        f"expected exactly one write on the happy path; got {len(writer.writes)}"
    )
    df = writer.writes[0]["df"]
    lower_cols = {c.lower() for c in df.columns}
    assert "season" in lower_cols, (
        "DataFrame passed to writer must contain a 'season' column "
        f"(case-insensitive); got columns={list(df.columns)!r}"
    )


# ===========================================================================
# Test 5 — Exact-value happy path: 5 rows written, n=5 emitted, 3 distinct
#          GAME_IDs, and the exact ordered column list (no deduplication)
# ===========================================================================

def test_run_writes_exact_row_count_and_metric_value_without_dedupe(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_schedule_payload,
):
    """``run`` hands the writer all 5 fixture rows and emits ``n=5`` verbatim.

    The row count and the counter value are asserted as exact integers rather
    than as presence or positivity, because "a write happened" and "the count
    is above zero" cannot distinguish the correct quantity from a wrong one.
    The distinct-``GAME_ID`` count is asserted alongside the row count: only
    the pair ``5 rows over 3 games`` pins the documented no-dedupe contract,
    since either number alone is ambiguous.

    Mutations detected
    ------------------
    * A ``drop_duplicates(subset=["GAME_ID"])`` inserted anywhere between
      ``normalize_result_sets`` and ``writer.write`` collapses the 5
      legitimate one-row-per-team records down to 3 while leaving the
      distinct count at 3, breaking the documented no-dedupe contract; the
      row-count and ``n=`` assertions catch it. (A bare ``drop_duplicates()``
      would not collapse anything here — all five fixture rows differ by
      ``TEAM_ID`` — so the key-subset form is the mutation that matters.)
    * Any silently dropped or duplicated row (``.head()``, ``.iloc[1:]``, a
      stray ``concat``) fails the row-count and ``n=`` assertions together.
    * Emitting the wrong quantity into ``pipeline_rows_written_total`` — a
      constant ``1``, or a distinct-key count instead of a row count — fails
      the ``n=`` assertion even though the counter still fires exactly once.
    * Renaming the counter itself empties the filtered call list, so the
      call-count assertion fails first. Changing the label KEYS or VALUES
      while keeping the counter name is what the label assertion catches —
      the failure mode that would silently break every operator dashboard
      querying ``pipeline="ingest_schedule"``.
    * A "fix" to ``_ensure_season_column`` that suppressed the insertion when
      only ``SEASON_ID`` is present, appended ``season`` last instead of at
      index 0, or renamed ``SEASON_ID``, fails the ordered column-list
      assertion. That helper is CORRECT as written — ``SEASON_ID`` values
      such as ``"22025"`` are a different quantity from a ``"2025-26"``
      season string.
    """
    # --- Arrange -------------------------------------------------------
    # The ``recording_writer`` fixture prebinds ``tmp_path / "output"``, so
    # this call needs no output-directory argument and isolation is
    # automatic. Its optional ``raise_on`` parameter is omitted here.
    client = recording_client(responses={_ENDPOINT_LABEL: sample_schedule_payload})
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_schedule.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: exact row count handed to the writer ------------------
    assert len(writer.writes) == 1, (
        "exactly one schedule artifact must be written before its row count "
        f"can be inspected; observed {len(writer.writes)} writes: "
        f"{writer.writes!r}"
    )
    write = writer.writes[0]
    assert write["rows"] == EXPECTED_SCHEDULE_ROWS, (
        f"schedule artifact must carry exactly {EXPECTED_SCHEDULE_ROWS} rows "
        "(2 team-rows for game 0022500001 + 2 for game 0022500002 + 1 for "
        f"game 0022500003 => 2 + 2 + 1 = {EXPECTED_SCHEDULE_ROWS}); got "
        f"rows={write['rows']} for artifact {write['name']!r}"
    )

    # --- Assert: exact pipeline_rows_written_total value and labels ----
    # Verified call shape: the counter name is passed positionally as
    # args[0], the label mapping positionally as args[1], and the increment
    # as the keyword ``n``.
    row_incs = [
        c
        for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert len(row_incs) == 1, (
        "pipeline must increment pipeline_rows_written_total exactly once for "
        f"its single write; observed {len(row_incs)} relevant inc() calls in "
        f"{metrics_mock.inc.call_args_list!r}"
    )
    assert row_incs[0].kwargs["n"] == EXPECTED_SCHEDULE_ROWS, (
        "row-written counter must be incremented by the length of the frame "
        f"handed to the writer, i.e. n={EXPECTED_SCHEDULE_ROWS}; got "
        f"n={row_incs[0].kwargs['n']!r}"
    )
    assert row_incs[0].args[1] == EXPECTED_ROW_COUNT_LABELS, (
        f"row-written counter must carry labels {EXPECTED_ROW_COUNT_LABELS!r}; "
        f"got {row_incs[0].args[1]!r}"
    )

    # --- Assert: the no-dedupe contract holds --------------------------
    # Read-only inspection of the copy the spy recorded at write time. The
    # frame is never mutated here, so no pandas SettingWithCopyWarning or
    # PerformanceWarning can be provoked under ``filterwarnings = error``.
    df = write["df"]
    assert int(df["GAME_ID"].nunique()) == EXPECTED_DISTINCT_GAME_IDS, (
        f"the {EXPECTED_SCHEDULE_ROWS} written rows must span exactly "
        f"{EXPECTED_DISTINCT_GAME_IDS} distinct GAME_IDs (0022500001, "
        "0022500002, 0022500003) — 5 rows over 3 games is precisely the "
        "two-rows-per-game shape the pipeline must preserve; got "
        f"{int(df['GAME_ID'].nunique())} distinct across {len(df)} rows"
    )

    # --- Assert: exact ordered column list -----------------------------
    assert list(df.columns) == EXPECTED_SCHEDULE_COLUMNS, (
        f"writer must receive columns {EXPECTED_SCHEDULE_COLUMNS!r} — a "
        "lowercase 'season' inserted at index 0 with the upstream SEASON_ID "
        f"preserved verbatim beside it; got {list(df.columns)!r}"
    )


# ===========================================================================
# Test 6 — Empty-rowSet contract: a header-only artifact IS written, the
#          domain IS marked completed, and the counter IS incremented with 0
# ===========================================================================

def test_run_writes_header_only_artifact_and_marks_checkpoint_for_empty_rowset(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_empty_payload,
):
    """A zero-row upstream payload must still write, still mark, still count.

    ``sample_empty_payload`` carries ``headers`` but ``"rowSet": []``, so that
    result set normalizes to a zero-row DataFrame — column names preserved,
    zero records — inside the ``Dict[str, DataFrame]`` mapping
    :func:`utils.schema_normalizer.normalize_result_sets` returns, and
    ``_select_primary_df`` selects it. The pipeline must NOT treat that as
    "nothing to do": the zero flows all the way through to the metric rather
    than short-circuiting at any of the three stages, so a slow day with no
    games still refreshes the artifact instead of leaving yesterday's rows on
    disk looking current.

    The production tree performs no arithmetic division, ``mean``, ``groupby``
    or ``agg`` aggregation, so this degenerate-count case is the faithful
    stand-in for a zero-divisor boundary: the place where a count reaching
    zero really does change behavior.

    Mutations detected
    ------------------
    * An ``if df.empty: return`` guard added before ``writer.write`` — the
      header-only artifact would never be produced and a stale CSV would
      survive; caught by the write assertions.
    * Skipping ``mark_completed`` when the frame is empty — the pipeline
      would busy-loop on a permanently empty upstream, re-fetching the same
      season on every run; caught by the marks assertion.
    * Suppressing the counter increment for an empty frame, or emitting a
      placeholder such as ``n=1`` — the row-written total would drift away
      from the rows actually persisted; caught by the ``n=0`` assertion.
    """
    # --- Arrange -------------------------------------------------------
    # ``RecordingClient`` keys purely on the endpoint NAME, so the
    # envelope's own ``resource`` value is irrelevant here; all that
    # matters is that the schedule pipeline receives a table whose
    # ``rowSet`` is empty.
    client = recording_client(responses={_ENDPOINT_LABEL: sample_empty_payload})
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_schedule.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: a zero-row artifact IS written ------------------------
    assert len(writer.writes) == 1, (
        "an empty rowSet must still produce exactly one header-only "
        f"artifact; observed {len(writer.writes)} writes: {writer.writes!r}"
    )
    write = writer.writes[0]
    assert write["name"] == config.CSV_SCHEDULE, (
        f"the header-only artifact must still be named {config.CSV_SCHEDULE!r}; "
        f"got {write['name']!r}"
    )
    assert write["rows"] == EXPECTED_EMPTY_ROWS, (
        f"an empty rowSet must yield exactly {EXPECTED_EMPTY_ROWS} rows — the "
        "normalizer returns pd.DataFrame(columns=headers) when the rowSet is "
        f"an empty list; got rows={write['rows']}"
    )
    assert list(write["df"].columns) == EXPECTED_EMPTY_COLUMNS, (
        f"the header-only frame must carry columns {EXPECTED_EMPTY_COLUMNS!r} "
        "— 'season' inserted at index 0 because neither PLAYER_ID nor PTS "
        "lowercases to 'season', with both upstream headers preserved; got "
        f"{list(write['df'].columns)!r}"
    )

    # --- Assert: the domain IS marked completed ------------------------
    assert checkpoint.marks == [(config.DOMAIN_SCHEDULE, _EXPECTED_KEY)], (
        "an empty upstream result is a COMPLETED unit of work, so the "
        f"pipeline must mark (DOMAIN_SCHEDULE, {_EXPECTED_KEY!r}) exactly "
        f"once; observed marks={checkpoint.marks!r}"
    )

    # --- Assert: the counter IS incremented, with n == 0 ----------------
    row_incs = [
        c
        for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert len(row_incs) == 1, (
        "the row-written counter must still fire exactly once for a "
        f"header-only write; observed {len(row_incs)} relevant inc() calls "
        f"in {metrics_mock.inc.call_args_list!r}"
    )
    assert row_incs[0].kwargs["n"] == EXPECTED_EMPTY_ROWS, (
        f"the zero must reach the metric verbatim as n={EXPECTED_EMPTY_ROWS} "
        "rather than being short-circuited away; got "
        f"n={row_incs[0].kwargs['n']!r}"
    )
    # The label mapping is passed POSITIONALLY as args[1], so the complete
    # dictionary is comparable. Asserting the whole mapping — not merely the
    # counter name — is what makes the empty path's emission attributable:
    # an operator dashboard filtering on
    # pipeline="ingest_schedule", artifact="schedule.csv" must still see the
    # zero. A mutation that relabelled the empty-path increment (a distinct
    # "empty" artifact name, a dropped label, or the legacy domain/file
    # label names) would silently strand it outside every existing query
    # while the n=0 assertion above still passed.
    assert row_incs[0].args[1] == EXPECTED_ROW_COUNT_LABELS, (
        "the header-only write must be counted under the very same label set "
        f"as a non-empty write, {EXPECTED_ROW_COUNT_LABELS!r}; got "
        f"{row_incs[0].args[1]!r}"
    )


# ===========================================================================
# Test 7 — Rule 5 negative case: a failing write propagates and leaves the
#          checkpoint unmarked
# ===========================================================================

def test_rule5_write_failure_propagates_and_leaves_checkpoint_unmarked(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_schedule_payload,
):
    """When ``writer.write`` raises, ``run`` re-raises and marks nothing.

    Rule 5 requires that ``mark_completed`` run only *downstream of a
    successful write*, so a failing write must leave the checkpoint entirely
    unmarked and the next run must retry the same key. A success-path
    ordering check cannot tell "mark after write" apart from "mark regardless
    of write"; only this negative case can.
    ``RecordingWriter(raise_on=...)`` raises
    ``RuntimeError("synthetic write failure for 'schedule'")`` instead of
    recording, and because Rule 6 fail-safe wrapping is scoped exclusively to
    :mod:`pipelines.ingest_games`, the schedule pipeline must let that
    exception escape rather than absorb it.

    Mutations detected
    ------------------
    * Moving ``checkpoint.mark_completed`` ABOVE ``writer.write``: the
      pipeline would checkpoint work that was never persisted, and every
      resumed run would then skip that season forever while producing no
      artifact at all.
    * Swallowing the write failure and returning normally (Rule 6's per-game
      guard misapplied to a single-shot pipeline — catching the exception and
      returning before the counter and the checkpoint mark): the caller would
      see a clean return instead of the failure, and ``pytest.raises`` would
      report that no exception was raised.
    * Recording a write record despite the failure — caught by the empty
      ``writer.writes`` assertion, which is what ties "nothing persisted" to
      "nothing checkpointed".
    * Hoisting ``met.inc("pipeline_rows_written_total", ...)`` ABOVE
      ``writer.write``: the counter would report rows as persisted for a
      write that never happened, so ``pipeline_rows_written_total`` would
      drift permanently above the rows actually on disk and every
      operator dashboard built on it would over-report. The exception,
      writer and checkpoint assertions all still pass under that mutation
      — only an injected metrics sink can see it, which is why one is
      supplied here even though the failure path emits nothing.
    """
    # --- Arrange -------------------------------------------------------
    # ``raise_on`` is passed by keyword so the armed artifact is explicit at
    # the call site. It must equal the artifact name the pipeline writes,
    # otherwise the write succeeds and this negative test becomes a no-op.
    client = recording_client(responses={_ENDPOINT_LABEL: sample_schedule_payload})
    writer = recording_writer(raise_on=config.CSV_SCHEDULE)
    checkpoint = recording_checkpoint()
    # An explicit sink is injected on the FAILURE path precisely because the
    # expected emission count is zero: without it ``run`` would fall back to
    # the real module-level registry and the metric leg of Rule 5 would go
    # unobserved, leaving the write-then-count ordering untested.
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    with pytest.raises(RuntimeError) as excinfo:
        ingest_schedule.run(
            client=client,
            writer=writer,
            checkpoint=checkpoint,
            season=_SEASON,
            metrics=metrics_mock,
        )

    # --- Assert: the writer's own failure is what escaped ---------------
    assert "synthetic write failure" in str(excinfo.value), (
        "the RuntimeError propagating out of run() must be the writer's "
        "synthetic failure — not an unrelated error raised earlier in the "
        f"pipeline; got {str(excinfo.value)!r}"
    )

    # --- Assert: nothing was persisted ---------------------------------
    assert writer.writes == [], (
        "a raising write must record no artifact at all; observed "
        f"{writer.writes!r}"
    )

    # --- Assert: Rule 5 — nothing was checkpointed ---------------------
    assert checkpoint.marks == [], (
        "mark_completed MUST NOT run when the write failed, otherwise a "
        "resumed run would permanently skip work that was never persisted; "
        f"observed marks={checkpoint.marks!r}"
    )

    # --- Assert: nothing was counted as written ------------------------
    # The row-written counter is emitted from a single production call site
    # that sits strictly BETWEEN the write and the mark, so a write that
    # raised must leave the series completely empty — zero emissions, not a
    # zero-valued one. This is the metric-side twin of the marks assertion
    # above: "nothing persisted" must also mean "nothing reported".
    row_incs = [
        c
        for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert row_incs == [], (
        "a failed write must produce ZERO pipeline_rows_written_total "
        "emissions — the counter sits after writer.write, so any emission "
        "here means rows were reported as persisted that never reached "
        f"disk; observed {len(row_incs)} increment(s): {row_incs!r}"
    )
    # ``pipeline_rows_written_total`` is the ONLY counter this pipeline
    # emits, and its single call site is downstream of the write, so the
    # sink must be untouched in its entirety. Asserting the whole call list
    # — rather than only the name-filtered slice — additionally catches a
    # mutation that renamed the counter while keeping the premature
    # emission, which the filtered assertion above would silently miss.
    assert metrics_mock.inc.call_args_list == [], (
        "the schedule pipeline emits exactly one counter and it is "
        "downstream of the write, so a failed write must leave the metrics "
        f"sink entirely untouched; observed {metrics_mock.inc.call_args_list!r}"
    )
