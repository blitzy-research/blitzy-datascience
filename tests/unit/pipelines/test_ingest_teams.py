"""Unit tests for :mod:`pipelines.ingest_teams` (Feature F-010).

Scope
-----
This module exercises the Teams ingestion pipeline orchestrator at
:mod:`pipelines.ingest_teams` — the F-010 component in the Agent Action Plan
(AAP) feature catalog. The pipeline is deliberately narrow: it fetches a
single NBA Stats endpoint (``leaguedashteamstats``) and emits a single flat
CSV artifact (``teams.csv``) with a single checkpoint key
(``"leaguedashteamstats:<season>"``). Per-team detail endpoints
(:func:`endpoints.teams.fetch_teamgamelog` and
:func:`endpoints.teams.fetch_teamdashboardbygeneralsplits`) exist as library
functions but are intentionally NOT invoked by :func:`ingest_teams.run`; the
F-010 pipeline is scoped to league-wide team aggregates only.

Verified behaviors
------------------
1. **Happy path** — invoking :func:`ingest_teams.run` against a clean
   checkpoint results in exactly one ``leaguedashteamstats`` call on the
   injected :class:`RecordingClient`, exactly one ``writer.write`` call with
   ``name == config.CSV_TEAMS`` and ``season == "2025-26"``, and exactly one
   ``checkpoint.mark_completed`` call keyed by ``(config.DOMAIN_TEAMS,
   "leaguedashteamstats:2025-26")``. The ``pipeline_rows_written_total``
   metric is incremented exactly once.

2. **Idempotency (Rule 5 resume behavior)** — invoking
   :func:`ingest_teams.run` with a pre-seeded checkpoint short-circuits BEFORE
   any HTTP call, any CSV write, or any additional ``mark_completed``
   invocation. Only the ``is_completed`` probe should be observed.

3. **Rule 5 ordering** — ``checkpoint.is_completed`` runs BEFORE fetch,
   ``writer.write`` precedes ``checkpoint.mark_completed``, and
   ``mark_completed`` is the last side-effect recorded by the spies. Per AAP
   §0.7.2.5, this ordering is the durability invariant that makes the pipeline
   resumable across crashes.

4. **Season column guarantee** — the DataFrame handed to ``writer.write``
   carries a lowercase ``"season"`` column. :mod:`pipelines.ingest_teams`
   normalizes the upstream envelope and invokes an internal
   ``_ensure_season_column`` helper that inserts the column at position 0 if
   the upstream payload did not already carry a season-like column. Without
   this guarantee the downstream consumer of ``teams.csv`` would be unable to
   partition team aggregates by season, which is the README output contract.

Style conventions
-----------------
* Domain and CSV-artifact identifiers are referenced symbolically via
  :data:`config.DOMAIN_TEAMS` and :data:`config.CSV_TEAMS` (AAP Phase 7 style
  requirement) so a future rename of either constant automatically propagates
  to the assertions without test-file edits.
* Collaborators (client/writer/checkpoint) are handwritten spies supplied by
  :mod:`tests.conftest` factory fixtures (``recording_client``,
  ``recording_writer``, ``recording_checkpoint``) —
  :class:`~unittest.mock.MagicMock` is used ONLY for the optional ``metrics``
  collaborator, whose interface surface (``inc``, ``observe``) is deliberately
  narrow.
* Rule 6 (fail-safe per-entity iteration) does NOT apply to this pipeline —
  per AAP §0.7.2.6 only :mod:`pipelines.ingest_games` wraps its loop in
  ``try/except Exception``; teams propagates exceptions upward.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest  # noqa: F401  (imported to satisfy Phase 1 header convention)

import config
from pipelines import ingest_teams


# ---------------------------------------------------------------------------
# Module-level constants shared by every test
# ---------------------------------------------------------------------------
#
# ``_SEASON``           — the canonical NBA season identifier exercised by all
#                         four tests. Chosen to match AAP §0.1.1 default.
# ``_ENDPOINT_LABEL``   — the exact string passed to ``NBAClient.get`` by
#                         :func:`endpoints.teams.fetch_leaguedashteamstats`;
#                         also the leading token of the checkpoint key.
# ``_EXPECTED_KEY``     — the fully-qualified checkpoint key that
#                         :mod:`pipelines.ingest_teams` constructs internally
#                         (see the ``key = f"{endpoint_label}:{season}"`` line
#                         in the pipeline source). Duplicated here so the test
#                         file is self-contained and does not reach into the
#                         pipeline module's private construction logic.
_SEASON = "2025-26"
_ENDPOINT_LABEL = "leaguedashteamstats"
_EXPECTED_KEY = f"{_ENDPOINT_LABEL}:{_SEASON}"


# ---------------------------------------------------------------------------
# Hand-derived EXPECTED values
#
# Every constant below follows from the ``sample_single_table_payload`` and
# ``sample_empty_payload`` fixtures in :mod:`tests.conftest` together with this
# pipeline's INTENDED semantics, and the arithmetic behind each number is
# spelled out beside it.
#
# These are EXPECTED (output) values and are deliberately kept separate from the
# fixture INPUT values, which remain owned by ``tests/conftest.py``.
# ---------------------------------------------------------------------------

#: Rows the writer must receive on the happy path.
#:
#: ``sample_single_table_payload`` declares a single ``resultSets`` table whose
#: ``rowSet`` holds three entries — one for Nikola Jokić, one for Luka Dončić
#: and one for Jayson Tatum. 1 + 1 + 1 = 3.
#:
#: ``ingest_teams`` performs no filtering, no aggregation and no
#: deduplication between ``normalize_result_sets`` and ``writer.write``: it
#: selects the primary frame, guarantees a ``season`` column and hands the
#: frame straight to the writer. All 3 rows must therefore arrive verbatim.
EXPECTED_TEAMS_ROWS = 3

#: Distinct ``PLAYER_ID`` values spanned by those 3 rows: 203999, 1629029 and
#: 1628369 → 3.
#:
#: Asserting 3 distinct IDs ALONGSIDE 3 rows is strictly stronger than either
#: number alone: a mutation that duplicated one record while dropping another
#: (a stray ``concat`` paired with a ``.head(3)``, or an ``iloc`` slice
#: rebuilt from the wrong index) preserves the row count but collapses the
#: distinct count to 2. Only the pair pins row-for-row conservation.
EXPECTED_DISTINCT_PLAYER_IDS = 3

#: Exact ordered column list of the frame handed to the writer.
#:
#: ``sample_single_table_payload`` declares the headers ``PLAYER_ID``,
#: ``PLAYER_NAME``, ``TEAM_ID``, ``PTS`` in that order, and the normalizer
#: preserves them verbatim. The pipeline's private ``_ensure_season_column``
#: helper then tests ``c.lower() == "season"`` against every column: none of
#: the four matches, so a lowercase ``season`` column is inserted at index 0
#: and every upstream column is preserved untouched behind it. Hence
#: 1 inserted + 4 upstream = 5 columns in this order.
EXPECTED_TEAMS_COLUMNS = [
    "season",
    "PLAYER_ID",
    "PLAYER_NAME",
    "TEAM_ID",
    "PTS",
]

#: Rows written for a payload whose table carries ``headers`` but an empty
#: ``rowSet``.
#:
#: ``sample_empty_payload`` supplies ``"rowSet": []``, so that result set
#: normalizes to a zero-row DataFrame — column names preserved, zero records —
#: inside the ``Dict[str, DataFrame]`` mapping ``normalize_result_sets``
#: returns, and ``_select_primary_df`` selects it. len(df) is therefore 0,
#: which is both the ``rows`` the writer records and the ``n`` the row-count
#: counter receives.
EXPECTED_EMPTY_ROWS = 0

#: Exact ordered column list for the empty-``rowSet`` case.
#:
#: ``sample_empty_payload`` declares the headers ``PLAYER_ID`` and ``PTS``.
#: Neither lowercases to ``"season"``, so ``_ensure_season_column`` inserts
#: ``season`` at index 0 exactly as it does on the happy path —
#: 1 inserted + 2 upstream = 3 columns — proving the header-only artifact is a
#: real, fully-formed frame rather than an empty placeholder.
EXPECTED_EMPTY_COLUMNS = [
    "season",
    "PLAYER_ID",
    "PTS",
]

#: Label set every ``pipeline_rows_written_total`` increment must carry.
#:
#: The ``{"pipeline": "ingest_<domain>", "artifact": "<csv_name>.csv"}``
#: convention is repository-wide — all five pipelines emit it — so it is a
#: published observability contract. Built from :data:`config.CSV_TEAMS` so
#: renaming the artifact stays a single-point edit.
EXPECTED_ROW_COUNT_LABELS = {
    "pipeline": "ingest_teams",
    "artifact": f"{config.CSV_TEAMS}.csv",
}


# ---------------------------------------------------------------------------
# Test 1 — Happy path: fetch → write → mark checkpoint
# ---------------------------------------------------------------------------
def test_run_happy_path_writes_teams_and_marks_checkpoint(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_single_table_payload,
    tmp_path,
):
    """End-to-end happy path: fetch ``leaguedashteamstats`` → write ``teams.csv`` → mark checkpoint.

    Given a clean (empty) checkpoint and a recording client preloaded with a
    canonical single-table ``resultSets`` envelope for the
    ``leaguedashteamstats`` endpoint, invoking :func:`ingest_teams.run` must:

    * produce at least one ``leaguedashteamstats`` call on the client spy,
    * produce exactly one ``writer.write`` call whose ``name`` equals
      :data:`config.CSV_TEAMS`, whose ``season`` equals ``_SEASON``, and whose
      recorded ``rows`` count is strictly positive,
    * record the ``is_completed`` probe for
      ``(config.DOMAIN_TEAMS, _EXPECTED_KEY)``,
    * produce exactly one ``mark_completed`` call for the same
      ``(domain, key)`` tuple (Rule 5),
    * increment the ``pipeline_rows_written_total`` metric exactly once.
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_single_table_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    ingest_teams.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Client spy: the leaguedashteamstats endpoint was invoked ----------
    assert any(call[0] == _ENDPOINT_LABEL for call in client.calls), (
        f"expected a {_ENDPOINT_LABEL} call; got {client.calls!r}"
    )

    # --- Writer spy: exactly one write with the expected identifiers ------
    assert len(writer.writes) == 1, (
        f"expected exactly 1 writer.write call; got {len(writer.writes)}: {writer.writes!r}"
    )
    write = writer.writes[0]
    assert write["name"] == config.CSV_TEAMS, (
        f"expected CSV name {config.CSV_TEAMS!r}; got {write['name']!r}"
    )
    assert write["season"] == _SEASON, (
        f"expected season {_SEASON!r}; got {write['season']!r}"
    )
    assert write["rows"] > 0, (
        f"expected non-empty DataFrame; got rows={write['rows']}"
    )

    # --- Checkpoint spy: is_completed probed, mark_completed called once --
    assert (config.DOMAIN_TEAMS, _EXPECTED_KEY) in checkpoint.checks, (
        f"expected is_completed probe for {(config.DOMAIN_TEAMS, _EXPECTED_KEY)!r}; "
        f"got checks={checkpoint.checks!r}"
    )
    assert checkpoint.marks == [(config.DOMAIN_TEAMS, _EXPECTED_KEY)], (
        f"expected exactly one mark_completed for {(config.DOMAIN_TEAMS, _EXPECTED_KEY)!r}; "
        f"got marks={checkpoint.marks!r}"
    )

    # --- Metrics spy: pipeline_rows_written_total incremented exactly once --
    rows_written_calls = [
        c for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert len(rows_written_calls) == 1, (
        f"expected exactly 1 pipeline_rows_written_total increment; "
        f"got {len(rows_written_calls)}: {metrics_mock.inc.call_args_list!r}"
    )


# ---------------------------------------------------------------------------
# Test 2 — Idempotency: pre-seeded checkpoint short-circuits the pipeline
# ---------------------------------------------------------------------------
def test_run_idempotent_skip_when_already_checkpointed(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_single_table_payload,
    tmp_path,
):
    """If checkpoint reports completion, the pipeline must not fetch or write.

    Pre-seeding the :class:`RecordingCheckpoint` with
    ``{config.DOMAIN_TEAMS: [_EXPECTED_KEY]}`` simulates a prior successful
    run. On re-invocation the pipeline must:

    * make zero client calls (no HTTP against NBA Stats),
    * make zero writer calls (no CSV overwrite),
    * make zero additional ``mark_completed`` calls (the existing checkpoint
      entry already records completion),
    * still record the ``is_completed`` probe — this IS expected because the
      pipeline discovers its short-circuit precisely by asking the checkpoint,
    * make zero ``pipeline_rows_written_total`` increments (nothing was
      written, nothing to count).
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_single_table_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint(
        completed={config.DOMAIN_TEAMS: [_EXPECTED_KEY]},
    )
    metrics_mock = MagicMock()

    ingest_teams.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- No side effects on fetch, write, or mark paths ------------------
    assert client.calls == [], f"client must not be called; got {client.calls!r}"
    assert writer.writes == [], f"writer must not be called; got {writer.writes!r}"
    assert checkpoint.marks == [], (
        f"no additional mark_completed calls expected; got {checkpoint.marks!r}"
    )

    # --- But the is_completed probe IS expected (that's how we skipped) --
    assert (config.DOMAIN_TEAMS, _EXPECTED_KEY) in checkpoint.checks, (
        f"expected is_completed probe for {(config.DOMAIN_TEAMS, _EXPECTED_KEY)!r}; "
        f"got checks={checkpoint.checks!r}"
    )

    # --- Metrics: zero rows-written increments when nothing was written --
    rows_written_calls = [
        c for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert rows_written_calls == [], (
        f"expected zero pipeline_rows_written_total increments on idempotent skip; "
        f"got {rows_written_calls!r}"
    )


# ---------------------------------------------------------------------------
# Test 3 — Rule 5 ordering: is_completed precedes mark_completed
# ---------------------------------------------------------------------------
def test_rule5_is_completed_precedes_mark_completed(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_single_table_payload,
    tmp_path,
):
    """Rule 5: ``is_completed`` precedes fetch; ``mark_completed`` follows ``writer.write``.

    The ordering invariant established by AAP §0.7.2.5 requires that:

    1. The checkpoint is probed BEFORE the endpoint is fetched (so a resumed
       run never re-incurs the HTTP cost of a completed pull).
    2. The checkpoint is marked AFTER the CSV is successfully written (so a
       crash between fetch and write leaves the key in a re-attemptable
       state).

    The :class:`RecordingCheckpoint` spy records ``checks`` and ``marks`` as
    ordered lists, letting us assert the exact sequence of observations:
    exactly one ``is_completed`` probe, exactly one ``writer.write``, exactly
    one ``mark_completed``, all for the same ``(domain, key)`` tuple.
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_single_table_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint()

    ingest_teams.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
    )

    # Exactly one is_completed probe, for the expected (domain, key) tuple
    assert checkpoint.checks == [(config.DOMAIN_TEAMS, _EXPECTED_KEY)], (
        f"expected exactly one is_completed probe for {(config.DOMAIN_TEAMS, _EXPECTED_KEY)!r}; "
        f"got checks={checkpoint.checks!r}"
    )

    # Exactly one writer.write — the side effect that must precede mark
    assert len(writer.writes) == 1, (
        f"expected exactly 1 writer.write call; got {len(writer.writes)}: {writer.writes!r}"
    )

    # Exactly one mark_completed — and for the same (domain, key) tuple
    assert checkpoint.marks == [(config.DOMAIN_TEAMS, _EXPECTED_KEY)], (
        f"expected exactly one mark_completed for {(config.DOMAIN_TEAMS, _EXPECTED_KEY)!r}; "
        f"got marks={checkpoint.marks!r}"
    )


# ---------------------------------------------------------------------------
# Test 4 — Season column guarantee: writer receives a DataFrame with `season`
# ---------------------------------------------------------------------------
def test_writer_receives_dataframe_with_season_column(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_single_table_payload,
    tmp_path,
):
    """The pipeline must guarantee a ``season`` column on the DataFrame handed to the writer.

    The :mod:`pipelines.ingest_teams` orchestrator normalizes the upstream
    ``resultSets`` envelope and invokes its internal ``_ensure_season_column``
    helper before handing the DataFrame to :meth:`RecordingWriter.write`. That
    helper inserts a lowercase ``"season"`` column at position 0 IFF the
    upstream payload did not already carry a season-like column.

    The :data:`sample_single_table_payload` fixture emits headers
    ``["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "PTS"]`` — none of which are
    season-like — so for this fixture the helper WILL inject the column. The
    lowercase-casefolded membership check below tolerates both the injected
    lowercase ``"season"`` and any future upstream passthrough using an
    uppercase variant (``SEASON``, ``SEASON_ID``), satisfying the README
    output contract that every row in ``teams.csv`` must be partitionable by
    season.

    The :class:`RecordingWriter` spy stores an independent ``.copy()`` of the
    DataFrame at write time, so ``writer.writes[0]["df"]`` accurately reflects
    the exact frame the pipeline handed off — immune to any subsequent
    mutation within :func:`ingest_teams.run` (there are none today, but the
    ``.copy()`` behavior is a defensive guarantee of the spy).
    """
    client = recording_client(responses={_ENDPOINT_LABEL: sample_single_table_payload})
    writer = recording_writer(tmp_path)
    checkpoint = recording_checkpoint()

    ingest_teams.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
    )

    df = writer.writes[0]["df"]
    lower_cols = {c.lower() for c in df.columns}
    assert "season" in lower_cols, (
        f"DataFrame must contain a 'season' column (any case); "
        f"got columns={list(df.columns)!r}"
    )


# ---------------------------------------------------------------------------
# Test 5 — Exact-value happy path: 3 rows written, n=3 emitted, 3 distinct
#          PLAYER_IDs, and the exact ordered column list
# ---------------------------------------------------------------------------
def test_run_writes_exact_row_count_and_metric_value_for_teams(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_single_table_payload,
):
    """``run`` hands the writer all 3 fixture rows and emits ``n=3`` verbatim.

    The row count and the counter value are asserted as exact integers rather
    than as presence or positivity, because "a write happened" and "the count
    is above zero" cannot distinguish the correct quantity from a wrong one.
    The distinct-``PLAYER_ID`` count is asserted alongside the row count: the
    pair ``3 rows over 3 distinct identifiers`` is what pins row-for-row
    conservation, since either number alone tolerates a swap.

    Mutations detected
    ------------------
    * A ``drop_duplicates()`` — with or without a key subset — or any other
      filtering step inserted between ``normalize_result_sets`` and
      ``writer.write`` that silently discards a record: the row-count and
      ``n=`` assertions fail together.
    * A record duplicated while another is dropped (a stray ``concat`` paired
      with a ``.head(3)``, or a reindexed ``iloc`` slice): the row count still
      reads 3, so only the distinct-``PLAYER_ID`` assertion catches it.
    * Emitting the wrong quantity into ``pipeline_rows_written_total`` — a
      constant ``1``, a column count, or a distinct-key count instead of a row
      count — fails the ``n=`` assertion even though the counter still fires
      exactly once.
    * Renaming the counter empties the filtered call list, so the call-count
      assertion fails first. Changing the label KEYS or VALUES while keeping
      the counter name is what the label assertion catches — the failure mode
      that would silently break every operator dashboard querying
      ``pipeline="ingest_teams"``.
    * A "fix" to ``_ensure_season_column`` that suppressed the insertion,
      appended ``season`` last instead of at index 0, or renamed an upstream
      header, fails the ordered column-list assertion. That helper is CORRECT
      as written — a ``SEASON_ID`` value such as ``"22025"`` is a different
      quantity from a ``"2025-26"`` season string, so matching only
      ``c.lower() == "season"`` is deliberate.
    """
    # --- Arrange -------------------------------------------------------
    # The ``recording_writer`` fixture prebinds ``tmp_path / "output"``, so
    # this call needs no output-directory argument and isolation is automatic.
    # Its optional ``raise_on`` parameter is omitted here.
    client = recording_client(responses={_ENDPOINT_LABEL: sample_single_table_payload})
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_teams.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: exact row count handed to the writer ------------------
    assert len(writer.writes) == 1, (
        "exactly one teams artifact must be written before its row count can "
        f"be inspected; observed {len(writer.writes)} writes: {writer.writes!r}"
    )
    write = writer.writes[0]
    assert write["rows"] == EXPECTED_TEAMS_ROWS, (
        f"teams artifact must carry exactly {EXPECTED_TEAMS_ROWS} rows (one "
        "rowSet entry each for Jokić, Dončić and Tatum => 1 + 1 + 1 = "
        f"{EXPECTED_TEAMS_ROWS}); got rows={write['rows']} for artifact "
        f"{write['name']!r}"
    )

    # --- Assert: exact pipeline_rows_written_total value and labels ----
    # Verified call shape: the counter name is passed positionally as args[0],
    # the label mapping positionally as args[1], and the increment as the
    # keyword ``n``.
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
    assert row_incs[0].kwargs["n"] == EXPECTED_TEAMS_ROWS, (
        "row-written counter must be incremented by the length of the frame "
        f"handed to the writer, i.e. n={EXPECTED_TEAMS_ROWS}; got "
        f"n={row_incs[0].kwargs['n']!r}"
    )
    assert row_incs[0].args[1] == EXPECTED_ROW_COUNT_LABELS, (
        f"row-written counter must carry labels {EXPECTED_ROW_COUNT_LABELS!r}; "
        f"got {row_incs[0].args[1]!r}"
    )

    # --- Assert: row-for-row conservation of the three fixture records --
    # Read-only inspection of the copy the spy recorded at write time. The
    # frame is never mutated here, so no pandas SettingWithCopyWarning or
    # PerformanceWarning can be provoked under ``filterwarnings = error``.
    df = write["df"]
    assert int(df["PLAYER_ID"].nunique()) == EXPECTED_DISTINCT_PLAYER_IDS, (
        f"the {EXPECTED_TEAMS_ROWS} written rows must span exactly "
        f"{EXPECTED_DISTINCT_PLAYER_IDS} distinct PLAYER_IDs (203999, 1629029, "
        "1628369) — 3 rows over 3 distinct identifiers is what proves every "
        "fixture record survived exactly once; got "
        f"{int(df['PLAYER_ID'].nunique())} distinct across {len(df)} rows"
    )

    # --- Assert: exact ordered column list -----------------------------
    assert list(df.columns) == EXPECTED_TEAMS_COLUMNS, (
        f"writer must receive columns {EXPECTED_TEAMS_COLUMNS!r} — a lowercase "
        "'season' inserted at index 0 with all four upstream headers preserved "
        f"verbatim behind it; got {list(df.columns)!r}"
    )


# ---------------------------------------------------------------------------
# Test 6 — Empty-rowSet contract: a header-only artifact IS written, the
#          domain IS marked completed, and the counter IS incremented with 0
# ---------------------------------------------------------------------------
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
    than short-circuiting at any of the three stages, so a season whose team
    aggregates are momentarily unavailable upstream still refreshes the
    artifact instead of leaving yesterday's rows on disk looking current.

    The production tree performs no arithmetic division, ``mean``, ``groupby``
    or ``agg`` aggregation, so this degenerate-count case is the faithful
    stand-in for a zero-divisor boundary: the place where a count reaching zero
    really does change behavior.

    Mutations detected
    ------------------
    * An ``if df.empty: return`` guard added before ``writer.write`` — the
      header-only artifact would never be produced and a stale CSV would
      survive on disk looking current; caught by the write assertions.
    * Skipping ``mark_completed`` when the frame is empty — the pipeline would
      busy-loop on a permanently empty upstream, re-fetching the same season on
      every run; caught by the marks assertion.
    * Suppressing the counter increment for an empty frame, or emitting a
      placeholder such as ``n=1`` — the row-written total would drift away from
      the rows actually persisted; caught by the ``n=0`` assertion.
    * Substituting a bare ``pd.DataFrame()`` for the normalizer's
      header-preserving zero-row frame — the artifact would lose its column
      headers entirely and a header-only CSV would degenerate into an empty
      file; caught by the ordered column-list assertion.
    """
    # --- Arrange -------------------------------------------------------
    # ``RecordingClient`` keys purely on the endpoint NAME, so the envelope's
    # own ``resource`` value is irrelevant here; all that matters is that the
    # teams pipeline receives a table whose ``rowSet`` is empty.
    client = recording_client(responses={_ENDPOINT_LABEL: sample_empty_payload})
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_teams.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: a zero-row artifact IS written ------------------------
    assert len(writer.writes) == 1, (
        "an empty rowSet must still produce exactly one header-only artifact; "
        f"observed {len(writer.writes)} writes: {writer.writes!r}"
    )
    write = writer.writes[0]
    assert write["name"] == config.CSV_TEAMS, (
        f"the header-only artifact must still be named {config.CSV_TEAMS!r}; "
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
    assert checkpoint.marks == [(config.DOMAIN_TEAMS, _EXPECTED_KEY)], (
        "an empty upstream result is a COMPLETED unit of work, so the pipeline "
        f"must mark (DOMAIN_TEAMS, {_EXPECTED_KEY!r}) exactly once; observed "
        f"marks={checkpoint.marks!r}"
    )

    # --- Assert: the counter IS incremented, with n == 0 ---------------
    row_incs = [
        c
        for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert len(row_incs) == 1, (
        "the row-written counter must still fire exactly once for a "
        f"header-only write; observed {len(row_incs)} relevant inc() calls in "
        f"{metrics_mock.inc.call_args_list!r}"
    )
    assert row_incs[0].kwargs["n"] == EXPECTED_EMPTY_ROWS, (
        f"the zero must reach the metric verbatim as n={EXPECTED_EMPTY_ROWS} "
        "rather than being short-circuited away; got "
        f"n={row_incs[0].kwargs['n']!r}"
    )
    # Asserting the whole label mapping — not merely the counter name — is what
    # makes the empty path's emission attributable: an operator dashboard
    # filtering on pipeline="ingest_teams", artifact="teams.csv" must still see
    # the zero. A mutation that relabelled the empty-path increment (a distinct
    # "empty" artifact name, a dropped label, or legacy domain/file label keys)
    # would silently strand it outside every existing query while the n=0
    # assertion above still passed.
    assert row_incs[0].args[1] == EXPECTED_ROW_COUNT_LABELS, (
        "the header-only write must be counted under the very same label set "
        f"as a non-empty write, {EXPECTED_ROW_COUNT_LABELS!r}; got "
        f"{row_incs[0].args[1]!r}"
    )


# ---------------------------------------------------------------------------
# Test 7 — Rule 5 negative case: a failing write propagates and leaves the
#          checkpoint unmarked
# ---------------------------------------------------------------------------
def test_rule5_write_failure_propagates_and_leaves_checkpoint_unmarked(
    recording_client,
    recording_writer,
    recording_checkpoint,
    sample_single_table_payload,
):
    """When ``writer.write`` raises, ``run`` re-raises and marks nothing.

    Rule 5 requires that ``mark_completed`` run only *downstream of a
    successful write*, so a failing write must leave the checkpoint entirely
    unmarked and the next run must retry the same key. A success-path ordering
    check cannot tell "mark after write" apart from "mark regardless of write";
    only this negative case can. ``RecordingWriter(raise_on=...)`` raises
    ``RuntimeError("synthetic write failure for 'teams'")`` instead of
    recording, and because Rule 6 fail-safe wrapping is scoped exclusively to
    :mod:`pipelines.ingest_games`, the teams pipeline must let that exception
    escape rather than absorb it.

    Mutations detected
    ------------------
    * Moving ``checkpoint.mark_completed`` ABOVE ``writer.write``: the pipeline
      would checkpoint work that was never persisted, and every resumed run
      would then skip that season forever while producing no artifact at
      all.
    * Swallowing the write failure and returning normally (Rule 6's per-entity
      guard misapplied to a single-shot pipeline — catching the exception and
      returning before the counter and the checkpoint mark): the caller would
      see a clean return instead of the failure, and ``pytest.raises`` would
      report that no exception was raised.
    * Recording a write record despite the failure — caught by the empty
      ``writer.writes`` assertion, which is what ties "nothing persisted" to
      "nothing checkpointed".
    * Hoisting ``met.inc("pipeline_rows_written_total", ...)`` ABOVE
      ``writer.write``: the counter would report rows as persisted for a write
      that never happened, so ``pipeline_rows_written_total`` would drift
      permanently above the rows actually on disk and every operator dashboard
      built on it would over-report. The exception, writer and checkpoint
      assertions all still pass under that mutation — only an injected metrics
      sink can see it, which is why one is supplied here even though the
      failure path is expected to emit nothing.
    """
    # --- Arrange -------------------------------------------------------
    # ``raise_on`` is passed by keyword so the armed artifact is explicit at the
    # call site. It must equal the artifact name the pipeline writes, otherwise
    # the write succeeds and this negative test silently becomes a no-op.
    client = recording_client(responses={_ENDPOINT_LABEL: sample_single_table_payload})
    writer = recording_writer(raise_on=config.CSV_TEAMS)
    checkpoint = recording_checkpoint()
    # An explicit sink is injected on the FAILURE path precisely because the
    # expected emission count is zero: without it ``run`` would fall back to the
    # real module-level registry and the metric leg of Rule 5 would go
    # unobserved, leaving the write-then-count ordering untested.
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    with pytest.raises(RuntimeError) as excinfo:
        ingest_teams.run(
            client=client,
            writer=writer,
            checkpoint=checkpoint,
            season=_SEASON,
            metrics=metrics_mock,
        )

    # --- Assert: the writer's own failure is what escaped --------------
    assert "synthetic write failure" in str(excinfo.value), (
        "the RuntimeError propagating out of run() must be the writer's "
        "synthetic failure — not an unrelated error raised earlier in the "
        f"pipeline; got {str(excinfo.value)!r}"
    )

    # --- Assert: nothing was persisted ---------------------------------
    assert writer.writes == [], (
        f"a raising write must record no artifact at all; observed {writer.writes!r}"
    )

    # --- Assert: Rule 5 — nothing was checkpointed ---------------------
    assert checkpoint.marks == [], (
        "mark_completed MUST NOT run when the write failed, otherwise a resumed "
        "run would permanently skip work that was never persisted; observed "
        f"marks={checkpoint.marks!r}"
    )

    # --- Assert: nothing was counted as written ------------------------
    # The row-written counter is emitted from a single production call site that
    # sits strictly BETWEEN the write and the mark, so a write that raised must
    # leave the series completely empty — zero emissions, not a zero-valued one.
    # This is the metric-side twin of the marks assertion above: "nothing
    # persisted" must also mean "nothing reported".
    row_incs = [
        c
        for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == "pipeline_rows_written_total"
    ]
    assert row_incs == [], (
        "a failed write must produce ZERO pipeline_rows_written_total emissions "
        "— the counter sits after writer.write, so any emission here means rows "
        "were reported as persisted that never reached disk; observed "
        f"{len(row_incs)} increment(s): {row_incs!r}"
    )
    # ``pipeline_rows_written_total`` is the ONLY counter this pipeline emits and
    # its single call site is downstream of the write, so the sink must be
    # untouched in its entirety. Asserting the whole call list — rather than only
    # the name-filtered slice — additionally catches a mutation that renamed the
    # counter while keeping the premature emission, which the filtered assertion
    # above would silently miss.
    assert metrics_mock.inc.call_args_list == [], (
        "the teams pipeline emits exactly one counter and it is downstream of "
        "the write, so a failed write must leave the metrics sink entirely "
        f"untouched; observed {metrics_mock.inc.call_args_list!r}"
    )
