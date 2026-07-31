"""Value-exact aggregation contracts for :mod:`pipelines.ingest_games` (F-011).

What this module pins
---------------------
The Games pipeline is the only pipeline that *aggregates*: it accumulates
one DataFrame per game into an in-memory buffer and re-writes the **full**
concatenated artifact after every game (``pipelines/ingest_games.py``
lines 794-799), then increments ``pipeline_rows_written_total`` with the
**per-game** row count -- never the cumulative frame length (lines
802-811). This module asserts that arithmetic exactly:

* the ordered cumulative ``games.csv`` write sizes ``[2, 5, 7]`` and
  ``play_by_play.csv`` write sizes ``[4, 5, 8]``, plus the interleaved
  ``games, pbp, games, pbp, games, pbp`` artifact-name order;
* the ordered ``pipeline_rows_written_total`` increment sequence
  ``[2, 4, 3, 1, 2, 3]`` and its two per-artifact partitions
  ``[2, 3, 2]`` and ``[4, 1, 3]``;
* row-count conservation across the concatenation -- exactly ``7``
  box-score rows and ``8`` play-by-play rows;
* the combined frame's ``PTS`` total ``189``, its per-``GAME_ID`` row
  distribution ``[2, 3, 2]``, its distinct game/player counts ``3``/``4``,
  its shape ``(7, 5)``, and its exact ordered column list
  ``['season', 'GAME_ID', 'PLAYER_ID', 'TEAM_ID', 'PTS']``;
* one ordered ``checkpoint.mark_completed`` per game and exactly one
  ``get_pending`` probe carrying the full enumerated tuple;
* the zero-enumerated-games short circuit -- no write, no mark, no metric
  increment, and no upstream fetch;
* the degenerate-frame boundary -- a zero-row payload still produces a
  write, still checkpoints, still increments the counter with ``n=0``, and
  is **not** swallowed by the Rule 6 handler.

Two of those contracts are observable only in the log stream, so a
handwritten :class:`logging.LoggerAdapter` spy is injected alongside the
collaborator spies: the *terminal event* that proves the zero-game run
took the skip branch (see :class:`_LoggerSpy` for why the collaborator
spies cannot see it), and the per-game ``box_rows`` / ``pbp_rows`` event
that independently witnesses the counts the row counter reports.

Why a separate sibling module
-----------------------------
``tests/unit/pipelines/test_ingest_games.py`` is ~1,800 lines across nine
tests and houses the mandatory Rule 6 canary suite. Its aggregation
assertions are frozen (see below), so the sensitivity this module adds
cannot be delivered by editing them. A focused sibling keeps one concern
reviewable in isolation and is collected automatically because
``pytest.ini`` sets ``python_files = test_*.py``.

The verified gap this module closes
-----------------------------------
The pre-existing happy-path test validates the aggregation with two
assertion shapes that a plausible bug survives:

1. a **monotonicity** check, ``assert w["rows"] >= prev_games_rows``
   (``test_ingest_games.py`` lines 393-406); and
2. a **presence-and-positivity** check, ``assert "n" in c.kwargs``
   followed by ``assert c.kwargs["n"] > 0`` (lines 469-476).

Both hold under a mutation that replaces the cumulative
``pd.concat(buffer, ...)`` with "write only the latest frame", and both
hold under a mutation that emits ``len(combined_games)`` instead of
``len(bs_df)``. They are blind because the fixtures those tests use
deliver **equal** per-game counts (2 box-score and 3 play-by-play rows
for every game), which makes the correct and the mutated sequences
numerically indistinguishable.

Both mutations were applied to a scratch copy of the pipeline to confirm
the gap rather than assume it. The "cumulative counts" mutation survives
the **entire** pre-existing offline suite. The "latest frame only"
mutation survives the pre-existing happy-path test as described, and is
caught elsewhere only incidentally -- by the *resume* test, whose
history-preservation assertions it also breaks. This module closes both
directly, in the aggregation path itself.

Why the per-game row counts are deliberately unequal
----------------------------------------------------
The canonical mini-season fixture contributes **2 / 3 / 2** box-score
rows and **4 / 1 / 3** play-by-play rows. That asymmetry is the whole
point: with unequal counts every cumulative size is unique, so
"write only the latest frame" yields ``[2, 3, 2]`` / ``[4, 1, 3]``
instead of ``[2, 5, 7]`` / ``[4, 5, 8]``, and emitting cumulative
lengths yields ``[2, 4, 5, 5, 7, 8]`` instead of ``[2, 4, 3, 1, 2, 3]``.
Every wrong implementation produces a different, immediately visible
sequence. The counts must not be normalised, equalised, or reordered.

The hand-derived arithmetic, in full
------------------------------------
========================================  =========================  ==============
Quantity                                  Derivation                 Expected
========================================  =========================  ==============
Cumulative ``games.csv`` write sizes      2 ; 2+3=5 ; 5+2=7          [2, 5, 7]
Cumulative ``play_by_play.csv`` sizes     4 ; 4+1=5 ; 5+3=8          [4, 5, 8]
Box-score row conservation                2+3+2                      7
Play-by-play row conservation             4+1+3                      8
Row-written increments, in order          (2,4), (3,1), (2,3)        [2,4,3,1,2,3]
Games-only increments                     per-game box counts        [2, 3, 2]
PBP-only increments                       per-game pbp counts        [4, 1, 3]
``PTS`` total, by game                    58+78+53                   189
``PTS`` cross-check, by player            55+63+31+40                189
Rows per ``GAME_ID`` in final frame       one entry per game         [2, 3, 2]
Distinct games / distinct players         --                         3 / 4
Final frame shape                         7 rows; 4+1 columns        (7, 5)
Total writes / total marks                2x3 ; 1x3                  6 / 3
========================================  =========================  ==============

The two ``PTS`` derivations are independent and must agree. By game:
G1 30+28=58 ; G2 25+31+22=78 ; G3 35+18=53 ; 58+78+53=189. By player:
Jokic 30+25=55 ; Doncic 28+35=63 ; Tatum 31 ; Curry 22+18=40 ;
55+63+31+40=189.

Hand-derived, never captured
----------------------------
Every expected value above follows from the fixture rows alone and was
computed on paper before this module ran. Nothing here is a snapshot of
observed output: no ``pandas.testing.assert_frame_equal`` against a
produced frame, no golden CSV, no record-then-compare helper. The
``EXPECTED_*`` constants below carry their arithmetic in an adjacent
comment so a reviewer can verify every number without leaving the file.
Expected values are kept strictly separate from the fixture *input*
values, which live in ``tests/conftest.py`` and are consumed as-is.

Tier and markers
----------------
This module carries **no** pytest marker, so it runs on the default
offline tier. ``pytest.ini`` registers exactly two markers
(``integration``, ``invariant``) and ``--strict-markers`` turns any
third into a hard collection error. Every collaborator is a spy, so
there is no network access, no sleep, and no wall-clock read.

Monkey-patch target
-------------------
:mod:`pipelines.ingest_games` performs ``from endpoints.schedule import
enumerate_game_ids`` at *module scope*, binding the name into its own
namespace at import time. The only effective seam is therefore

    monkeypatch.setattr(
        "pipelines.ingest_games.enumerate_game_ids",
        lambda client, season: [...],
    )

Patching ``endpoints.schedule.enumerate_game_ids`` would have no effect.
Because enumeration is patched out, this module is independent of the
canonical-``GAME_ID`` behaviour of ``endpoints/schedule.py``.

Rule 6 is deliberately NOT re-tested here
-----------------------------------------
Per-game failure isolation is already covered by
``test_ingest_games.py::TestRule6FailSafe``. This module adds no new
Rule 6 error case; it only asserts, in the boundary tests, that
``games_failed_total`` was **never** incremented -- which is what proves
a degenerate-but-valid payload flowed through the happy path instead of
being swallowed by the fail-safe handler.

There is no literal "zero-game divisor" in this codebase
--------------------------------------------------------
Stated plainly rather than papered over: there is no division, ``mean``,
``groupby`` aggregation, or ``agg`` call site anywhere in ``api/``,
``endpoints/``, ``pipelines/``, ``storage/``, ``utils/``, ``config.py``,
or ``run.py`` -- the only aggregation primitives are the two
``pd.concat`` calls this module targets. So there is no divisor to drive
to zero, and none is invented here. The requirement is honoured through
faithful analogues, of which this module supplies three: the
zero-enumerated-games short circuit (the loop body never executes),
cumulative row-count conservation (exactly 7 and 8), and per-game versus
cumulative counter semantics (the ``[2, 4, 3, 1, 2, 3]`` sequence).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest.mock import MagicMock

import pandas as pd
import pytest

import config
from pipelines import ingest_games
from tests.conftest import RecordingClient


# ---------------------------------------------------------------------------
# Module-level constants -- fixture-facing
# ---------------------------------------------------------------------------

#: Season string threaded through every ``ingest_games.run`` call in this
#: module. Asserted indirectly via ``writer.writes[*]["season"]``.
_SEASON = "2025-26"

#: Endpoint name for the traditional box score. Appears as ``calls[i][0]``
#: in :class:`RecordingClient.calls` -- see ``tests/conftest.py``.
_BOXSCORE_ENDPOINT = "boxscoretraditionalv2"

#: Endpoint name for the play-by-play endpoint.
_PLAYBYPLAY_ENDPOINT = "playbyplayv2"

#: Verbatim per-artifact row counter emitted after every successful game
#: (``pipelines/ingest_games.py`` lines 802-811).
_ROWS_WRITTEN_COUNTER = "pipeline_rows_written_total"

#: Verbatim Rule 6 failure counter (``pipelines/ingest_games.py`` line
#: 851). The boundary tests assert it was never incremented.
_GAMES_FAILED_COUNTER = "games_failed_total"

#: The ``pipeline`` label value both counters carry. The label *names*
#: (``pipeline``/``artifact``, not ``domain``/``file``) are the documented
#: operator contract.
_PIPELINE_LABEL = "ingest_games"

#: Log-message prefix of the per-game completion event emitted once per
#: successfully processed game (``pipelines/ingest_games.py`` line 820).
_GAME_COMPLETE_PREFIX = "pipeline.games.game_complete"

#: Log-message prefix shared by BOTH terminal events -- the skip variant
#: (line 736) and the processed/failed variant (line 854). Filtering on the
#: shared prefix is what lets a test assert *which* branch terminated the
#: run rather than merely that some completion event was emitted.
_PIPELINE_COMPLETE_PREFIX = "pipeline.complete"


# ---------------------------------------------------------------------------
# Module-level constants -- hand-derived EXPECTED values
# ---------------------------------------------------------------------------
#
# Every constant below is derived from the mini-season fixture rows alone.
# The fixture contributes 2 / 3 / 2 box-score rows and 4 / 1 / 3
# play-by-play rows across games G1 / G2 / G3, in that enumeration order.

#: Total ``writer.write`` calls: 2 artifacts x 3 games = 6.
EXPECTED_TOTAL_WRITES = 6

#: Cumulative games-CSV write sizes. The pipeline re-writes the FULL
#: buffer after every game, so the sizes are running totals:
#: 2 ; 2+3=5 ; 5+2=7. A "write only the latest frame" mutation would
#: instead produce the per-game counts [2, 3, 2].
EXPECTED_CUMULATIVE_GAMES_ROWS = [2, 5, 7]

#: Cumulative play-by-play write sizes: 4 ; 4+1=5 ; 5+3=8. The "latest
#: frame only" mutation would instead produce [4, 1, 3].
EXPECTED_CUMULATIVE_PBP_ROWS = [4, 5, 8]

#: ``pipeline_rows_written_total`` ``n=`` values in emission order. Each
#: value is the PER-GAME row count, never the cumulative frame length:
#: the pipeline emits (games, pbp) once per iteration -> (2,4), (3,1),
#: (2,3). A mutation emitting the cumulative length would instead read
#: [2, 4, 5, 5, 7, 8].
EXPECTED_ROW_INCREMENTS = [2, 4, 3, 1, 2, 3]

#: The games-CSV partition of the increment sequence: 2 ; 3 ; 2.
EXPECTED_GAMES_INCREMENTS = [2, 3, 2]

#: The play-by-play partition of the increment sequence: 4 ; 1 ; 3.
EXPECTED_PBP_INCREMENTS = [4, 1, 3]

#: Row-count conservation for the box score: 2+3+2 = 7.
EXPECTED_BOX_ROW_TOTAL = 7

#: Row-count conservation for the play-by-play: 4+1+3 = 8.
EXPECTED_PBP_ROW_TOTAL = 8

#: ``PTS`` total of the final combined frame.
#: By game:   G1 30+28=58 ; G2 25+31+22=78 ; G3 35+18=53 ; 58+78+53 = 189.
#: By player: Jokic 30+25=55 ; Doncic 28+35=63 ; Tatum 31 ;
#:            Curry 22+18=40 ; 55+63+31+40 = 189. The two agree.
EXPECTED_PTS_TOTAL = 189

#: Rows per ``GAME_ID`` inside the final combined frame, in enumeration
#: order: G1 2 ; G2 3 ; G3 2. Summing them reproduces
#: ``EXPECTED_BOX_ROW_TOTAL`` (2+3+2 = 7).
EXPECTED_ROWS_PER_GAME = [2, 3, 2]

#: Distinct ``GAME_ID`` values in the final combined frame: G1, G2, G3.
EXPECTED_DISTINCT_GAMES = 3

#: Distinct ``PLAYER_ID`` values: Jokic, Doncic, Tatum, Curry.
EXPECTED_DISTINCT_PLAYERS = 4

#: Final combined games frame shape: 2+3+2 = 7 rows; the 4 payload
#: columns plus the inserted ``season`` = 5 columns.
EXPECTED_FINAL_SHAPE = (7, 5)

#: Final combined games frame columns. ``season`` is inserted at index 0
#: because no case variant of it is present; ``game_id`` is NOT injected
#: because ``GAME_ID`` already matches ``c.lower() == "game_id"``, so the
#: payload's own column survives verbatim in its original position.
EXPECTED_FINAL_COLUMNS = ["season", "GAME_ID", "PLAYER_ID", "TEAM_ID", "PTS"]

#: Final combined play-by-play frame columns, by the same rule: 3 payload
#: columns plus the inserted ``season``.
EXPECTED_PBP_COLUMNS = ["season", "GAME_ID", "EVENTNUM", "EVENTDESC"]

#: One ``mark_completed`` per successfully processed game: 3 games -> 3.
EXPECTED_TOTAL_MARKS = 3

#: Columns of the degenerate boundary frame. A result set declaring
#: ``headers: []`` with ``rowSet: []`` normalises to a genuine 0x0 frame,
#: so BOTH insertion branches fire: ``season`` at index 0, then
#: ``game_id`` at index 1.
EXPECTED_DEGENERATE_COLUMNS = ["season", "game_id"]

#: Degenerate boundary frame shape: 0 rows; 0 payload columns + 2
#: inserted key columns = 2.
EXPECTED_DEGENERATE_SHAPE = (0, 2)

#: Columns of the header-only boundary frame: 2 declared payload headers
#: preceded by the 2 inserted key columns.
EXPECTED_HEADER_ONLY_COLUMNS = ["season", "game_id", "PLAYER_ID", "PTS"]

#: Header-only boundary frame shape: 0 rows; 2 payload + 2 inserted = 4.
EXPECTED_HEADER_ONLY_SHAPE = (0, 4)

#: Row-written increments for a single-game boundary run: the degenerate
#: box-score frame contributes 0 rows and G1's play-by-play contributes 4,
#: so the counter must still fire twice, as ``n=0`` then ``n=4``. A
#: mutation that skipped the counter for an empty frame would yield [4].
EXPECTED_BOUNDARY_INCREMENTS = [0, 4]

#: The ordered ``(box_rows, pbp_rows)`` pairs the pipeline logs once per
#: completed game: (2,4), (3,1), (2,3). This is an INDEPENDENT witness of
#: the very same per-game counts the row-written counter reports, so a
#: mutation that corrupted only one of the two surfaces still fails.
EXPECTED_GAME_COMPLETE_ROWS = [(2, 4), (3, 1), (2, 3)]

#: The single terminal completion event of a zero-enumerated-games run,
#: as an ordered ``(format string, args)`` pair. The pipeline reaches this
#: line ONLY through the ``if not pending:`` guard; the alternative
#: terminal event carries ``processed=%d failed=%d`` instead. Asserting
#: this exact pair is therefore what proves the guard fired, because with
#: an empty pending list the loop body would not execute either way and
#: the spies alone cannot tell the two branches apart.
EXPECTED_SKIP_COMPLETION_EVENTS = [
    (
        "pipeline.complete domain=%s season=%s status=skipped"
        " reason=all_checkpointed",
        (config.DOMAIN_GAMES, _SEASON),
    ),
]


# ---------------------------------------------------------------------------
# Module-local helpers
# ---------------------------------------------------------------------------


def _patch_mini_season_enumerate(
    monkeypatch: pytest.MonkeyPatch,
    game_ids: Iterable[str],
) -> None:
    """Patch :func:`pipelines.ingest_games.enumerate_game_ids`.

    Replaces the module-level import with a stub returning
    ``list(game_ids)`` regardless of its arguments, so the aggregation
    behaviour can be exercised without involving schedule enumeration.

    The target path is ``pipelines.ingest_games.enumerate_game_ids`` and
    **not** ``endpoints.schedule.enumerate_game_ids``: the pipeline binds
    the name into its own namespace at import time, so patching the
    defining module would have no effect at all.

    Parameters
    ----------
    monkeypatch:
        The pytest ``monkeypatch`` fixture, scoped to the invoking test;
        the patch is undone automatically on teardown.
    game_ids:
        The identifiers to enumerate, in the order the pipeline should
        iterate them. Pass an empty sequence to exercise the
        zero-enumerated-games short circuit.
    """
    ids: List[str] = list(game_ids)
    monkeypatch.setattr(
        "pipelines.ingest_games.enumerate_game_ids",
        lambda client, season: list(ids),
    )


def _writes_named(writer: Any, name: str) -> List[Dict[str, Any]]:
    """Return the ordered ``writer.writes`` records for one artifact.

    Filtering preserves emission order, which is what makes the cumulative
    size assertions meaningful: the returned list's ``rows`` values are the
    running totals for that artifact alone, with the other artifact's
    interleaved writes removed.
    """
    return [w for w in writer.writes if w["name"] == name]


def _last_frame_named(writer: Any, name: str) -> pd.DataFrame:
    """Return the DataFrame snapshot of the LAST write for one artifact.

    ``RecordingWriter`` records ``df.copy()`` at write time, so the frame
    returned here is the value the pipeline actually handed to the writer
    and cannot be retroactively altered by later pipeline work. Every
    assertion made against it is read-only, so no pandas
    ``SettingWithCopyWarning`` can be provoked under
    ``filterwarnings = error``.
    """
    named = _writes_named(writer, name)
    if not named:
        raise AssertionError(
            f"expected at least one {name!r} write to read a frame from; "
            f"recorded artifact names: {[w['name'] for w in writer.writes]!r}"
        )
    return named[-1]["df"]


def _counter_calls(metrics_mock: MagicMock, counter: str) -> List[Any]:
    """Return the ordered ``metrics.inc`` calls for one counter name.

    The production call shape is ``inc(<name>, <labels dict>, n=<int>)``:
    the counter name is ``call.args[0]``, the label mapping is passed
    POSITIONALLY as ``call.args[1]``, and the increment value arrives as
    the ``n`` keyword. Filtering on ``call.args[0]`` therefore isolates a
    single counter without touching the labels.
    """
    return [
        c
        for c in metrics_mock.inc.call_args_list
        if c.args and c.args[0] == counter
    ]


def _row_written_increments(
    metrics_mock: MagicMock,
    artifact: Optional[str] = None,
) -> List[int]:
    """Ordered ``n=`` values of every ``pipeline_rows_written_total`` call.

    With ``artifact=None`` the full interleaved sequence is returned; pass
    ``config.CSV_GAMES`` or ``config.CSV_PLAY_BY_PLAY`` to obtain that
    artifact's partition. The label mapping is compared in full (not by
    membership) so a mutation that renamed or dropped a label surfaces as
    an empty partition rather than passing silently.
    """
    calls = _counter_calls(metrics_mock, _ROWS_WRITTEN_COUNTER)
    if artifact is not None:
        expected_labels = {
            "pipeline": _PIPELINE_LABEL,
            "artifact": f"{artifact}.csv",
        }
        calls = [c for c in calls if len(c.args) > 1 and c.args[1] == expected_labels]
    return [c.kwargs["n"] for c in calls]


def _zero_row_boxscore_payload(
    game_id: str,
    headers: List[str],
) -> Dict[str, Any]:
    """Build a ``boxscoretraditionalv2`` envelope whose ``rowSet`` is empty.

    This is the *reachable* route to a degenerate primary frame through the
    pipeline's public surface. The naive ``{"resultSets": []}`` shape is
    not usable: ``utils.schema_normalizer.normalize_result_sets`` raises
    ``ValueError`` for it, and the pipeline's Rule 6 handler would swallow
    that exception, producing no write at all. A table that declares a
    ``name`` but an empty ``rowSet`` passes validation and normalises to a
    frame with ``len(headers)`` columns and zero rows, which is exactly
    what the boundary tests need.

    Parameters
    ----------
    game_id:
        The ``GAME_ID`` this envelope answers for; echoed into
        ``parameters`` so the envelope reads like a real response.
    headers:
        The declared column names. Pass ``[]`` for the 0x0 case and a
        non-empty list for the header-only case.
    """
    return {
        "resource": _BOXSCORE_ENDPOINT,
        "parameters": {"GameID": game_id},
        "resultSets": [
            {
                "name": "PlayerStats",
                "headers": list(headers),
                "rowSet": [],
            }
        ],
    }


class _LoggerSpy(logging.LoggerAdapter):
    """Handwritten adapter recording the pipeline's structured event stream.

    Two of this module's contracts are observable *only* in the log stream,
    because the collaborator spies cannot see them:

    * the zero-enumerated-games run terminates through the ``status=skipped
      reason=all_checkpointed`` branch rather than by falling through an
      empty loop -- with no pending games the loop body does not execute
      either way, so writes, marks, and metrics are identically empty in
      both cases; and
    * the per-game ``box_rows`` / ``pbp_rows`` values, which are a second,
      independent witness of the counts the row-written counter reports.

    A handwritten :class:`logging.LoggerAdapter` subclass is used rather
    than ``caplog`` or a :class:`~unittest.mock.MagicMock`, matching the
    established precedent in ``test_ingest_players.py``: it honours the
    handwritten-spy directive, it is immune to the autouse logger-handler
    reset (this spy never touches the real logging tree), and it satisfies
    the ``logger`` parameter's declared type. Propagation is disabled on the
    underlying logger so nothing leaks into captured test output.

    Records are ``(msg, args)`` pairs holding the **unformatted** format
    string and its positional arguments, so assertions compare the exact
    operator contract without re-implementing ``%``-interpolation. Methods
    not overridden here fall through to the real adapter, which is inert
    because propagation is off.
    """

    def __init__(self) -> None:
        underlying = logging.getLogger(
            "tests.pipelines.ingest_games_aggregation.spy"
        )
        underlying.propagate = False
        super().__init__(underlying, {})
        self.info_records: List[Tuple[str, Tuple[Any, ...]]] = []
        self.warning_records: List[Tuple[str, Tuple[Any, ...]]] = []

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        """Record an INFO event verbatim instead of emitting it."""
        self.info_records.append((str(msg), tuple(args)))

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        """Record a WARNING event verbatim instead of emitting it."""
        self.warning_records.append((str(msg), tuple(args)))

    def info_events_starting(
        self,
        prefix: str,
    ) -> List[Tuple[str, Tuple[Any, ...]]]:
        """Return the ordered INFO events whose format string starts with ``prefix``."""
        return [
            record for record in self.info_records if record[0].startswith(prefix)
        ]


class _MiniSeasonClient(RecordingClient):
    """Handwritten client spy that routes on ``params["GameID"]``.

    :class:`RecordingClient` keys its responses by *endpoint*, which cannot
    express "a different box score per game". This subclass adds
    ``GameID`` routing on top of it while inheriting the recording
    behaviour verbatim, so ``.calls`` keeps the exact ``(endpoint, params)``
    shape every other pipeline assertion in the suite relies on.

    Subclassing the handwritten spy -- rather than reaching for a
    :class:`~unittest.mock.MagicMock` -- is deliberate: an interface drift
    in the production client surfaces at instantiation time here instead of
    being masked by attribute-access magic. Both endpoint helpers set
    ``params["GameID"] = str(game_id)``, so one routing key serves both.

    An unmapped endpoint or an unmapped ``GameID`` raises
    :class:`AssertionError` rather than falling back to a synthetic
    envelope: a silent fallback would let a routing bug feed plausible but
    wrong row counts into the aggregation assertions.
    """

    def __init__(
        self,
        boxscore_payloads: Dict[str, Any],
        playbyplay_payloads: Dict[str, Any],
    ) -> None:
        super().__init__()
        self.boxscore_payloads: Dict[str, Any] = dict(boxscore_payloads)
        self.playbyplay_payloads: Dict[str, Any] = dict(playbyplay_payloads)

    def get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Record the call on ``.calls``, then route by endpoint and GameID."""
        # Delegate to the base spy purely for its recording side effect;
        # its endpoint-keyed return value is discarded because this
        # subclass resolves the response by ``GameID`` instead.
        super().get(endpoint, params)
        if endpoint == _BOXSCORE_ENDPOINT:
            envelopes = self.boxscore_payloads
        elif endpoint == _PLAYBYPLAY_ENDPOINT:
            envelopes = self.playbyplay_payloads
        else:
            raise AssertionError(
                f"_MiniSeasonClient routes only {_BOXSCORE_ENDPOINT!r} and "
                f"{_PLAYBYPLAY_ENDPOINT!r}; got endpoint={endpoint!r}"
            )
        game_id = (params or {}).get("GameID")
        if game_id not in envelopes:
            raise AssertionError(
                f"_MiniSeasonClient has no {endpoint!r} envelope for "
                f"GameID={game_id!r}; mapped ids: {sorted(envelopes)!r}"
            )
        return envelopes[game_id]


# ---------------------------------------------------------------------------
# Test A -- cumulative concatenation produces running row totals
# ---------------------------------------------------------------------------


def test_cumulative_concat_rewrites_running_row_totals_for_both_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_boxscore_payloads: Dict[str, Any],
    mini_season_playbyplay_payloads: Dict[str, Any],
    mini_season_game_ids: List[str],
) -> None:
    """Cumulative write sizes are exactly [2, 5, 7] and [4, 5, 8], in that order.

    Mutation detected: replacing ``pd.concat(games_buffer, ...)`` with the
    single per-game frame ("write only the latest frame") yields the
    per-game counts [2, 3, 2] and [4, 1, 3] instead of the running totals
    2/2+3=5/5+2=7 and 4/4+1=5/5+3=8. Both sequences are non-decreasing, so
    the pre-existing monotonicity check cannot separate them -- only the
    fixture's deliberately unequal per-game counts can. A dropped or
    double-appended frame, or a swap of the two ``writer.write`` calls
    inside one iteration, is caught by the same assertions.
    """
    # --- Arrange -------------------------------------------------------
    _patch_mini_season_enumerate(monkeypatch, mini_season_game_ids)
    client = _MiniSeasonClient(
        boxscore_payloads=mini_season_boxscore_payloads,
        playbyplay_payloads=mini_season_playbyplay_payloads,
    )
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_games.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: write count guard ------------------------------------
    # 2 artifacts x 3 games = 6 writes; indexing below depends on it.
    assert len(writer.writes) == EXPECTED_TOTAL_WRITES, (
        f"expected {EXPECTED_TOTAL_WRITES} writer.write calls "
        f"(2 artifacts x {len(mini_season_game_ids)} games); "
        f"got {len(writer.writes)}: {[w['name'] for w in writer.writes]!r}"
    )

    # --- Assert: interleaved artifact order ---------------------------
    # Each iteration writes games first, then play-by-play, so the whole
    # ordered name sequence is that pair repeated once per game.
    expected_names = [
        config.CSV_GAMES,
        config.CSV_PLAY_BY_PLAY,
    ] * len(mini_season_game_ids)
    observed_names = [w["name"] for w in writer.writes]
    assert observed_names == expected_names, (
        f"artifact writes must interleave as {expected_names!r} "
        f"(games then play-by-play, once per game); got {observed_names!r}"
    )

    # --- Assert: cumulative row totals per artifact -------------------
    games_sizes = [w["rows"] for w in _writes_named(writer, config.CSV_GAMES)]
    pbp_sizes = [w["rows"] for w in _writes_named(writer, config.CSV_PLAY_BY_PLAY)]
    assert games_sizes == EXPECTED_CUMULATIVE_GAMES_ROWS, (
        f"cumulative games-CSV write sizes must be "
        f"{EXPECTED_CUMULATIVE_GAMES_ROWS} (2 ; 2+3=5 ; 5+2=7); "
        f"got {games_sizes}"
    )
    assert pbp_sizes == EXPECTED_CUMULATIVE_PBP_ROWS, (
        f"cumulative play-by-play write sizes must be "
        f"{EXPECTED_CUMULATIVE_PBP_ROWS} (4 ; 4+1=5 ; 5+3=8); "
        f"got {pbp_sizes}"
    )

    # --- Assert: season propagated to every write ---------------------
    expected_seasons = [_SEASON] * EXPECTED_TOTAL_WRITES
    observed_seasons = [w["season"] for w in writer.writes]
    assert observed_seasons == expected_seasons, (
        f"every write must carry season={_SEASON!r}; got {observed_seasons!r}"
    )


# ---------------------------------------------------------------------------
# Test B -- the row-written counter carries PER-GAME deltas (headline test)
# ---------------------------------------------------------------------------


def test_row_written_counter_emits_per_game_deltas_in_emission_order(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_boxscore_payloads: Dict[str, Any],
    mini_season_playbyplay_payloads: Dict[str, Any],
    mini_season_game_ids: List[str],
) -> None:
    """``pipeline_rows_written_total`` emits exactly [2, 4, 3, 1, 2, 3].

    Mutation detected: emitting ``n=len(combined_games)`` /
    ``n=len(combined_pbp)`` instead of ``n=len(bs_df)`` / ``n=len(pbp_df)``
    turns the sequence into the cumulative [2, 4, 5, 5, 7, 8]. The
    pre-existing ``assert "n" in c.kwargs`` presence-and-positivity check
    is satisfied by both, so this is the single most important assertion in
    the module. Renaming or dropping either label empties the per-artifact
    partitions, and reordering the two ``inc`` calls within an iteration
    breaks the interleaved sequence. The per-game log event is asserted
    alongside as an independent witness, so a mutation that corrupted only
    one of the two observability surfaces is still caught.
    """
    # --- Arrange -------------------------------------------------------
    _patch_mini_season_enumerate(monkeypatch, mini_season_game_ids)
    client = _MiniSeasonClient(
        boxscore_payloads=mini_season_boxscore_payloads,
        playbyplay_payloads=mini_season_playbyplay_payloads,
    )
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()
    logger_spy = _LoggerSpy()

    # --- Act -----------------------------------------------------------
    ingest_games.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        logger=logger_spy,
        metrics=metrics_mock,
    )

    # --- Assert: full interleaved increment sequence ------------------
    # (games, pbp) per iteration -> (2,4), (3,1), (2,3).
    all_increments = _row_written_increments(metrics_mock)
    assert all_increments == EXPECTED_ROW_INCREMENTS, (
        f"row-written increments must be the PER-GAME counts "
        f"{EXPECTED_ROW_INCREMENTS} (interleaved (2,4), (3,1), (2,3)); "
        f"the cumulative-length mutation reads [2, 4, 5, 5, 7, 8]; "
        f"got {all_increments}"
    )

    # --- Assert: per-artifact partitions ------------------------------
    games_increments = _row_written_increments(metrics_mock, config.CSV_GAMES)
    pbp_increments = _row_written_increments(metrics_mock, config.CSV_PLAY_BY_PLAY)
    assert games_increments == EXPECTED_GAMES_INCREMENTS, (
        f"games-CSV increments must be the per-game box-score counts "
        f"{EXPECTED_GAMES_INCREMENTS}; got {games_increments}"
    )
    assert pbp_increments == EXPECTED_PBP_INCREMENTS, (
        f"play-by-play increments must be the per-game event counts "
        f"{EXPECTED_PBP_INCREMENTS}; got {pbp_increments}"
    )

    # --- Assert: increments sum to the conserved row totals -----------
    # 2+3+2 = 7 box-score rows and 4+1+3 = 8 play-by-play rows, i.e. the
    # counter totals must agree with the final artifact sizes.
    assert sum(games_increments) == EXPECTED_BOX_ROW_TOTAL, (
        f"games-CSV increments must sum to {EXPECTED_BOX_ROW_TOTAL} "
        f"(2+3+2); got {sum(games_increments)} from {games_increments}"
    )
    assert sum(pbp_increments) == EXPECTED_PBP_ROW_TOTAL, (
        f"play-by-play increments must sum to {EXPECTED_PBP_ROW_TOTAL} "
        f"(4+1+3); got {sum(pbp_increments)} from {pbp_increments}"
    )

    # --- Assert: the per-game log is an independent witness ------------
    # ``pipeline.games.game_complete game_id=%s box_rows=%d pbp_rows=%d``
    # carries the same per-game counts, so the log and the counter must
    # agree: (G1, 2, 4), (G2, 3, 1), (G3, 2, 3).
    expected_game_events = [
        (gid, box_rows, pbp_rows)
        for gid, (box_rows, pbp_rows) in zip(
            mini_season_game_ids, EXPECTED_GAME_COMPLETE_ROWS,
        )
    ]
    observed_game_events = [
        args
        for _msg, args in logger_spy.info_events_starting(_GAME_COMPLETE_PREFIX)
    ]
    assert observed_game_events == expected_game_events, (
        f"the per-game completion log must report the PER-GAME row counts "
        f"{expected_game_events!r} (one event per game, in enumeration "
        f"order); got {observed_game_events!r}"
    )


# ---------------------------------------------------------------------------
# Test C -- the final combined frame conserves rows, points, and column order
# ---------------------------------------------------------------------------


def test_final_combined_frame_conserves_rows_points_and_column_order(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_boxscore_payloads: Dict[str, Any],
    mini_season_playbyplay_payloads: Dict[str, Any],
    mini_season_game_ids: List[str],
) -> None:
    """The last games write is a (7, 5) frame summing to 189 points.

    Mutation detected: any row loss or duplication inside the
    concatenation breaks the 2+3+2=7 conservation, the 58+78+53=189 points
    total, and the [2, 3, 2] per-``GAME_ID`` distribution simultaneously.
    Inserting ``game_id`` when ``GAME_ID`` is already present, or inserting
    ``season`` at the wrong index, shifts the exact ordered column list.
    Every assertion reads the recorded snapshot without mutating it, so no
    pandas warning can be provoked under ``filterwarnings = error``.
    """
    # --- Arrange -------------------------------------------------------
    _patch_mini_season_enumerate(monkeypatch, mini_season_game_ids)
    client = _MiniSeasonClient(
        boxscore_payloads=mini_season_boxscore_payloads,
        playbyplay_payloads=mini_season_playbyplay_payloads,
    )
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_games.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: shape and column order of the combined games frame ----
    games_df = _last_frame_named(writer, config.CSV_GAMES)
    assert games_df.shape == EXPECTED_FINAL_SHAPE, (
        f"final games frame must be {EXPECTED_FINAL_SHAPE} -- 2+3+2=7 rows "
        f"and 4 payload columns plus the inserted 'season'; "
        f"got {games_df.shape}"
    )
    observed_columns = list(games_df.columns)
    assert observed_columns == EXPECTED_FINAL_COLUMNS, (
        f"final games frame columns must be {EXPECTED_FINAL_COLUMNS} -- "
        f"'season' inserted at index 0 and 'game_id' NOT injected because "
        f"'GAME_ID' already matches case-insensitively; "
        f"got {observed_columns}"
    )

    # --- Assert: points total, derived two independent ways ------------
    # By game:   G1 30+28=58 ; G2 25+31+22=78 ; G3 35+18=53 ; total 189.
    # By player: Jokic 55 ; Doncic 63 ; Tatum 31 ; Curry 40 ; total 189.
    observed_pts = int(games_df["PTS"].sum())
    assert observed_pts == EXPECTED_PTS_TOTAL, (
        f"final games frame PTS must total {EXPECTED_PTS_TOTAL} "
        f"(58+78+53, cross-checked as 55+63+31+40); got {observed_pts}"
    )

    # --- Assert: per-GAME_ID row distribution, in enumeration order ----
    # Derived explicitly per identifier rather than through a groupby so
    # the ordering is stated by the comprehension itself and mirrors the
    # hand arithmetic 2 ; 3 ; 2 (which sums back to 7).
    rows_per_game = [
        int((games_df["GAME_ID"] == gid).sum()) for gid in mini_season_game_ids
    ]
    assert rows_per_game == EXPECTED_ROWS_PER_GAME, (
        f"rows per GAME_ID must be {EXPECTED_ROWS_PER_GAME} in enumeration "
        f"order {mini_season_game_ids}; got {rows_per_game}"
    )
    assert sum(rows_per_game) == EXPECTED_BOX_ROW_TOTAL, (
        f"the per-game distribution must account for all "
        f"{EXPECTED_BOX_ROW_TOTAL} rows (2+3+2); "
        f"got {sum(rows_per_game)} from {rows_per_game}"
    )

    # --- Assert: distinct key cardinality ------------------------------
    observed_games = int(games_df["GAME_ID"].nunique())
    observed_players = int(games_df["PLAYER_ID"].nunique())
    assert observed_games == EXPECTED_DISTINCT_GAMES, (
        f"final games frame must span {EXPECTED_DISTINCT_GAMES} distinct "
        f"GAME_IDs; got {observed_games}"
    )
    assert observed_players == EXPECTED_DISTINCT_PLAYERS, (
        f"final games frame must span {EXPECTED_DISTINCT_PLAYERS} distinct "
        f"PLAYER_IDs (Jokic, Doncic, Tatum, Curry); got {observed_players}"
    )

    # --- Assert: play-by-play conservation and column order ------------
    pbp_df = _last_frame_named(writer, config.CSV_PLAY_BY_PLAY)
    assert pbp_df.shape[0] == EXPECTED_PBP_ROW_TOTAL, (
        f"final play-by-play frame must hold {EXPECTED_PBP_ROW_TOTAL} rows "
        f"(4+1+3); got {pbp_df.shape[0]}"
    )
    observed_pbp_columns = list(pbp_df.columns)
    assert observed_pbp_columns == EXPECTED_PBP_COLUMNS, (
        f"final play-by-play frame columns must be {EXPECTED_PBP_COLUMNS}; "
        f"got {observed_pbp_columns}"
    )


# ---------------------------------------------------------------------------
# Test D -- one ordered checkpoint mark per game, one pending probe
# ---------------------------------------------------------------------------


def test_checkpoint_marks_once_per_game_in_enumeration_order(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_boxscore_payloads: Dict[str, Any],
    mini_season_playbyplay_payloads: Dict[str, Any],
    mini_season_game_ids: List[str],
) -> None:
    """Marks are the ordered per-game pairs and the pending probe fires once.

    Mutation detected: marking out of enumeration order, marking a game
    twice, hoisting ``mark_completed`` out of the loop, or probing
    ``get_pending`` per game instead of once at the top of the run. The
    whole ordered collection is compared -- not its length and not
    membership -- so a reordering that preserves the count still fails.
    """
    # --- Arrange -------------------------------------------------------
    _patch_mini_season_enumerate(monkeypatch, mini_season_game_ids)
    client = _MiniSeasonClient(
        boxscore_payloads=mini_season_boxscore_payloads,
        playbyplay_payloads=mini_season_playbyplay_payloads,
    )
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_games.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: ordered marks, one per game --------------------------
    expected_marks = [
        (config.DOMAIN_GAMES, gid) for gid in mini_season_game_ids
    ]
    assert checkpoint.marks == expected_marks, (
        f"expected one mark per game in enumeration order "
        f"{expected_marks!r}; got {checkpoint.marks!r}"
    )
    assert len(checkpoint.marks) == EXPECTED_TOTAL_MARKS, (
        f"expected exactly {EXPECTED_TOTAL_MARKS} marks (one per game); "
        f"got {len(checkpoint.marks)}"
    )

    # --- Assert: the single top-of-run pending probe -------------------
    # ``RecordingCheckpoint.get_pending`` records the domain plus the
    # stringified key tuple, so the whole probe list is comparable.
    expected_pendings = [(config.DOMAIN_GAMES, tuple(mini_season_game_ids))]
    assert checkpoint.pendings == expected_pendings, (
        f"expected exactly one get_pending probe {expected_pendings!r}; "
        f"got {checkpoint.pendings!r}"
    )


# ---------------------------------------------------------------------------
# Test E -- zero enumerated games: the aggregation loop never executes
# ---------------------------------------------------------------------------


def test_zero_enumerated_games_short_circuits_before_any_aggregation(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_boxscore_payloads: Dict[str, Any],
    mini_season_playbyplay_payloads: Dict[str, Any],
) -> None:
    """Zero enumerated games produces zero writes, marks, and increments.

    This is the closest structural analogue this codebase has to a
    "zero-game divisor": the aggregation loop body never executes, so
    every downstream quantity must be exactly empty rather than merely
    small. The pending probe still fires -- that is how the skip is
    decided -- and it records an EMPTY key tuple, which only happens when
    enumeration itself returned nothing.

    Mutations detected:

    * **Bypassing the checkpoint consult** (assigning the enumerated list
      straight to ``pending``) -- ``checkpoint.pendings`` becomes empty
      instead of carrying the one recorded probe.
    * **Returning before the consult** (an "optimisation" that skips
      ``get_pending`` when enumeration is empty) -- same assertion.
    * **Removing the ``if not pending: return`` guard** -- caught by the
      terminal-log-event assertion and *only* by it. This is a verified
      finding rather than an assumption: with an empty pending list the
      loop body does not execute either way, so writes, marks, and metric
      increments are identically empty in both the guarded and unguarded
      pipeline. The one observable difference is which terminal event is
      logged -- ``status=skipped reason=all_checkpointed`` versus
      ``processed=0 failed=0`` -- so the spy-only assertions below would
      pass against the mutant without it.
    """
    # --- Arrange -------------------------------------------------------
    # Enumeration returns nothing; the payload mappings are still supplied
    # so that any fetch attempt would succeed -- proving the absence of
    # fetches is the guard's doing, not a missing envelope.
    _patch_mini_season_enumerate(monkeypatch, [])
    client = _MiniSeasonClient(
        boxscore_payloads=mini_season_boxscore_payloads,
        playbyplay_payloads=mini_season_playbyplay_payloads,
    )
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()
    logger_spy = _LoggerSpy()

    # --- Act -----------------------------------------------------------
    ingest_games.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        logger=logger_spy,
        metrics=metrics_mock,
    )

    # --- Assert: no upstream fetch, no write, no mark -----------------
    assert client.calls == [], (
        f"no endpoint may be fetched when enumeration is empty; "
        f"got {client.calls!r}"
    )
    assert writer.writes == [], (
        f"no artifact may be written when enumeration is empty; "
        f"got {[w['name'] for w in writer.writes]!r}"
    )
    assert checkpoint.marks == [], (
        f"no game may be checkpointed when enumeration is empty; "
        f"got {checkpoint.marks!r}"
    )

    # --- Assert: the pending probe fired once, with an empty tuple ------
    expected_pendings = [(config.DOMAIN_GAMES, ())]
    assert checkpoint.pendings == expected_pendings, (
        f"expected exactly one get_pending probe carrying an empty key "
        f"tuple {expected_pendings!r}; got {checkpoint.pendings!r}"
    )

    # --- Assert: no row-written increment at all -----------------------
    assert _row_written_increments(metrics_mock) == [], (
        f"no {_ROWS_WRITTEN_COUNTER} increment may fire when the "
        f"aggregation loop never runs; got "
        f"{_row_written_increments(metrics_mock)}"
    )

    # --- Assert: the run terminated through the SKIP branch ------------
    # Exactly one terminal event, and it must be the skip variant with the
    # domain and season interpolated. The alternative terminal event
    # (``processed=%d failed=%d``) would mean the guard was bypassed and
    # the pipeline fell through an empty loop instead.
    completion_events = logger_spy.info_events_starting(
        _PIPELINE_COMPLETE_PREFIX
    )
    assert completion_events == EXPECTED_SKIP_COMPLETION_EVENTS, (
        f"a zero-enumerated-games run must terminate through the skip "
        f"branch, logging exactly {EXPECTED_SKIP_COMPLETION_EVENTS!r}; "
        f"got {completion_events!r}"
    )

    # --- Assert: no per-game event was logged --------------------------
    game_events = logger_spy.info_events_starting(_GAME_COMPLETE_PREFIX)
    assert game_events == [], (
        f"no per-game completion event may be logged when no game is "
        f"processed; got {game_events!r}"
    )


# ---------------------------------------------------------------------------
# Test F -- boundary: a 0x0 primary frame still writes and still checkpoints
# ---------------------------------------------------------------------------


def test_degenerate_zero_by_zero_frame_still_writes_and_checkpoints(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_playbyplay_payloads: Dict[str, Any],
    mini_season_game_ids: List[str],
) -> None:
    """A ``headers: []`` / ``rowSet: []`` box score yields a (0, 2) artifact.

    A result set declaring no headers and no rows normalises to a genuine
    0x0 frame, so ``_ensure_game_columns`` takes BOTH insertion branches:
    ``season`` at index 0, then ``game_id`` at index 1 -- giving columns
    ['season', 'game_id'] and shape (0, 2). The degenerate frame must flow
    through the happy path: the write is still issued, the counter still
    fires with ``n=0``, and the game is still checkpointed.

    Mutation detected: a "skip the write when the frame is empty" or
    "skip the counter when the frame is empty" short circuit, and any
    change that lets a degenerate-but-valid payload fall into the Rule 6
    handler. Asserting ``games_failed_total`` was never incremented is
    mandatory here: without it, a swallowed exception would masquerade as
    success.
    """
    # --- Arrange -------------------------------------------------------
    # Exactly one game, whose box score is degenerate while its
    # play-by-play is the normal 4-row G1 envelope.
    game_id = mini_season_game_ids[0]
    _patch_mini_season_enumerate(monkeypatch, [game_id])
    client = _MiniSeasonClient(
        boxscore_payloads={
            game_id: _zero_row_boxscore_payload(game_id, headers=[]),
        },
        playbyplay_payloads={
            game_id: mini_season_playbyplay_payloads[game_id],
        },
    )
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_games.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: the games write WAS recorded -------------------------
    games_writes = _writes_named(writer, config.CSV_GAMES)
    assert len(games_writes) == 1, (
        f"a degenerate payload must still produce exactly 1 games write; "
        f"got {len(games_writes)} from {[w['name'] for w in writer.writes]!r}"
    )
    degenerate_df = games_writes[0]["df"]
    assert list(degenerate_df.columns) == EXPECTED_DEGENERATE_COLUMNS, (
        f"a 0x0 primary frame must gain both key columns as "
        f"{EXPECTED_DEGENERATE_COLUMNS}; got {list(degenerate_df.columns)}"
    )
    assert degenerate_df.shape == EXPECTED_DEGENERATE_SHAPE, (
        f"a 0x0 primary frame must widen to {EXPECTED_DEGENERATE_SHAPE} "
        f"(0 rows; 0 payload + 2 inserted columns); "
        f"got {degenerate_df.shape}"
    )

    # --- Assert: the rest of the iteration ran normally ---------------
    # G1's play-by-play carries 4 rows, so the second write is unaffected.
    pbp_sizes = [w["rows"] for w in _writes_named(writer, config.CSV_PLAY_BY_PLAY)]
    assert pbp_sizes == [EXPECTED_PBP_INCREMENTS[0]], (
        f"the play-by-play half must be untouched by the degenerate box "
        f"score, writing {[EXPECTED_PBP_INCREMENTS[0]]} rows; "
        f"got {pbp_sizes}"
    )

    # --- Assert: the counter still fired, as n=0 then n=4 -------------
    boundary_increments = _row_written_increments(metrics_mock)
    assert boundary_increments == EXPECTED_BOUNDARY_INCREMENTS, (
        f"the counter must report the zero verbatim as "
        f"{EXPECTED_BOUNDARY_INCREMENTS} (games 0, play-by-play 4); "
        f"got {boundary_increments}"
    )

    # --- Assert: checkpointed, and Rule 6 never engaged ---------------
    assert checkpoint.marks == [(config.DOMAIN_GAMES, game_id)], (
        f"a degenerate-but-valid payload must still be checkpointed as "
        f"{[(config.DOMAIN_GAMES, game_id)]!r}; got {checkpoint.marks!r}"
    )
    failure_calls = _counter_calls(metrics_mock, _GAMES_FAILED_COUNTER)
    assert failure_calls == [], (
        f"{_GAMES_FAILED_COUNTER} must never be incremented -- a degenerate "
        f"payload is valid input, not a Rule 6 failure; got {failure_calls!r}"
    )


# ---------------------------------------------------------------------------
# Test G -- boundary: a zero-row frame keeps its declared payload columns
# ---------------------------------------------------------------------------


def test_zero_row_frame_with_declared_headers_keeps_payload_columns(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_playbyplay_payloads: Dict[str, Any],
    mini_season_game_ids: List[str],
) -> None:
    """Declared headers with an empty ``rowSet`` yield a (0, 4) artifact.

    The complement of the 0x0 case. Headers ['PLAYER_ID', 'PTS'] with no
    rows normalise to a 0x2 frame; neither declared name matches ``season``
    or ``game_id`` case-insensitively, so both key columns are inserted in
    front -- ['season', 'game_id', 'PLAYER_ID', 'PTS'], i.e. 2 payload + 2
    inserted = 4 columns and still 0 rows.

    Mutation detected: dropping the declared columns when the row set is
    empty (which would collapse this case onto the 0x0 shape and silently
    narrow the artifact's schema), appending the key columns instead of
    inserting them at the front, or skipping the checkpoint for an empty
    frame.
    """
    # --- Arrange -------------------------------------------------------
    game_id = mini_season_game_ids[0]
    _patch_mini_season_enumerate(monkeypatch, [game_id])
    client = _MiniSeasonClient(
        boxscore_payloads={
            game_id: _zero_row_boxscore_payload(
                game_id, headers=["PLAYER_ID", "PTS"],
            ),
        },
        playbyplay_payloads={
            game_id: mini_season_playbyplay_payloads[game_id],
        },
    )
    writer = recording_writer()
    checkpoint = recording_checkpoint()
    metrics_mock = MagicMock()

    # --- Act -----------------------------------------------------------
    ingest_games.run(
        client=client,
        writer=writer,
        checkpoint=checkpoint,
        season=_SEASON,
        metrics=metrics_mock,
    )

    # --- Assert: declared columns survive behind the inserted keys -----
    header_only_df = _last_frame_named(writer, config.CSV_GAMES)
    assert list(header_only_df.columns) == EXPECTED_HEADER_ONLY_COLUMNS, (
        f"declared payload columns must survive an empty rowSet as "
        f"{EXPECTED_HEADER_ONLY_COLUMNS}; got {list(header_only_df.columns)}"
    )
    assert header_only_df.shape == EXPECTED_HEADER_ONLY_SHAPE, (
        f"a zero-row frame with 2 declared headers must be "
        f"{EXPECTED_HEADER_ONLY_SHAPE} (0 rows; 2 payload + 2 inserted); "
        f"got {header_only_df.shape}"
    )

    # --- Assert: still counted as n=0 and still checkpointed ----------
    boundary_increments = _row_written_increments(metrics_mock)
    assert boundary_increments == EXPECTED_BOUNDARY_INCREMENTS, (
        f"a header-only frame must still be counted verbatim as "
        f"{EXPECTED_BOUNDARY_INCREMENTS} (games 0, play-by-play 4); "
        f"got {boundary_increments}"
    )
    assert checkpoint.marks == [(config.DOMAIN_GAMES, game_id)], (
        f"a header-only frame must still be checkpointed as "
        f"{[(config.DOMAIN_GAMES, game_id)]!r}; got {checkpoint.marks!r}"
    )
