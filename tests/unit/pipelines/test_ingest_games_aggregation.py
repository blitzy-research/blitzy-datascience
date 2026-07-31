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
* the complete ``(labels, n)`` ownership of every row-written emission, so
  a swapped or malformed games/play-by-play label pair cannot hide behind a
  correct value sequence;
* one ordered ``checkpoint.mark_completed`` per game and exactly one
  ``get_pending`` probe carrying the full enumerated tuple;
* the zero-enumerated-games short circuit -- no write, no mark, no metric
  increment, no upstream fetch, and **no prior-session buffer load**:
  neither ``_load_existing_games`` nor ``_load_existing_pbp`` is reached,
  so an all-checkpointed run performs no artifact stat and no
  :func:`pandas.read_csv`. Its positive control proves each loader *does*
  run exactly once, with the writer's output directory and the full
  pending list, as soon as a game is pending;
* the degenerate-frame boundary -- a zero-row payload still produces a
  write, still checkpoints, still increments the counter with ``n=0`` under
  its own artifact label, and is **not** swallowed by the Rule 6 handler.

Two of those contracts are observable only in the log stream, so a
handwritten :class:`logging.LoggerAdapter` spy is injected alongside the
collaborator spies: the *terminal event* that proves the zero-game run
took the skip branch (see :class:`_LoggerSpy` for why the collaborator
spies cannot see it), and the per-game ``box_rows`` / ``pbp_rows`` event
that independently witnesses the counts the row counter reports. Both are
compared as complete ``(format string, args)`` records, so the
operator-facing field NAMES are pinned alongside their values -- an
args-only comparison would accept a renamed or re-shaped event.

One contract is observable only through the *absence* of work, so the two
prior-session buffer loaders are replaced with recording spies (see
:class:`_BufferLoaderSpy`): hoisting either above the ``if not pending:``
guard is a pure performance regression that leaves every write, mark,
metric and log record unchanged.

Why a separate sibling module
-----------------------------
``tests/unit/pipelines/test_ingest_games.py`` owns the Rule 6 canary
suite and checks the aggregation with shapes that constrain direction and
sign rather than exact values (see below). This module carries the
value-exact assertions instead, so each concern stays reviewable on its
own. A sibling module is collected automatically because ``pytest.ini``
sets ``python_files = test_*.py``.

The assertion shapes this module strengthens
--------------------------------------------
The happy-path test in ``test_ingest_games.py`` checks the aggregation
with two shapes that a plausible bug can satisfy:

1. a **monotonicity** check on the cumulative write sizes,
   ``assert w["rows"] >= prev_games_rows``; and
2. a **presence-and-positivity** check on the row counter,
   ``assert "n" in c.kwargs`` followed by ``assert c.kwargs["n"] > 0``.

Neither shape can separate the correct implementation from a mutation
that replaces the cumulative ``pd.concat(buffer, ...)`` with "write only
the latest frame", nor from one that emits ``len(combined_games)``
instead of ``len(bs_df)``, when every game contributes the same number
of rows: the fixtures that test uses deliver **equal** per-game counts
(2 box-score and 3 play-by-play rows per game), so the latest-frame
sequence is flat and still non-decreasing, and every positive ``n=``
looks alike whether it is a per-game delta or a running total. This
module asserts the sequences themselves, in the aggregation path.

Why the per-game row counts are deliberately unequal
----------------------------------------------------
The canonical mini-season fixture contributes **2 / 3 / 2** box-score
rows and **4 / 1 / 3** play-by-play rows. That asymmetry is what makes
the two mutations above observable: "write only the latest frame" yields
``[2, 3, 2]`` / ``[4, 1, 3]`` rather than the running totals
``[2, 5, 7]`` / ``[4, 5, 8]``, and emitting cumulative lengths yields
``[2, 4, 5, 5, 7, 8]`` rather than the per-game ``[2, 4, 3, 1, 2, 3]``.
Because consecutive games contribute different counts, every running
total is strictly larger than the one before it, so each mutated sequence
diverges from the correct one from the second write onward -- and the
latest-frame sequences additionally decrease, which no cumulative
sequence can. The counts must not be normalised, equalised, or reordered.

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
Rule 6 error case. The boundary tests do additionally assert that
``games_failed_total`` was **never** incremented: that is negative-space
evidence, alongside their recorded write, row-increment and
checkpoint-mark assertions, that a degenerate-but-valid payload flowed
through the happy path rather than being swallowed by the fail-safe
handler.

There is no literal "zero-game divisor" in this codebase
--------------------------------------------------------
Stated plainly rather than papered over: no arithmetic division, no
``mean``, no ``groupby`` aggregation and no ``agg`` call site exists
anywhere in ``api/``, ``endpoints/``, ``pipelines/``, ``storage/``,
``utils/``, ``config.py``, or ``run.py``, so no calculation there can
produce a zero-game denominator. (The ``/`` operator does appear in
production, but only to compose :class:`pathlib.Path` values.) The only
aggregation primitives are the two ``pd.concat`` calls this module
targets. So there is no divisor to drive to zero, and none is invented
here. The requirement is honoured through faithful analogues, of which
this module supplies three: the zero-enumerated-games short circuit (the
loop body never executes), cumulative row-count conservation (exactly 7
and 8), and per-game versus cumulative counter semantics (the
``[2, 4, 3, 1, 2, 3]`` sequence).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple
from unittest.mock import MagicMock

import pandas as pd
import pytest

import config
from pipelines import ingest_games


# ---------------------------------------------------------------------------
# Module-level constants -- fixture-facing
# ---------------------------------------------------------------------------

#: Season string threaded through every ``ingest_games.run`` call in this
#: module. Asserted indirectly via ``writer.writes[*]["season"]``.
_SEASON = "2025-26"

#: Endpoint name for the traditional box score. Appears as ``calls[i][0]``
#: in :attr:`_MiniSeasonClient.calls`, the module-local client spy defined
#: below.
_BOXSCORE_ENDPOINT = "boxscoretraditionalv2"

#: Endpoint name for the play-by-play endpoint.
_PLAYBYPLAY_ENDPOINT = "playbyplayv2"

#: Verbatim per-artifact row counter emitted after every successful game
#: (``pipelines/ingest_games.py`` lines 802-811).
_ROWS_WRITTEN_COUNTER = "pipeline_rows_written_total"

#: Verbatim Rule 6 failure counter (``pipelines/ingest_games.py`` line
#: 851). The boundary tests assert it was never incremented.
_GAMES_FAILED_COUNTER = "games_failed_total"

#: The ``pipeline`` label value every ``pipeline_rows_written_total``
#: increment carries. For that counter the label *names*
#: (``pipeline``/``artifact``, not ``domain``/``file``) are the documented
#: operator contract. It does not apply to ``games_failed_total``, which is
#: emitted with a single-key label set instead -- ``{"reason": <exception
#: class name>}`` (``pipelines/ingest_games.py`` line 851).
_PIPELINE_LABEL = "ingest_games"

#: Log-message prefix of the per-game completion event emitted once per
#: successfully processed game (``pipelines/ingest_games.py`` line 820).
_GAME_COMPLETE_PREFIX = "pipeline.games.game_complete"

#: The COMPLETE, verbatim format string of that per-game event. The
#: production call site splits the literal across two source lines
#: (``pipelines/ingest_games.py`` lines 820-825), which the interpreter
#: concatenates into exactly the single string reproduced here -- one space
#: between ``game_id=%s`` and ``box_rows=%d``.
#:
#: Comparing the format string itself, and not merely its interpolated
#: arguments, is what pins the operator-facing FIELD NAMES and the
#: ``%d``/``%s`` conversion kinds. Renaming ``box_rows`` to ``rows``,
#: dropping ``pbp_rows``, reordering the fields, or appending misleading
#: trailing content after the same prefix all leave the positional
#: arguments identical, so an args-only comparison cannot see any of them.
_GAME_COMPLETE_FORMAT = (
    "pipeline.games.game_complete game_id=%s box_rows=%d pbp_rows=%d"
)

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

#: The COMPLETE label mapping every games-CSV row-written increment must
#: carry, transcribed from ``pipelines/ingest_games.py`` lines 802-806. The
#: label NAMES are ``pipeline``/``artifact`` (not ``domain``/``file``) and
#: the artifact value is the CSV filename, so it is built from
#: ``config.CSV_GAMES`` rather than a duplicated literal.
EXPECTED_GAMES_LABELS = {
    "pipeline": _PIPELINE_LABEL,
    "artifact": f"{config.CSV_GAMES}.csv",
}

#: The COMPLETE label mapping every play-by-play row-written increment must
#: carry (``pipelines/ingest_games.py`` lines 808-811).
EXPECTED_PBP_LABELS = {
    "pipeline": _PIPELINE_LABEL,
    "artifact": f"{config.CSV_PLAY_BY_PLAY}.csv",
}

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

#: The same two boundary emissions as COMPLETE ordered ``(labels, n)``
#: pairs: the games artifact owns the ``0`` and the play-by-play artifact
#: owns the ``4``. Derivation: the degenerate box score contributes 0 rows
#: (``EXPECTED_BOUNDARY_INCREMENTS[0]``) and G1's play-by-play contributes
#: 4 (``EXPECTED_PBP_INCREMENTS[0]``), emitted games-then-pbp within the
#: single iteration.
#:
#: The values alone cannot prove ARTIFACT OWNERSHIP: swapping the two
#: label mappings leaves the ``[0, 4]`` value sequence intact while
#: reporting the empty box score against ``play_by_play.csv`` and the four
#: play-by-play events against ``games.csv`` -- so every per-artifact
#: operator dashboard would silently invert. Pairing each value with its
#: full label mapping, in emission order, is what closes that.
EXPECTED_BOUNDARY_EMISSIONS = [
    (EXPECTED_GAMES_LABELS, EXPECTED_BOUNDARY_INCREMENTS[0]),
    (EXPECTED_PBP_LABELS, EXPECTED_BOUNDARY_INCREMENTS[1]),
]

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


def _row_written_emissions(
    metrics_mock: MagicMock,
) -> List[Tuple[Optional[Dict[str, str]], Optional[int]]]:
    """Ordered ``(labels, n)`` pairs of every row-written increment.

    The complement of :func:`_row_written_increments`: instead of
    discarding the label mapping to compare values, each value is returned
    still attached to the COMPLETE mapping it was emitted with, in emission
    order. That is what makes artifact ownership assertable -- swapping the
    two label mappings leaves the value sequence untouched, so only the
    paired form can detect it.

    ``None`` is substituted for a missing label mapping or a missing ``n``
    keyword rather than raising, so a mutation that passed the labels by
    keyword or dropped the increment surfaces as a readable mismatch in the
    caller's assertion message instead of as an ``IndexError`` or
    ``KeyError`` from inside this helper.
    """
    emissions: List[Tuple[Optional[Dict[str, str]], Optional[int]]] = []
    for call in _counter_calls(metrics_mock, _ROWS_WRITTEN_COUNTER):
        labels = call.args[1] if len(call.args) > 1 else None
        emissions.append((labels, call.kwargs.get("n")))
    return emissions


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


class _BufferLoaderSpy:
    """Handwritten recording stand-in for a prior-session buffer loader.

    ``ingest_games.run`` seeds its two in-memory buffers by calling
    ``_load_existing_games`` and ``_load_existing_pbp`` (``pipelines/
    ingest_games.py`` lines 759-765) -- and it does so *after* the
    ``if not pending:`` short circuit at line 735. Each helper performs a
    ``Path.is_file()`` probe and, when the artifact exists, a full
    :func:`pandas.read_csv` of a file that grows to a whole season of box
    scores and play-by-play events. On an all-checkpointed run that work is
    pure waste, which is exactly why the guard precedes it.

    Nothing observable to the collaborator spies changes if that ordering
    is inverted: with an empty pending list the loop body does not execute
    either way, so writes, marks, metrics and even the terminal log event
    stay identical while every run silently pays for two file probes and
    two potentially large CSV parses. Recording the invocations is
    therefore the only way to pin the guard's PERFORMANCE contract.

    Returning ``[]`` reproduces the real behaviour under
    ``RecordingWriter`` faithfully: that spy creates its ``output_dir`` but
    writes no CSV, so both production helpers take their
    ``if not path.is_file(): return []`` branch. Substituting this spy
    therefore leaves every cumulative row count in this module unchanged.

    A handwritten callable class is used rather than a
    :class:`~unittest.mock.MagicMock` so the production call signature is
    enforced positionally at call time -- a renamed or reordered loader
    parameter surfaces as a ``TypeError`` here instead of being absorbed by
    attribute-access magic.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: List[Tuple[Any, str, Tuple[str, ...]]] = []

    def __call__(
        self,
        output_dir: Any,
        season: str,
        pending_ids: Iterable[str],
        log: Any,
    ) -> List[pd.DataFrame]:
        """Record ``(output_dir, season, pending_ids)`` and seed nothing."""
        self.calls.append((output_dir, str(season), tuple(pending_ids)))
        return []


def _patch_buffer_loaders(
    monkeypatch: pytest.MonkeyPatch,
    games_loader: _BufferLoaderSpy,
    pbp_loader: _BufferLoaderSpy,
) -> None:
    """Install both :class:`_BufferLoaderSpy` instances on the pipeline.

    The patch targets are the names in the *pipeline's* namespace, which is
    also where they are defined, so ``monkeypatch.setattr``'s default
    ``raising=True`` guarantees a typo cannot silently produce a vacuously
    passing "was never called" assertion: a wrong attribute name raises
    :class:`AttributeError` at patch time. ``run`` resolves both helpers
    from module globals at call time, so the substitution takes effect for
    the invocation under test and is undone on teardown.
    """
    monkeypatch.setattr(
        "pipelines.ingest_games._load_existing_games", games_loader,
    )
    monkeypatch.setattr(
        "pipelines.ingest_games._load_existing_pbp", pbp_loader,
    )


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


class _MiniSeasonClient:
    """Standalone handwritten client spy that routes on ``params["GameID"]``.

    A self-contained, module-local stand-in for
    :class:`api.nba_client.NBAClient`. It mirrors the production
    ``get(endpoint, params) -> dict`` signature verbatim (see
    ``api/nba_client.py`` line 368) and resolves its response by
    ``GameID``, which is what lets the mini-season return a *different*
    box score per game -- something an endpoint-keyed spy cannot express.

    It records call tuples on :attr:`calls` using the same
    ``(endpoint, params)`` shape as the shared ``RecordingClient`` spy, so
    assertions over ``client.calls`` stay semantically compatible with
    every other pipeline test in the suite. Owning that recording locally
    -- rather than deriving from the shared spy -- keeps this
    single-module concern out of the shared fixture surface entirely; it
    is the same standalone shape ``_SelectiveFailureClient`` uses in
    ``test_ingest_games.py``.

    Being *handwritten* rather than a :class:`~unittest.mock.MagicMock` is
    equally deliberate: a drift in the production client's ``get``
    signature surfaces here as a real ``TypeError`` at call time instead
    of being silently absorbed by attribute-access magic.

    Both endpoint helpers set ``params["GameID"] = str(game_id)``
    (``endpoints/games.py`` lines 251 and 399), so one routing key serves
    the box-score and play-by-play calls alike.

    An unmapped endpoint or an unmapped ``GameID`` raises
    :class:`AssertionError` rather than falling back to a synthetic
    envelope: a silent fallback would let a routing bug feed plausible but
    wrong row counts into the aggregation assertions.

    Attributes
    ----------
    calls:
        Ordered ``(endpoint, params)`` tuples, one appended per
        :meth:`get` invocation. ``params`` is a defensive shallow copy, so
        later mutation by the caller cannot retroactively change what was
        recorded.
    boxscore_payloads:
        ``GAME_ID`` -> ``boxscoretraditionalv2`` envelope map, copied at
        construction time.
    playbyplay_payloads:
        ``GAME_ID`` -> ``playbyplayv2`` envelope map, copied at
        construction time.
    """

    def __init__(
        self,
        boxscore_payloads: Dict[str, Any],
        playbyplay_payloads: Dict[str, Any],
    ) -> None:
        self.boxscore_payloads: Dict[str, Any] = dict(boxscore_payloads)
        self.playbyplay_payloads: Dict[str, Any] = dict(playbyplay_payloads)
        self.calls: List[Tuple[str, Dict[str, Any]]] = []

    def get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Record the call on :attr:`calls`, then route by endpoint and GameID."""
        # Snapshot the parameter map defensively before routing, so the
        # recorded tuple reflects the arguments exactly as they were
        # passed even if the caller mutates its dict afterwards.
        self.calls.append((str(endpoint), dict(params or {})))
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
    2/2+3=5/5+2=7 and 4/4+1=5/5+3=8. A monotonicity check on the write
    sizes cannot separate the two when every game contributes the same
    number of rows, because the mutant's sequence is then flat and still
    non-decreasing. The deliberately unequal per-game counts used here make
    the latest-frame sequences both numerically different from the
    cumulative totals and non-monotonic, so exact ordered equality is what
    pins the behaviour. A dropped or double-appended frame, or a swap of the
    two ``writer.write`` calls inside one iteration, is caught by the same
    assertions.
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
# Test B -- the row-written counter carries PER-GAME deltas
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
    turns the sequence into the cumulative [2, 4, 5, 5, 7, 8]. Every value
    in both sequences is positive, so a presence-and-positivity check on the
    ``n=`` keyword is satisfied either way; only the exact ordered
    comparison distinguishes a per-game delta from a running frame length.
    Renaming or dropping either label empties the per-artifact partitions,
    and reordering the two ``inc`` calls within an iteration breaks the
    interleaved sequence. The per-game log event is asserted alongside as an
    independent witness, so a mutation that corrupted only one of the two
    observability surfaces is still caught.
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
    #
    # The comparison is against the COMPLETE ``(format string, args)``
    # record, not the arguments alone. The format string is the operator
    # contract -- it names the fields a log-parsing rule keys on -- so
    # renaming ``box_rows``, dropping ``pbp_rows``, swapping ``%d`` for
    # ``%s``, or appending trailing content after the same prefix must all
    # fail here even though each leaves the three positional arguments
    # byte-identical.
    expected_game_events = [
        (_GAME_COMPLETE_FORMAT, (gid, box_rows, pbp_rows))
        for gid, (box_rows, pbp_rows) in zip(
            mini_season_game_ids, EXPECTED_GAME_COMPLETE_ROWS,
        )
    ]
    observed_game_events = logger_spy.info_events_starting(
        _GAME_COMPLETE_PREFIX
    )
    assert observed_game_events == expected_game_events, (
        f"the per-game completion log must emit the exact format string "
        f"{_GAME_COMPLETE_FORMAT!r} carrying the PER-GAME row counts "
        f"{[args for _fmt, args in expected_game_events]!r} (one event per "
        f"game, in enumeration order); got {observed_game_events!r}"
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
    * **Hoisting either buffer loader ABOVE the guard** -- a PERFORMANCE
      regression that every other assertion in this test survives. Both
      helpers stat the artifact and, when it exists, parse a whole season
      of rows with :func:`pandas.read_csv`; moving them above the guard
      makes an all-checkpointed run -- the common case for an operator
      re-running a completed season -- pay for two file probes and two
      full CSV parses to produce nothing. Only instrumenting the loaders
      detects it, which is what the two loader-spy assertions below do.
      Their non-vacuity is established by
      ``test_buffer_loaders_run_once_each_after_the_pending_guard``, which
      proves the very same patch seam records invocations when games ARE
      pending.
    """
    # --- Arrange -------------------------------------------------------
    # Enumeration returns nothing; the payload mappings are still supplied so
    # they stay populated for every mini-season ID. A fetch for one of those
    # mapped IDs therefore could not fail merely because fixture data was
    # absent, which is what makes the absence of fetches below attributable
    # to the guard. (Unmapped endpoints and unmapped ``GameID`` values raise
    # AssertionError by design -- see :class:`_MiniSeasonClient`.)
    _patch_mini_season_enumerate(monkeypatch, [])
    # Recording spies replace the two prior-session buffer loaders so their
    # ABSENCE of invocation is assertable. They return [] exactly as the
    # real helpers do against a RecordingWriter directory holding no CSV,
    # so nothing else about the run changes.
    games_loader = _BufferLoaderSpy("_load_existing_games")
    pbp_loader = _BufferLoaderSpy("_load_existing_pbp")
    _patch_buffer_loaders(monkeypatch, games_loader, pbp_loader)
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

    # --- Assert: neither buffer loader was even reached ----------------
    # The guard must return BEFORE the prior-session seeds are loaded, so
    # an all-checkpointed run performs no filesystem probe and no CSV
    # parse at all. Empty call lists are the only observable proof of that
    # ordering: every other quantity in this test is identically empty
    # whether the loaders ran or not.
    assert games_loader.calls == [], (
        f"{games_loader.name} must NOT run when nothing is pending -- the "
        f"if-not-pending guard precedes it precisely so an all-checkpointed "
        f"run performs no {config.CSV_GAMES}.csv stat and no "
        f"pandas.read_csv; got {games_loader.calls!r}"
    )
    assert pbp_loader.calls == [], (
        f"{pbp_loader.name} must NOT run when nothing is pending -- same "
        f"guard, same wasted {config.CSV_PLAY_BY_PLAY}.csv parse; got "
        f"{pbp_loader.calls!r}"
    )


# ---------------------------------------------------------------------------
# Test E2 -- positive control: both buffer loaders DO run, once, past the guard
# ---------------------------------------------------------------------------


def test_buffer_loaders_run_once_each_after_the_pending_guard(
    monkeypatch: pytest.MonkeyPatch,
    recording_writer,
    recording_checkpoint,
    mini_season_boxscore_payloads: Dict[str, Any],
    mini_season_playbyplay_payloads: Dict[str, Any],
    mini_season_game_ids: List[str],
) -> None:
    """Each buffer loader is invoked exactly once, with the pending list.

    This is the POSITIVE CONTROL for Test E's "neither loader was called"
    assertions. Without it those assertions could only prove that *nothing*
    reached the loaders -- not that the same patch seam is capable of
    observing an invocation at all. Here the identical seam records one call
    per loader, so Test E's empty call lists are attributable to the
    ``if not pending:`` guard rather than to an inert spy.

    It also pins the loaders' own resume contract, which is otherwise
    unasserted: each receives the WRITER's ``output_dir`` (so artifacts are
    sought where they are written, never in the operator's real
    ``config.OUTPUT_DIR``), the caller's season, and the FULL pending list
    -- the last of which the helpers need to filter already-present rows
    for the games about to be re-fetched.

    Mutations detected
    ------------------
    * Calling either loader twice, or once per pending game instead of once
      per run: a season of 1,230 games would then perform 1,230 CSV parses
      instead of one, and the per-run call-count assertions fail.
    * Passing ``config.OUTPUT_DIR`` instead of the writer's directory: a
      test run would read the operator's real artifacts, and the recorded
      ``output_dir`` no longer matches ``writer.output_dir``.
    * Passing the full enumerated list, an empty list, or a single
      ``GAME_ID`` where the pending list belongs: the recorded tuple stops
      matching, and the loaders would lose their ability to dedupe
      prior-session rows for exactly the games being re-fetched.
    """
    # --- Arrange -------------------------------------------------------
    # A fresh checkpoint means every enumerated game is pending, so the
    # guard falls through and the loaders must run.
    _patch_mini_season_enumerate(monkeypatch, mini_season_game_ids)
    games_loader = _BufferLoaderSpy("_load_existing_games")
    pbp_loader = _BufferLoaderSpy("_load_existing_pbp")
    _patch_buffer_loaders(monkeypatch, games_loader, pbp_loader)
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

    # --- Assert: exactly one invocation each, with the exact arguments --
    # Hand-derived: ``run`` resolves the directory as
    # ``getattr(writer, "output_dir", config.OUTPUT_DIR)``, and a fresh
    # checkpoint leaves all three enumerated ids pending, so each loader
    # must see (writer.output_dir, "2025-26", (G1, G2, G3)) exactly once.
    expected_loader_calls = [
        (writer.output_dir, _SEASON, tuple(mini_season_game_ids)),
    ]
    assert games_loader.calls == expected_loader_calls, (
        f"{games_loader.name} must run exactly once, seeded from the "
        f"writer's own output directory and handed the full pending list "
        f"{expected_loader_calls!r}; got {games_loader.calls!r}"
    )
    assert pbp_loader.calls == expected_loader_calls, (
        f"{pbp_loader.name} must run exactly once with the same "
        f"{expected_loader_calls!r}; got {pbp_loader.calls!r}"
    )

    # --- Assert: substituting the spies changed no aggregate ------------
    # Fidelity check for Test E: the spies seed [] exactly as the real
    # helpers do against a RecordingWriter directory holding no CSV, so the
    # cumulative sizes must remain the canonical 2 ; 2+3=5 ; 5+2=7.
    games_sizes = [w["rows"] for w in _writes_named(writer, config.CSV_GAMES)]
    assert games_sizes == EXPECTED_CUMULATIVE_GAMES_ROWS, (
        f"seeding an empty buffer must leave the cumulative games-CSV sizes "
        f"at {EXPECTED_CUMULATIVE_GAMES_ROWS} (2 ; 2+3=5 ; 5+2=7), proving "
        f"the loader spies are behaviour-preserving; got {games_sizes}"
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
    handler -- the recorded write, the ``[0, 4]`` increments and the
    checkpoint mark all vanish once an exception is swallowed there. The
    zero-``games_failed_total`` assertion is an additional negative-space
    check that names that failure mode explicitly.
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
    # ...and each value must be attributed to the RIGHT artifact. The
    # values alone survive a label swap, which would report the empty box
    # score against play_by_play.csv and G1's four events against
    # games.csv -- inverting every per-artifact dashboard while the
    # assertion above still passed.
    boundary_emissions = _row_written_emissions(metrics_mock)
    assert boundary_emissions == EXPECTED_BOUNDARY_EMISSIONS, (
        f"the ordered (labels, n) emissions must be "
        f"{EXPECTED_BOUNDARY_EMISSIONS!r} -- the zero owned by "
        f"{config.CSV_GAMES}.csv and the 4 owned by "
        f"{config.CSV_PLAY_BY_PLAY}.csv, each under the complete "
        f"pipeline/artifact label set; got {boundary_emissions!r}"
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
    # Same artifact-ownership pin as the 0x0 case: a swapped or malformed
    # label mapping leaves the [0, 4] value sequence intact, so only the
    # ordered (labels, n) pairs can detect it.
    boundary_emissions = _row_written_emissions(metrics_mock)
    assert boundary_emissions == EXPECTED_BOUNDARY_EMISSIONS, (
        f"the ordered (labels, n) emissions must be "
        f"{EXPECTED_BOUNDARY_EMISSIONS!r} -- the header-only frame's zero "
        f"owned by {config.CSV_GAMES}.csv and the 4 owned by "
        f"{config.CSV_PLAY_BY_PLAY}.csv; got {boundary_emissions!r}"
    )
    assert checkpoint.marks == [(config.DOMAIN_GAMES, game_id)], (
        f"a header-only frame must still be checkpointed as "
        f"{[(config.DOMAIN_GAMES, game_id)]!r}; got {checkpoint.marks!r}"
    )
