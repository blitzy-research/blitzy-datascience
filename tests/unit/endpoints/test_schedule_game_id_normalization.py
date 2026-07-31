"""``GAME_ID`` normalization and dedupe tests for :mod:`endpoints.schedule`.

What this module pins
---------------------

The **deduplication stage** of
:func:`endpoints.schedule.enumerate_game_ids` — the cross-domain helper
:mod:`pipelines.ingest_games` calls to decide which games to iterate.
Four contracts are pinned here:

1. **Order-preserving dedupe with exact cardinality and canonical
   form.** Five team-rows describing three games must collapse to
   exactly three identifiers, each exactly ten characters wide — the
   zero-padded form the helper's own docstring promises.
2. **Five row- and envelope-shape guards**: a row shorter than the
   ``GAME_ID`` index, an empty row, a ``None`` ``GAME_ID`` cell, a table
   that declares ``GAME_ID`` but carries no ``rowSet`` key at all, and
   ``resultSets`` delivered as a dict instead of a list.
3. **Degrading to an empty list rather than raising.** Where the helper
   is documented to return ``[]`` instead of raising, each test asserts
   ``result == []`` exactly — never a bare "it did not raise", and never
   ``assert not result``, which would also accept ``None``, ``0``, or
   ``""``.
4. **Canonicalization across mixed identifier wire types**, so one game
   delivered as both an integer and a zero-padded string collapses to a
   single identifier. See the dedicated test at the bottom of this
   module.

Hand-derived expected values
----------------------------

Every expected value in this module was derived by **reading the fixture
and applying the helper's intended semantics on paper**, never by
running :func:`enumerate_game_ids` and capturing what it returned. The
arithmetic is shown beside each module-level constant and restated in
the docstring of the test that consumes it. Snapshot-style assertions
appear nowhere: there is no golden file, no record-then-compare helper,
and no DataFrame comparison — this module constructs no DataFrames and
imports no pandas symbols.

Every test also names, in its docstring, the specific single-statement
mutation it detects. **Mutation resistance, not line coverage, is the
acceptance bar for this module**, so no test here settles for a "runs
without error" or "output is non-empty" assertion.

Test tier
---------

No test here carries a pytest marker. ``pytest.ini`` registers exactly
two markers (``integration`` and ``invariant``) under
``--strict-markers``, so an unregistered marker would be a hard
collection error. Unmarked tests land on the default offline tier and
therefore run in every invocation mode, including ``-m "not
integration"``.

Log assertions
--------------

The records asserted below are **logging** records emitted through
``logger.warning(...)`` and ``logger.info(...)`` — they are NOT Python
``warnings``, and they do not interact with ``pytest.ini``'s
``filterwarnings = error``. They are therefore captured with pytest's
built-in ``caplog`` fixture via
``caplog.at_level(level, logger="endpoints.schedule")``; that logger
name follows from ``get_logger(__name__)`` in ``endpoints/schedule.py``.
``pytest.warns`` would not see these records and must not be
substituted here. Message assertions use lenient substring matching so
the production format string stays uncoupled from the test, matching the
substring-matching convention used in ``test_schedule.py``.

Rule compliance
---------------

* Rule 1 (Single HTTP Client) — no ``import requests`` anywhere. Every
  HTTP interaction is mediated by the handwritten ``RecordingClient``
  spy obtained from the ``recording_client`` factory fixture in
  :mod:`tests.conftest`; no network I/O occurs and no ``MagicMock`` is
  used.
* Rule 7 (Pluggable Storage) — no ``DataFrame.to_csv`` call and no
  pandas import at all. This module emits no CSV and touches no
  filesystem path.
"""
from __future__ import annotations

import logging

import config
from endpoints import schedule


# ---------------------------------------------------------------------------
# Hand-derived expectations
#
# Every constant below was computed by reading the fixture rows and applying
# the intended dedupe semantics on paper. None of these values was captured
# from a run of the code under test.
# ---------------------------------------------------------------------------

#: Derived by hand from ``sample_schedule_payload``: its ``rowSet`` holds 5
#: rows, and the ``GAME_ID`` column (``headers`` index 2) carries
#: ``"0022500001"`` at rows 1-2, ``"0022500002"`` at rows 3-4, and
#: ``"0022500003"`` at row 5. So 2 + 2 + 1 = 5 input rows describe 3 distinct
#: games, emitted in the order each identifier is first seen.
EXPECTED_SAMPLE_GAME_IDS = ["0022500001", "0022500002", "0022500003"]

#: Derived by hand for the three row-shape guard tests below. Each supplies a
#: 3-row ``rowSet`` whose MIDDLE row is malformed — too short, empty, or
#: carrying a ``None`` ``GAME_ID``. The malformed row is skipped while rows 1
#: and 3 survive, so 3 input rows yield 2 identifiers in first-seen order.
EXPECTED_SURVIVING_GAME_IDS = ["0022500001", "0022500002"]

#: Derived by hand from ``schedule_mixed_game_id_payload``: row 1 carries the
#: bare integer ``22500001`` and row 2 carries the string ``"0022500001"`` —
#: the SAME game under two different wire types — while row 3 is a genuinely
#: different game. Keying on the canonical zero-padded form collapses rows
#: 1-2 into one identifier, so 3 input rows describe 2 distinct games.
#:
#: The arithmetic behind that equivalence: ``str(22500001)`` is
#: ``"22500001"`` (8 characters) while ``str("0022500001")`` is
#: ``"0022500001"`` (10 characters). Two spellings of one identifier are only
#: recognised as equal once both are widened to the canonical 10-character
#: form; keying on the unpadded string form instead yields THREE identifiers
#: for TWO games.
EXPECTED_MIXED_TYPE_GAME_IDS = ["0022500001", "0022500002"]

#: The canonical NBA ``GAME_ID`` width promised by ``enumerate_game_ids``' own
#: docstring: "10-character zero-padded identifiers such as ``0022500001``".
CANONICAL_GAME_ID_LENGTH = 10


# ---------------------------------------------------------------------------
# Canonical form and cardinality — the happy path
# ---------------------------------------------------------------------------


def test_enumerate_game_ids_collapses_five_team_rows_to_three_canonical_ids(
    recording_client, sample_schedule_payload
):
    """Five team-rows MUST collapse to exactly 3 ids, each 10 characters wide.

    Hand derivation from ``sample_schedule_payload`` — never from captured
    output. Its ``headers`` are ``["SEASON_ID", "TEAM_ID", "GAME_ID",
    "GAME_DATE"]``, so the ``GAME_ID`` index is 2, and that column carries
    ``"0022500001"`` at rows 1-2, ``"0022500002"`` at rows 3-4, and
    ``"0022500003"`` at row 5. Therefore 2 + 2 + 1 = 5 input rows describe 3
    distinct games. Deduplication uses insertion-ordered dict keys, so the
    output is ordered by first appearance. Every identifier literal in the
    fixture is already the 10-character zero-padded form.

    Three properties are asserted together: the exact ordered list, the
    5 -> 3 collapse CARDINALITY, and the every-element-is-ten-characters
    CANONICAL FORM. The list equality is what makes the other two
    interpretable, since a count and a width mean little without knowing
    which identifiers were produced.

    Mutation detected: dropping the dedupe — for example rewriting the loop as
    ``ordered_ids = [str(row[game_id_index]) for row in rows]`` — returns all 5
    rows, so ``pipelines.ingest_games`` would fetch two of the three games
    twice and duplicate their rows in ``games.csv``. The equality and
    cardinality assertions both fail on that mutation.
    """
    # Arrange
    client = recording_client(responses={"leaguegamefinder": sample_schedule_payload})

    # Act
    result = schedule.enumerate_game_ids(
        client,
        config.DEFAULT_SEASON,
        config.DEFAULT_SEASON_TYPE,
        config.DEFAULT_LEAGUE_ID,
    )

    # Assert
    assert result == EXPECTED_SAMPLE_GAME_IDS, (
        "5 team-rows over 3 games must enumerate in first-seen order as "
        f"{EXPECTED_SAMPLE_GAME_IDS!r}; got {result!r}"
    )
    assert len(result) == 3, (
        "the 5-row fixture describes exactly 3 distinct games (2 + 2 + 1), so the "
        f"dedupe must collapse 5 rows to 3 ids; got {len(result)} ids in {result!r}"
    )
    assert all(len(game_id) == CANONICAL_GAME_ID_LENGTH for game_id in result), (
        f"every enumerated GAME_ID must be exactly {CANONICAL_GAME_ID_LENGTH} "
        "characters wide (the zero-padded canonical form promised by "
        f"enumerate_game_ids' docstring); got widths "
        f"{[len(game_id) for game_id in result]} for {result!r}"
    )


# ---------------------------------------------------------------------------
# Row-shape guards — a malformed row is skipped, never fatal
# ---------------------------------------------------------------------------


def test_enumerate_game_ids_skips_row_shorter_than_the_game_id_index(recording_client):
    """A row with fewer cells than the ``GAME_ID`` index MUST be skipped.

    Hand derivation: with ``headers = ["SEASON_ID", "TEAM_ID", "GAME_ID",
    "GAME_DATE"]`` the ``GAME_ID`` index is 2. The middle row below carries
    only 2 cells, so the guard's ``game_id_index >= len(row)`` clause
    evaluates ``2 >= 2`` -> ``True`` and the row is skipped. Rows 1 and 3
    survive in first-seen order, giving exactly 2 identifiers from 3 input
    rows.

    Mutation detected: removing the ``game_id_index >= len(row)`` clause from
    the guard makes ``row[game_id_index]`` subscript position 2 of a 2-element
    list, raising ``IndexError`` — one malformed row would abort the entire
    enumeration instead of being skipped.
    """
    # Arrange — envelope built inline so the ragged row is visible beside the
    # header list that gives GAME_ID index 2.
    payload = {
        "resultSets": [
            {
                "name": "LeagueGameFinderResults",
                "headers": ["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"],
                "rowSet": [
                    ["22025", 1610612747, "0022500001", "2025-10-21"],
                    ["22025", 1610612744],
                    ["22025", 1610612738, "0022500002", "2025-10-22"],
                ],
            }
        ]
    }
    client = recording_client(responses={"leaguegamefinder": payload})

    # Act
    result = schedule.enumerate_game_ids(
        client,
        config.DEFAULT_SEASON,
        config.DEFAULT_SEASON_TYPE,
        config.DEFAULT_LEAGUE_ID,
    )

    # Assert
    assert result == EXPECTED_SURVIVING_GAME_IDS, (
        "the 2-cell row cannot reach GAME_ID index 2 and must be skipped, "
        f"leaving exactly {EXPECTED_SURVIVING_GAME_IDS!r}; got {result!r}"
    )
    assert len(result) == 2, (
        "3 input rows minus 1 unreachable-GAME_ID row must yield 2 identifiers; "
        f"got {len(result)} in {result!r}"
    )


def test_enumerate_game_ids_skips_empty_row(recording_client):
    """A completely empty ``[]`` row MUST be skipped, never fatal.

    Hand derivation: the middle row below is the empty list. Rows 1 and 3 are
    well formed and carry distinct identifiers, so 3 input rows yield exactly 2
    identifiers in first-seen order.

    Mutation detected: removing the ``if not row or game_id_index >= len(row)``
    guard **ENTIRELY** makes ``row[game_id_index]`` subscript an empty list and
    raises ``IndexError`` instead of skipping the row.

    Note carefully that the guard is **compound**, and that both of its clauses
    are true for ``[]``: ``not []`` is ``True``, and ``game_id_index >=
    len([])`` is ``2 >= 0`` -> ``True``. Removing only the ``not row`` clause
    would therefore STILL skip this row, so this test does not claim otherwise.
    What the ``not row`` clause adds is short-circuiting on a FALSY row — a
    ``None`` cell in place of a row, for instance — so ``len(row)`` is never
    evaluated for it. It is not a general defence against unsized rows: a
    truthy object with no ``__len__`` still reaches ``len(row)`` and raises
    ``TypeError``.
    """
    # Arrange — envelope built inline so the empty row sits beside the header
    # list that gives GAME_ID index 2.
    payload = {
        "resultSets": [
            {
                "name": "LeagueGameFinderResults",
                "headers": ["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"],
                "rowSet": [
                    ["22025", 1610612747, "0022500001", "2025-10-21"],
                    [],
                    ["22025", 1610612738, "0022500002", "2025-10-22"],
                ],
            }
        ]
    }
    client = recording_client(responses={"leaguegamefinder": payload})

    # Act
    result = schedule.enumerate_game_ids(
        client,
        config.DEFAULT_SEASON,
        config.DEFAULT_SEASON_TYPE,
        config.DEFAULT_LEAGUE_ID,
    )

    # Assert
    assert result == EXPECTED_SURVIVING_GAME_IDS, (
        "the empty row must be skipped without raising, leaving exactly "
        f"{EXPECTED_SURVIVING_GAME_IDS!r}; got {result!r}"
    )
    assert len(result) == 2, (
        "3 input rows minus 1 empty row must yield 2 identifiers; "
        f"got {len(result)} in {result!r}"
    )


def test_enumerate_game_ids_skips_none_game_id_cell(recording_client):
    """A ``None`` in the ``GAME_ID`` column MUST be skipped, not stringified.

    Hand derivation: the middle row below is non-empty and long enough, so it
    passes the row-shape guard; it is rejected one line later by the
    ``if game_id is None`` cell guard. Rows 1 and 3 survive, so 3 input rows
    yield exactly 2 identifiers in first-seen order.

    Mutation detected: removing the ``if game_id is None`` guard lets
    ``str(None)`` produce the bogus identifier ``"None"``, which the canonical
    zero-padding then widens to ``"000000None"`` — a value
    ``pipelines.ingest_games`` would go on to request from the box-score
    endpoint. The exact-list and cardinality assertions both catch that leak
    because the mutated result has 3 elements.

    The third assertion scans each element for the ``"None"`` substring rather
    than testing ``"None" not in result``. That choice is deliberate and it
    matters: because the leaked value is padded to ``"000000None"`` and not
    left as ``"None"``, a plain membership test would pass under the very
    mutation it is supposed to detect.
    """
    # Arrange — envelope built inline so the None cell sits beside the header
    # list that gives GAME_ID index 2.
    payload = {
        "resultSets": [
            {
                "name": "LeagueGameFinderResults",
                "headers": ["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"],
                "rowSet": [
                    ["22025", 1610612747, "0022500001", "2025-10-21"],
                    ["22025", 1610612744, None, "2025-10-21"],
                    ["22025", 1610612738, "0022500002", "2025-10-22"],
                ],
            }
        ]
    }
    client = recording_client(responses={"leaguegamefinder": payload})

    # Act
    result = schedule.enumerate_game_ids(
        client,
        config.DEFAULT_SEASON,
        config.DEFAULT_SEASON_TYPE,
        config.DEFAULT_LEAGUE_ID,
    )

    # Assert
    assert result == EXPECTED_SURVIVING_GAME_IDS, (
        "the None GAME_ID cell must be skipped rather than stringified, leaving "
        f"exactly {EXPECTED_SURVIVING_GAME_IDS!r}; got {result!r}"
    )
    assert len(result) == 2, (
        "3 input rows minus 1 None-GAME_ID row must yield 2 identifiers; "
        f"got {len(result)} in {result!r}"
    )
    assert all("None" not in game_id for game_id in result), (
        "no enumerated identifier may be derived from str(None) — an unguarded "
        "None cell leaks 'None', which zero-padding widens to '000000None'; "
        f"got {result!r}"
    )


# ---------------------------------------------------------------------------
# Envelope-shape guards — degrade to exactly [] rather than raise
# ---------------------------------------------------------------------------


def test_enumerate_game_ids_returns_empty_list_when_row_set_key_is_absent(
    recording_client, caplog
):
    """A table declaring ``GAME_ID`` with NO ``rowSet`` key yields exactly ``[]``.

    The shape under test is the ``"rowSet"`` key being **absent entirely**,
    which is distinct from ``"rowSet": []`` — the key present with an empty
    value. Production normalizes both at
    ``rows = list(target_table.get("rowSet") or [])``.

    Hand derivation: ``resultSets`` is non-empty and the table's ``headers``
    contain ``"GAME_ID"``, so ``target_table`` **IS** found. ``.get("rowSet")``
    returns ``None`` for the absent key, ``None or []`` yields ``[]``, the
    dedupe loop body never executes, and the result is exactly ``[]``.

    Because ``target_table`` is found, **no WARNING fires on this path** — the
    only record emitted after table discovery is the INFO summary carrying
    ``game_count=0``. The third assertion pins that absence, which is what
    distinguishes this branch from the two envelope branches that DO warn.

    Mutation detected: replacing ``target_table.get("rowSet") or []`` with
    ``target_table["rowSet"]``, which raises ``KeyError`` for this envelope
    instead of returning ``[]`` and turns a table that simply carries no rows
    into a fatal enumeration failure.
    """
    # Arrange — a single table with "name" and "headers" but no "rowSet" key.
    payload = {
        "resultSets": [
            {
                "name": "LeagueGameFinderResults",
                "headers": ["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"],
            }
        ]
    }
    client = recording_client(responses={"leaguegamefinder": payload})

    # Act
    with caplog.at_level(logging.INFO, logger="endpoints.schedule"):
        result = schedule.enumerate_game_ids(
            client,
            config.DEFAULT_SEASON,
            config.DEFAULT_SEASON_TYPE,
            config.DEFAULT_LEAGUE_ID,
        )

    # Assert
    assert result == [], (
        "an absent rowSet key must degrade to exactly the empty list, never "
        f"raise and never yield a placeholder; got {result!r}"
    )
    assert len(result) == 0, (
        "a table with headers but no rows contributes zero identifiers; "
        f"got {len(result)} in {result!r}"
    )
    assert [record.levelname for record in caplog.records if record.levelname == "WARNING"] == [], (
        "no WARNING may be emitted on this path: the GAME_ID column WAS found, so "
        "only the INFO summary should fire; got WARNING messages "
        f"{[r.getMessage() for r in caplog.records if r.levelname == 'WARNING']!r}"
    )
    assert any("game_count=0" in record.getMessage() for record in caplog.records), (
        "the INFO summary must report game_count=0 for a zero-row table; got "
        f"messages {[r.getMessage() for r in caplog.records]!r}"
    )


def test_enumerate_game_ids_returns_empty_list_and_warns_when_result_sets_is_a_dict(
    recording_client, caplog
):
    """``resultSets`` sent as a non-empty dict yields ``[]`` plus a WARNING.

    Hand derivation of the mechanism: ``payload.get("resultSets")`` returns the
    dict, and a NON-EMPTY dict is truthy, so the empty-payload branch is not
    taken. Iterating a dict yields its **keys** — plain strings here — so
    ``isinstance(entry, dict)`` is ``False`` for every entry and each is
    skipped. ``target_table`` stays ``None``, so the "no GAME_ID column"
    WARNING fires and exactly ``[]`` is returned.

    The dict is deliberately NON-EMPTY. An empty ``{}`` is falsy and would fire
    the *empty-payload* branch instead, so only a non-empty dict reaches the
    ``isinstance``-False path under test. Note that the ``GAME_ID`` header **is**
    present inside the nested value below, yet because the outer container is a
    dict rather than a list the header-based discovery loop never sees it.

    The message assertion uses lenient substring matching on the phrase "no
    GAME_ID column" only. That phrase is what distinguishes this branch from
    the empty-payload branch, which warns "empty payload" instead; matching the
    whole format string would couple the test to production wording.

    Mutation detected: removing the ``if not isinstance(entry, dict): continue``
    guard hands a plain ``str`` to ``entry.get("headers")``, raising
    ``AttributeError`` instead of degrading to ``[]``.
    """
    # Arrange — a plausible upstream mistake: the tables keyed by name in a
    # dict rather than delivered as a list.
    payload = {
        "resultSets": {
            "LeagueGameFinderResults": {
                "headers": ["SEASON_ID", "TEAM_ID", "GAME_ID", "GAME_DATE"],
                "rowSet": [["22025", 1610612747, "0022500001", "2025-10-21"]],
            }
        }
    }
    client = recording_client(responses={"leaguegamefinder": payload})

    # Act
    with caplog.at_level(logging.WARNING, logger="endpoints.schedule"):
        result = schedule.enumerate_game_ids(
            client,
            config.DEFAULT_SEASON,
            config.DEFAULT_SEASON_TYPE,
            config.DEFAULT_LEAGUE_ID,
        )

    # Assert
    assert result == [], (
        "a dict-shaped resultSets must degrade to exactly the empty list rather "
        f"than raising AttributeError; got {result!r}"
    )
    warnings_emitted = [
        record.getMessage() for record in caplog.records if record.levelname == "WARNING"
    ]
    assert len(warnings_emitted) == 1, (
        "exactly one WARNING must document the unexpected envelope shape; got "
        f"{len(warnings_emitted)}: {warnings_emitted!r}"
    )
    assert any("no GAME_ID column" in message for message in warnings_emitted), (
        "the WARNING must identify the no-GAME_ID-column branch (not the "
        f"empty-payload branch); got {warnings_emitted!r}"
    )


# ---------------------------------------------------------------------------
# Mixed numeric/string GAME_ID canonicalization
# ---------------------------------------------------------------------------


def test_enumerate_game_ids_collapses_mixed_int_and_str_forms_of_one_game(
    recording_client, schedule_mixed_game_id_payload
):
    """One game arriving as both int and str MUST collapse to ONE canonical id.

    The ``schedule_mixed_game_id_payload`` fixture carries 3 rows describing
    only 2 distinct games, with ``headers`` giving ``GAME_ID`` index 2:

    1. ``GAME_ID`` as the bare integer ``22500001``
    2. **the same game**, ``GAME_ID`` as the string ``"0022500001"``
    3. a genuinely different control game, ``"0022500002"``

    Hand derivation: ``str(22500001)`` is ``"22500001"`` — **8** characters —
    while ``str("0022500001")`` is ``"0022500001"`` — **10**. Keying the dedupe
    on the unpadded string form therefore treats rows 1 and 2 as different
    games and returns ``["22500001", "0022500001", "0022500002"]`` — **three
    identifiers for two games** — after which
    ``pipelines.ingest_games.run`` fetches and appends that one game twice,
    duplicating its rows in ``games.csv``. Widening each key to the
    10-character zero-padded canonical form instead yields exactly 2
    identifiers, every element 10 characters wide. The padding is idempotent,
    so ``"0022500001"`` and ``"0022500002"`` pass through byte-identical.

    Ten-character zero-padding is the canonical ``GAME_ID`` form throughout
    this codebase: ``enumerate_game_ids``' own docstring promises
    "10-character zero-padded identifiers such as ``0022500001``",
    ``pipelines/ingest_games.py`` applies the mirror-image ``.str.zfill(10)``
    normalization when matching pending identifiers against CSV cells, and
    ``endpoints/games.py`` warns that stripping leading zeros corrupts the ID
    so callers must preserve the string form upstream. The envelope is
    realistic rather than contrived — the inline commentary in
    ``endpoints/schedule.py`` records that the upstream occasionally returns
    numeric types for identifiers that look numeric — and nothing downstream
    would absorb the duplicate, because ``utils/checkpoint.py::get_pending``
    returns the original elements verbatim in their original order.

    Mutation detected: dropping the ``zfill(10)`` widening from the key
    derivation, leaving a bare ``str(game_id)``. That reintroduces the
    3-identifiers-for-2-games leak and fails all three assertions below.
    """
    # Arrange
    client = recording_client(
        responses={"leaguegamefinder": schedule_mixed_game_id_payload}
    )

    # Act
    result = schedule.enumerate_game_ids(
        client,
        config.DEFAULT_SEASON,
        config.DEFAULT_SEASON_TYPE,
        config.DEFAULT_LEAGUE_ID,
    )

    # Assert
    assert result == EXPECTED_MIXED_TYPE_GAME_IDS, (
        "the same game arriving as int 22500001 and str '0022500001' must collapse "
        f"to one canonical 10-character id, giving {EXPECTED_MIXED_TYPE_GAME_IDS!r}; "
        f"got {result!r}"
    )
    assert len(result) == 2, (
        "3 rows describing 2 distinct games must enumerate 2 identifiers; getting 3 "
        "means the int and str forms of one game were keyed separately and that game "
        f"would be fetched twice; got {len(result)} in {result!r}"
    )
    assert all(len(game_id) == CANONICAL_GAME_ID_LENGTH for game_id in result), (
        f"every enumerated GAME_ID must be exactly {CANONICAL_GAME_ID_LENGTH} "
        "characters wide, including the one that arrived as a bare integer; got "
        f"widths {[len(game_id) for game_id in result]} for {result!r}"
    )
