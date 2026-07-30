"""Malformed-input and type-coercion tests for ``utils.schema_normalizer``.

Covers the **parsing** and **type-coercion** stages of the ingestion
pipeline: the validation gate of
:func:`utils.schema_normalizer.normalize_result_sets` and the pandas
dtype-inference behaviour of the single
``pd.DataFrame(row_set, columns=headers)`` construction site
(``utils/schema_normalizer.py`` line 331).

Why this module exists — the verified gap
-----------------------------------------
The identifiers below were counted across the whole ``tests/`` tree
before this module was written:

* ``_extract_tables`` — **0** references anywhere in ``tests/``.
* ``_require_str`` — **0** references anywhere in ``tests/``.
* ``_require_list`` — **0** references anywhere in ``tests/``.

And inside the sibling module ``test_schema_normalizer.py``
(596 lines / 27 tests):

* ``dtype``, ``int64``, ``float64``, ``isna``, ``nan``, ``to_numeric``
  — **0** hits each. Repository-wide, ``dtypes[`` has **0** hits
  anywhere in ``tests/``.

No test in this repository asserted a dtype before this module. Three
private validation helpers had never been exercised by anything, so
deleting any one of their ``raise`` statements would not have failed a
single test. The objective here is therefore **fault detection
(mutation resistance)**, not a coverage percentage: this project ships
no coverage instrument by design (``pytest-cov`` and ``coverage`` are
absent and forbidden by the ``tests/conftest.py`` do-not list), so no
percentage is claimed anywhere.

Test-file contract highlights
-----------------------------
* **Private helpers are reached ONLY through the public entry point.**
  ``_extract_tables``, ``_require_str``, and ``_require_list`` receive
  their first-ever coverage here, exclusively by feeding malformed
  envelopes to the public ``normalize_result_sets``. No private helper
  is imported, promoted, or given a test-only hook, so no production
  source file changes. Contrast the sibling module, which aliases the
  module as ``sn_module`` to reach ``_snake_case`` directly — that
  pattern is deliberately NOT copied here.
* **Seven rejection branches, seven exact messages.** Each envelope in
  the shared ``malformed_result_set_payloads`` fixture is the minimal
  shape that trips exactly one guard, and each expected message is the
  verbatim production string with its source line cited in
  :data:`EXPECTED_MESSAGES`. Because ``pytest.raises(match=...)``
  performs a :func:`re.search`, every pattern is wrapped in
  :func:`re.escape` — the messages contain ``'``, ``[``, ``]``, ``(``,
  and ``)``, any of which would silently alter an unescaped pattern and
  make the assertion pass for the wrong reason. Two cases additionally
  assert full-string ``==`` equality, which is strictly stronger than a
  containment match.
* **The dtype and null contract is pinned exactly.** An integer column
  containing ``None`` upcasts to ``float64`` with ``NaN``, while an
  object column preserves ``None`` as the literal ``None`` — yet
  ``.isna()`` reports ``True`` for both. All three facts (dtype, value
  identity, null mask) are asserted together so the asymmetry cannot
  drift unnoticed.
* **No value repair.** ``_build_dataframe`` contains no ``astype``, no
  ``to_numeric``, no ``fillna``, and no ``convert_dtypes``; a numeric
  supplied as the string ``"31"`` therefore stays an ``object``-dtype
  ``str``. The normalizer's job is faithful structural translation, and
  that is asserted rather than assumed.
* **Duplicate names are uniquified, never overwritten.** Only the
  genuinely new *collision* case is covered here: an upstream name that
  already ends in a digit (``PlayByPlay2``) colliding with the
  suffixing scheme applied to a repeated ``PlayByPlay``. The simple
  ``["x", "x_2", "x_3"]`` case is already covered by the sibling module
  and is deliberately not duplicated.
* **Every expected value is hand-derived, never captured.** Each
  constant below is justified by the production source lines cited
  beside it — no assertion compares against recorded output, no
  snapshot or golden file is used, and
  :func:`pandas.testing.assert_frame_equal` is never applied to a
  code-produced frame.
* **No test doubles at all.** ``normalize_result_sets`` is a pure
  function over a JSON-like envelope with no I/O, so mocking at the
  boundary of the code under test would violate the repository
  convention. Inputs are literal dicts plus the one shared fixture; no
  mock, spy, monkeypatch, filesystem, network, or metrics access
  appears here.
* **Offline tier, no marker.** ``pytest.ini`` registers exactly two
  markers (``integration`` and ``invariant``) under
  ``--strict-markers``, so this module carries none and runs in every
  invocation mode. It is auto-collected without any configuration
  change because ``pytest.ini`` sets ``python_files = test_*.py``.
* **A focused sibling, not an edit.** ``test_schema_normalizer.py`` is
  treated as frozen: its 27 tests, including two deliberately lenient
  row-mismatch assertions, must keep passing byte-for-byte. This module
  *supplements* those lenient assertions with verbatim-message
  equivalents rather than tightening them in place.
* **Rule 4 is deliberately absent.** Nested-cell rejection and generic
  flatness are owned by
  ``tests/invariants/test_rule4_no_nested_cells.py`` (and additionally
  covered in the sibling unit module); re-asserting them here would add
  no fault detection.

Authoritative references
------------------------
* AAP §0.2.2.1 — parsing-stage gap inventory (three zero-reference
  helpers, uncovered non-string-headers and row-not-a-sequence guards).
* AAP §0.2.2.2 — type-coercion gap inventory (dtype inference, the
  ``None``-versus-``NaN`` distinction, no string-to-numeric coercion).
* AAP §0.4.2.3 — the seven exact rejection messages and the
  duplicate-name uniquification derivation.
* AAP §0.5.1 / §0.5.2 / §0.8.1 — this module's CREATE mandate and the
  constraints C1 (no snapshots), C2 (no source modification), C3 (no
  weakening of existing tests), and C4 (no smoke tests).
* ``utils/schema_normalizer.py`` — the production contract; every line
  number cited below was read from that file.
* ``tests/conftest.py`` line 1409 — the shared
  ``malformed_result_set_payloads`` fixture consumed by the
  parametrized error-case matrix.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

import pandas as pd
import pytest

from utils.schema_normalizer import normalize_result_sets


# ---------------------------------------------------------------------------
# Hand-derived expected values (kept separate from fixture INPUT values)
# ---------------------------------------------------------------------------
#
# Every constant in this section is derived from the production source,
# not from a recorded run. The line references point at
# ``utils/schema_normalizer.py``, and the derivation is spelled out so a
# reviewer can verify each literal without executing anything.
# ---------------------------------------------------------------------------

#: Verbatim ``ValueError`` text for each rejection branch, keyed by the
#: case names used in ``tests/conftest.py::malformed_result_set_payloads``.
#:
#: Each string is the f-string at the cited line rendered with that
#: envelope's values. The result-set name is ``"t"`` throughout because
#: ``_snake_case("t")`` is ``"t"``: neither ``_CAMEL_BOUNDARY_1``
#: (``(.)([A-Z][a-z]+)``, L396) nor ``_CAMEL_BOUNDARY_2``
#: (``([a-z0-9])([A-Z])``, L400) matches a single lowercase character,
#: and ``.lower().strip("_")`` leaves it untouched (L433-435).
EXPECTED_MESSAGES: Dict[str, str] = {
    # L326-329: f"Result set '{name}' row {idx} has {len(row)} values "
    #           f"but {expected_width} headers are declared"
    # Envelope: headers ["A", "B", "C"] (width 3) against rowSet [[1, 2]]
    # (row 0, length 2) -> idx=0, len(row)=2, expected_width=3.
    "row_width_mismatch": (
        "Result set 't' row 0 has 2 values but 3 headers are declared"
    ),
    # L233-236: f"Result set is missing required string field '{key}' "
    #           f"(got {type(value).__name__})"
    # Envelope omits "name" entirely, so ``table.get("name")`` (L231) is
    # ``None`` and ``type(None).__name__`` is "NoneType".
    "missing_name": (
        "Result set is missing required string field 'name' (got NoneType)"
    ),
    # L267-270: f"Result set field '{key}' must be a list; "
    #           f"got {type(value).__name__}"
    # Envelope supplies headers as the dict {"A": 1} -> "dict".
    "headers_not_list": (
        "Result set field 'headers' must be a list; got dict"
    ),
    # L267-270 again, reached for the ``rowSet`` key on the next line
    # (L132). The envelope omits "rowSet", so ``table.get("rowSet")`` is
    # ``None`` -> "NoneType". An EMPTY list would be valid here
    # (L244-246): only a non-list trips this guard.
    "missing_row_set": (
        "Result set field 'rowSet' must be a list; got NoneType"
    ),
    # L135-137: f"Result set '{name}' contains non-string headers: {headers}"
    # ``{headers}`` interpolates the list's repr, so ["A", 7] renders as
    # ['A', 7] -- note the single quotes around A and the space after the
    # comma. This guard (L134) runs AFTER both _require_list calls, so the
    # envelope must still carry a valid name and a list-typed rowSet.
    "non_string_header": (
        "Result set 't' contains non-string headers: ['A', 7]"
    ),
    # L321-324: f"Result set '{name}' row {idx} is {type(row).__name__}, "
    #           f"expected list/tuple"
    # The row TYPE check (L320) precedes the row WIDTH check (L325), so a
    # dict row raises THIS message and never the width message, even
    # though {"A": 1} also has a length that could mismatch.
    "row_not_sequence": (
        "Result set 't' row 0 is dict, expected list/tuple"
    ),
    # L193-195: f"'resultSets' must be a list or dict, got {type(raw).__name__}"
    # The envelope supplies the str "not-a-list" -> "str". This is the
    # _extract_tables TYPE guard, distinct from the "Payload contains no
    # result sets" message (L119-122) raised when the extracted table
    # list is empty.
    "result_sets_wrong_type": (
        "'resultSets' must be a list or dict, got str"
    ),
}

#: One malformed envelope per rejection branch. Fixed by the seven-entry
#: table above; asserted against the shared fixture so a dropped envelope
#: cannot silently shrink the parametrized matrix.
EXPECTED_REJECTION_BRANCH_COUNT = 7

#: Deterministic ``(case, expected_message)`` pairs for parametrization.
#: A fixture cannot be referenced inside ``@pytest.mark.parametrize``, so
#: the local table drives the matrix and the fixture is indexed by case
#: name inside the test body. Sorted for a stable test-id ordering.
MALFORMED_CASES: Tuple[Tuple[str, str], ...] = tuple(
    sorted(EXPECTED_MESSAGES.items())
)

#: Headers of the inline dtype-probe envelope, in upstream order. The
#: normalizer passes ``headers`` straight through as ``columns`` (L331),
#: so column identity and order must survive verbatim.
EXPECTED_DTYPE_COLUMNS: List[str] = ["PLAYER_ID", "PTS", "NOTE"]

#: pandas dtype inference for ``pd.DataFrame(row_set, columns=headers)``
#: -- the sole construction site (L331) -- over the rowSet
#: ``[[203999, None, "ok"], [1629029, 31, None]]``:
#:
#: * ``PLAYER_ID`` holds two Python ints and nothing else -> ``int64``.
#: * ``PTS`` holds one int and one ``None``. ``None`` is a missing-value
#:   sentinel among numerics, and ``int64`` cannot represent it, so the
#:   column upcasts to ``float64`` and the hole becomes ``NaN``.
#: * ``NOTE`` holds a ``str`` and a ``None``, which share no numeric
#:   type, so the column stays ``object`` and ``None`` is preserved as
#:   the literal ``None`` rather than converted to ``NaN``.
EXPECTED_DTYPES: Dict[str, str] = {
    "PLAYER_ID": "int64",
    "PTS": "float64",
    "NOTE": "object",
}

#: ``.isna()`` masks for the dtype-probe frame, in row order. Both the
#: ``float64`` ``NaN`` and the ``object`` ``None`` report ``True``, which
#: is exactly why the null mask alone cannot distinguish them and the
#: dtype plus value identity must be asserted alongside it.
EXPECTED_PTS_NULL_MASK: List[bool] = [True, False]
EXPECTED_NOTE_NULL_MASK: List[bool] = [False, True]

#: Keys produced by the duplicate-name COLLISION envelope, whose tables
#: are named ``PlayByPlay``, ``PlayByPlay2``, ``PlayByPlay`` in that
#: order. Derivation, traced through both ``_snake_case`` regexes
#: (L396 / L400 / L433-435) and the uniquifier (L142-147):
#:
#: * ``"PlayByPlay"``: ``_CAMEL_BOUNDARY_1`` matches ``"yBy"`` at
#:   offset 3 -> ``"Play_ByPlay"``; ``_CAMEL_BOUNDARY_2`` then matches
#:   ``"yP"`` -> ``"Play_By_Play"``; lower/strip -> ``"play_by_play"``.
#: * ``"PlayByPlay2"``: the same two substitutions give
#:   ``"Play_By_Play2"`` -> ``"play_by_play2"``. It is a genuinely
#:   DIFFERENT upstream name, so it takes a bare key and does not
#:   collide.
#: * The third table snake-cases to ``"play_by_play"``, which is already
#:   a key, so ``seen_names["play_by_play"] = seen_names.get(
#:   "play_by_play", 1) + 1`` evaluates to ``2`` and the key becomes
#:   ``f"play_by_play_{2}"``.
EXPECTED_COLLISION_KEYS: List[str] = [
    "play_by_play",
    "play_by_play2",
    "play_by_play_2",
]

#: First-column cell of each collision frame, in key order. The three
#: distinct values prove no table was lost and none was overwritten.
EXPECTED_COLLISION_CELLS: List[int] = [1, 2, 3]


# ---------------------------------------------------------------------------
# Type coercion — dtype inference at the sole DataFrame construction site
# ---------------------------------------------------------------------------
#
# ``_build_dataframe`` ends in a single
# ``pd.DataFrame(row_set, columns=headers)`` call (L331) and performs no
# value repair whatsoever: the helper body contains no ``astype``, no
# ``to_numeric``, no ``fillna``, and no ``convert_dtypes``. Whatever
# pandas infers from the literal rowSet is therefore the pipeline's real
# and intended semantics, and it propagates unchanged into every CSV
# artifact the writer emits.
#
# The three tests below pin that inference exactly, because the two
# plausible "helpful" mutations here are silent data corruption rather
# than crashes: a ``fillna(0)`` would turn a player's MISSING points
# into ZERO points, and a ``pd.to_numeric`` would rewrite identifier and
# measurement columns on disk. Neither would fail any pre-existing test.
# ---------------------------------------------------------------------------


def test_mixed_row_set_infers_exact_dtype_per_column() -> None:
    """Each column's inferred dtype and the column order are pinned exactly.

    Hand-derived from :data:`EXPECTED_DTYPES`: an all-int column is
    ``int64``, an int column holding ``None`` upcasts to ``float64``, and
    a text column holding ``None`` stays ``object``. Dtypes are compared
    field by field rather than as a whole ``Series`` -- a ``Series ==``
    comparison yields a boolean ``Series`` whose truthiness is not the
    assertion anyone intends.

    Mutation detected: any silent widening or narrowing of an inferred
    type (for example inserting ``convert_dtypes`` or ``astype(str)``),
    and any reordering or renaming of the payload's declared headers.
    """
    # Arrange -- 203999 is Jokić and 1629029 is Dončić, matching the
    # real-world identifier space used by the shared payload fixtures.
    payload: Dict[str, Any] = {
        "resultSets": [
            {
                "name": "DtypeProbe",
                "headers": ["PLAYER_ID", "PTS", "NOTE"],
                "rowSet": [[203999, None, "ok"], [1629029, 31, None]],
            }
        ]
    }

    # Act
    out = normalize_result_sets(payload)
    df = out["dtype_probe"]

    # Assert -- column identity and order survive the round trip.
    assert list(df.columns) == EXPECTED_DTYPE_COLUMNS, (
        f"headers must pass through as columns verbatim; expected "
        f"{EXPECTED_DTYPE_COLUMNS} but got {list(df.columns)}"
    )
    assert df.shape == (2, 3), (
        f"two rows of three headers must yield a (2, 3) frame; got {df.shape}"
    )
    # Assert -- one exact dtype comparison per column.
    for column, expected_dtype in EXPECTED_DTYPES.items():
        observed_dtype = str(df.dtypes[column])
        assert observed_dtype == expected_dtype, (
            f"column {column!r} must infer dtype {expected_dtype!r}; "
            f"got {observed_dtype!r}"
        )


def test_none_upcasts_to_nan_in_numeric_column_but_survives_in_object_column() -> None:
    """``None`` becomes ``NaN`` among numerics yet stays ``None`` in an object column.

    The asymmetry is the point: ``PTS`` holds ``[None, 31]`` and upcasts
    to ``float64`` with a real ``NaN``, while ``NOTE`` holds
    ``["ok", None]`` and keeps the literal ``None`` -- and ``.isna()``
    reports ``True`` for both, so the null mask alone cannot tell them
    apart. Dtype, value identity, and null mask are therefore asserted
    together. ``pd.isna`` is used for the ``NaN`` cell because ``NaN``
    compares unequal to everything including itself, which would make an
    ``== float("nan")`` assertion vacuously false.

    Mutation detected: a "helpful" ``pd.to_numeric(..., errors="coerce")``
    or a ``fillna(0)`` inside ``_build_dataframe`` -- the latter would
    silently convert a player's MISSING points into ZERO points, a
    data-corruption bug no pre-existing test can see.
    """
    # Arrange
    payload: Dict[str, Any] = {
        "resultSets": [
            {
                "name": "DtypeProbe",
                "headers": ["PLAYER_ID", "PTS", "NOTE"],
                "rowSet": [[203999, None, "ok"], [1629029, 31, None]],
            }
        ]
    }

    # Act
    df = normalize_result_sets(payload)["dtype_probe"]
    player_ids = df["PLAYER_ID"].tolist()
    points = df["PTS"].tolist()
    notes = df["NOTE"].tolist()

    # Assert -- the non-null integer column is untouched.
    assert str(df.dtypes["PLAYER_ID"]) == "int64", (
        f"an all-integer column must stay int64; got {df.dtypes['PLAYER_ID']!r}"
    )
    assert player_ids == [203999, 1629029], (
        f"identifier values must pass through unchanged; got {player_ids}"
    )

    # Assert -- the numeric column upcast, and the hole is a real NaN.
    assert str(df.dtypes["PTS"]) == "float64", (
        f"an integer column containing None must upcast to float64; "
        f"got {df.dtypes['PTS']!r}"
    )
    assert pd.isna(points[0]), (
        f"the missing PTS cell must be NaN, not a filled value; got {points[0]!r}"
    )
    assert points[1] == 31.0, (
        f"the present PTS cell must survive the upcast as 31.0; got {points[1]!r}"
    )
    assert df["PTS"].isna().tolist() == EXPECTED_PTS_NULL_MASK, (
        f"PTS null mask must be {EXPECTED_PTS_NULL_MASK}; "
        f"got {df['PTS'].isna().tolist()}"
    )

    # Assert -- the object column preserved None by identity, not as NaN.
    assert str(df.dtypes["NOTE"]) == "object", (
        f"a text column containing None must stay object; got {df.dtypes['NOTE']!r}"
    )
    assert notes[0] == "ok", (
        f"the present NOTE cell must be preserved verbatim; got {notes[0]!r}"
    )
    assert notes[1] is None, (
        f"the missing NOTE cell must remain the literal None, not NaN; "
        f"got {notes[1]!r} of type {type(notes[1]).__name__}"
    )
    assert df["NOTE"].isna().tolist() == EXPECTED_NOTE_NULL_MASK, (
        f"NOTE null mask must be {EXPECTED_NOTE_NULL_MASK}; "
        f"got {df['NOTE'].isna().tolist()}"
    )


def test_numeric_supplied_as_string_is_not_coerced_to_a_number() -> None:
    """A numeric arriving as the string ``"31"`` stays an ``object``-dtype ``str``.

    ``_build_dataframe`` performs faithful structural translation, not
    value repair: its body (L274-331) contains no ``astype``, no
    ``to_numeric``, no ``fillna``, and no ``convert_dtypes``. A column of
    two strings therefore infers ``object``, and each cell remains the
    exact ``str`` the envelope supplied. The neighbouring ``PLAYER_ID``
    column proves genuine integers are still inferred as ``int64``, so
    the ``object`` result is specific to the string input rather than a
    blanket loss of inference.

    Mutation detected: introducing ``pd.to_numeric`` "for convenience",
    which would silently rewrite the on-disk representation of every
    downstream CSV artifact.
    """
    # Arrange
    payload: Dict[str, Any] = {
        "resultSets": [
            {
                "name": "CoercionProbe",
                "headers": ["PLAYER_ID", "PTS"],
                "rowSet": [[203999, "31"], [1629029, "28"]],
            }
        ]
    }

    # Act
    df = normalize_result_sets(payload)["coercion_probe"]
    points = df["PTS"].tolist()

    # Assert -- no string-to-numeric coercion took place.
    assert str(df.dtypes["PTS"]) == "object", (
        f"string-valued cells must leave the column dtype object; "
        f"got {df.dtypes['PTS']!r}"
    )
    assert points == ["31", "28"], (
        f"string cells must be preserved verbatim; got {points}"
    )
    assert [type(value) is str for value in points] == [True, True], (
        f"every PTS cell must remain a str, not an int; got types "
        f"{[type(value).__name__ for value in points]}"
    )

    # Assert -- real integers in a sibling column are still inferred.
    assert str(df.dtypes["PLAYER_ID"]) == "int64", (
        f"a genuinely integral column must still infer int64; "
        f"got {df.dtypes['PLAYER_ID']!r}"
    )


# ---------------------------------------------------------------------------
# Duplicate result-set names — the suffix COLLISION case
# ---------------------------------------------------------------------------
#
# Duplicate names are a duplicate-record concern inside the PARSING
# stage: two tables sharing a name must both survive, because the
# uniquifier (L142-147) is the only thing standing between a repeated
# upstream name and one table's data vanishing from the output.
#
# The plain repeat case (three tables all named "X" -> "x", "x_2",
# "x_3") is already covered by the sibling module and is NOT duplicated
# here. What is new is the COLLISION: an upstream table whose own name
# already ends in the digit the uniquifier would append. ``PlayByPlay2``
# snake-cases to ``play_by_play2`` while a repeated ``PlayByPlay``
# becomes ``play_by_play_2`` -- the underscore is the only thing
# separating them, which is exactly the kind of near-miss that a naive
# suffixing scheme collapses.
# ---------------------------------------------------------------------------


def test_duplicate_name_colliding_with_numeric_suffix_keeps_every_table() -> None:
    """A repeated name and a digit-suffixed sibling both survive as distinct keys.

    Hand-derived in :data:`EXPECTED_COLLISION_KEYS`: ``PlayByPlay`` ->
    ``play_by_play``, ``PlayByPlay2`` -> ``play_by_play2`` (a different
    upstream name, so it keeps a bare key), and the repeated
    ``PlayByPlay`` -> ``play_by_play_2`` via
    ``seen_names.get("play_by_play", 1) + 1``. The whole ordered key list
    is asserted rather than membership, and each frame carries a distinct
    cell value so overwriting cannot hide behind a correct key count.

    Mutation detected: replacing the suffixing with plain assignment --
    the third table would overwrite the first and one table's data would
    vanish entirely; or renumbering the uniquifier to start at one, which
    would make the repeat collide with the ``play_by_play2`` sibling.
    """
    # Arrange -- the third entry repeats the first name exactly.
    payload: Dict[str, Any] = {
        "resultSets": [
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[1]]},
            {"name": "PlayByPlay2", "headers": ["A"], "rowSet": [[2]]},
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[3]]},
        ]
    }

    # Act
    out = normalize_result_sets(payload)

    # Assert -- the complete ordered key list, not a membership check.
    assert list(out.keys()) == EXPECTED_COLLISION_KEYS, (
        f"duplicate names must be uniquified without colliding with a "
        f"digit-suffixed sibling; expected {EXPECTED_COLLISION_KEYS} but got "
        f"{list(out.keys())}"
    )
    assert len(out) == len(EXPECTED_COLLISION_KEYS), (
        f"all three tables must survive; expected "
        f"{len(EXPECTED_COLLISION_KEYS)} frames but got {len(out)}"
    )

    # Assert -- distinguishing cells prove nothing was overwritten.
    observed_cells = [int(out[key].iloc[0, 0]) for key in EXPECTED_COLLISION_KEYS]
    assert observed_cells == EXPECTED_COLLISION_CELLS, (
        f"each key must map to its own table's data; expected cells "
        f"{EXPECTED_COLLISION_CELLS} but got {observed_cells}"
    )


# ---------------------------------------------------------------------------
# Malformed envelopes — the seven exact ValueError messages
# ---------------------------------------------------------------------------
#
# This section gives ``_extract_tables`` (L155), ``_require_str``
# (L208), and ``_require_list`` (L240) their first-ever coverage, plus
# the previously unexercised non-string-headers branch (L134) and
# row-not-a-sequence branch (L320). All five are reached exclusively
# through the public ``normalize_result_sets``; no private helper is
# imported and no production source file is touched.
#
# Guard order inside the per-table loop (L129-140) determines which
# envelope trips which branch, so the shared fixture's shapes are
# minimal but deliberate:
#
#   L130  name    = _snake_case(_require_str(table, "name"))
#   L131  headers = _require_list(table, "headers")
#   L132  row_set = _require_list(table, "rowSet")
#   L134  non-string headers check   (AFTER both _require_list calls)
#   L139  _build_dataframe -> row TYPE check (L320) then WIDTH (L325)
#   L140  _assert_rule4_flat          (Rule 4; covered elsewhere)
#
# Because ``pytest.raises(match=...)`` runs a ``re.search``, every
# pattern below is wrapped in ``re.escape``: the messages contain ``'``,
# ``[``, ``]``, ``(``, and ``)``, and an unescaped ``[`` or ``(`` would
# become a character class or capture group -- a pattern that still
# matches, but for the wrong reason. Two branches are additionally
# asserted with full-string ``==`` equality, which is strictly stronger
# than a containment search.
# ---------------------------------------------------------------------------


def test_malformed_fixture_supplies_exactly_one_envelope_per_rejection_branch(
    malformed_result_set_payloads: Dict[str, Any],
) -> None:
    """The shared fixture's case names match this module's expected-message table exactly.

    Guards the parametrized matrix below against silent shrinkage: the
    matrix is driven by :data:`EXPECTED_MESSAGES` and indexes the fixture
    by case name, so a renamed or removed envelope would otherwise
    surface as a confusing ``KeyError`` instead of a clear contract
    failure. Exact set equality and an exact count are asserted, not
    membership or a lower bound.

    Mutation detected: dropping or renaming a rejection-branch envelope
    in ``tests/conftest.py``, which would reduce the error-case matrix
    below seven and leave a validation guard unexercised again.
    """
    # Arrange / Act -- the fixture IS the value under test.
    observed_cases = set(malformed_result_set_payloads)

    # Assert
    assert observed_cases == set(EXPECTED_MESSAGES), (
        f"fixture case names {sorted(observed_cases)} must match the "
        f"expected-message table {sorted(EXPECTED_MESSAGES)}"
    )
    assert len(malformed_result_set_payloads) == EXPECTED_REJECTION_BRANCH_COUNT, (
        f"expected {EXPECTED_REJECTION_BRANCH_COUNT} rejection branches, "
        f"got {len(malformed_result_set_payloads)}"
    )


@pytest.mark.parametrize(("case", "expected_message"), MALFORMED_CASES)
def test_malformed_envelope_raises_valueerror_with_exact_message(
    case: str,
    expected_message: str,
    malformed_result_set_payloads: Dict[str, Any],
) -> None:
    """Each malformed envelope raises ``ValueError`` carrying its verbatim message.

    The seven expected strings in :data:`EXPECTED_MESSAGES` are the
    production f-strings rendered by hand, each annotated with its source
    line. ``re.escape`` keeps the regex literal so the punctuation in the
    messages cannot silently relax the pattern. The error class is pinned
    too: a non-dict payload raises ``TypeError`` at L112-115, so
    ``ValueError`` here confirms the DATA-error path rather than the
    programmer-error path.

    Mutation detected: deleting or loosening any validation guard -- a
    removed ``raise`` would let a malformed envelope through silently --
    and any drift in the operator-facing message text, such as dropping
    the result-set name or the offending row index.
    """
    # Arrange
    payload = malformed_result_set_payloads[case]

    # Act / Assert -- containment match on the escaped literal.
    with pytest.raises(ValueError, match=re.escape(expected_message)) as exc_info:
        normalize_result_sets(payload)

    # Assert -- and the full string, which is strictly stronger.
    assert str(exc_info.value) == expected_message, (
        f"case {case!r} must raise exactly {expected_message!r}; "
        f"got {str(exc_info.value)!r}"
    )
    # Assert -- exactly ValueError, not a subclass and not TypeError.
    assert type(exc_info.value) is ValueError, (
        f"case {case!r} is a DATA error and must raise ValueError itself, not "
        f"{type(exc_info.value).__name__}"
    )


def test_row_narrower_than_headers_reports_index_count_and_declared_width(
    malformed_result_set_payloads: Dict[str, Any],
) -> None:
    """A two-value row against three declared headers raises the complete verbatim message.

    Hand-derived from L326-329 with the fixture's own numbers: row index
    ``0``, ``len(row) == 2``, and ``expected_width == len(headers) == 3``.
    The sibling module already covers this branch, but only leniently --
    one test asserts the snake_case set name appears and another accepts
    either the word "row" or the word "index". This test supplements
    those with full-string equality, which is the new information; the
    lenient assertions are left untouched.

    Mutation detected: changing the reported row index, the reported
    value count, or the declared-header count; swapping the width check
    ahead of the row-type check; or moving the width check somewhere that
    no longer names the offending row.
    """
    # Arrange
    payload = malformed_result_set_payloads["row_width_mismatch"]
    expected_message = EXPECTED_MESSAGES["row_width_mismatch"]

    # Act
    with pytest.raises(ValueError) as exc_info:
        normalize_result_sets(payload)

    # Assert -- exact, anchored, non-regex comparison.
    assert str(exc_info.value) == expected_message, (
        f"row-width mismatch must raise exactly {expected_message!r}; "
        f"got {str(exc_info.value)!r}"
    )
    assert type(exc_info.value) is ValueError, (
        f"a shape mismatch is a DATA error and must raise ValueError, not "
        f"{type(exc_info.value).__name__}"
    )


def test_row_supplied_as_dict_trips_the_type_guard_before_the_width_guard(
    malformed_result_set_payloads: Dict[str, Any],
) -> None:
    """A dict row raises the row-TYPE message, never the row-WIDTH message.

    Inside ``_build_dataframe`` the ``isinstance(row, (list, tuple))``
    check (L320) precedes the ``len(row) != expected_width`` check
    (L325). The fixture's envelope declares one header and supplies the
    row ``{"A": 1}``, whose length is also one -- so the width guard
    would not fire even if it ran first. Asserting the type message and
    the absence of the width wording together pins the ordering itself,
    not merely the outcome.

    Mutation detected: reordering the two guards, or replacing the type
    check with a bare ``len()`` call, which would raise ``TypeError`` on
    an unsized row instead of a diagnostic ``ValueError``.
    """
    # Arrange
    payload = malformed_result_set_payloads["row_not_sequence"]
    expected_message = EXPECTED_MESSAGES["row_not_sequence"]

    # Act
    with pytest.raises(ValueError) as exc_info:
        normalize_result_sets(payload)
    message = str(exc_info.value)

    # Assert -- the type message verbatim ...
    assert message == expected_message, (
        f"a dict row must raise exactly {expected_message!r}; got {message!r}"
    )
    # ... and demonstrably NOT the width message that follows it.
    assert "headers are declared" not in message, (
        f"the row-type guard must fire before the row-width guard; "
        f"got the width wording in {message!r}"
    )
