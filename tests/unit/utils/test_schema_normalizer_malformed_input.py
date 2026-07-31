"""Malformed-input and type-coercion tests for ``utils.schema_normalizer``.

Covers the **parsing** and **type-coercion** stages of the ingestion
pipeline: the validation gate of
:func:`utils.schema_normalizer.normalize_result_sets` and the pandas
dtype-inference behaviour of the single
``pd.DataFrame(row_set, columns=headers)`` construction site
(``utils/schema_normalizer.py`` line 331).

Contracts pinned here
---------------------
* **Private helpers are reached ONLY through the public entry point.**
  ``_extract_tables``, ``_require_str``, and ``_require_list`` are
  exercised exclusively by feeding malformed envelopes to the public
  ``normalize_result_sets``. No private helper is imported, promoted, or
  given a test-only hook, so no production source file changes.
* **Seven rejection branches, seven exact messages.** Each envelope in
  the shared ``malformed_result_set_payloads`` fixture is the minimal
  shape that trips exactly one guard, and each expected message is the
  verbatim production string with its source line cited in
  :data:`EXPECTED_MESSAGES`. Because ``pytest.raises(match=...)``
  performs a :func:`re.search`, every pattern is wrapped in
  :func:`re.escape` — the messages contain ``'``, ``[``, ``]``, ``(``,
  and ``)``, any of which would silently alter an unescaped pattern and
  make the assertion pass for the wrong reason. Every case additionally
  asserts full-string ``==`` equality alongside the containment match,
  and the row-width branch carries a dedicated boundary test that does
  the same — both strictly stronger than a containment search.
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
* **Duplicate names are uniquified, never overwritten.** Two strictly
  different cases are covered here, and confusing them is precisely what
  let a real defect (CD-2, below) survive: the **near miss**, where an
  upstream name already ends in a digit (``PlayByPlay2`` ->
  ``play_by_play2``) and merely resembles the generated
  ``play_by_play_2`` without ever occupying it; and the **true
  collision**, where an upstream name snake-cases to exactly the key the
  uniquifier wants to generate (``PlayByPlay_2`` -> ``play_by_play_2``)
  and therefore does occupy it.

Confirmed defect CD-2 and its minimal fix
-----------------------------------------
Constraint C2 permits exactly one kind of non-test source change — a
minimal fix for a genuine bug, called out explicitly with the failing
case. This module documents one such fix.

* **Site.** ``utils/schema_normalizer.py``, the duplicate-name
  uniquifier inside ``normalize_result_sets``.
* **Failing case that motivated it.** A ``playbyplayv2`` envelope whose
  ``resultSets`` are named, in order, ``PlayByPlay``, ``PlayByPlay_2``,
  ``PlayByPlay`` — three tables carrying three distinct rows.
* **Behaviour before the fix.** ``normalize_result_sets`` returned only
  ``['play_by_play', 'play_by_play_2']`` — **two frames for three
  upstream tables** — because the third table's generated key
  ``play_by_play_2`` was already held by the second table and the
  uniquifier assigned it unconditionally. The second table's DataFrame
  was replaced by the third's and vanished from the mapping. **No
  exception was raised**, so a schema-drifting or hostile upstream could
  delete an entire result table from every downstream DataFrame and CSV
  artifact while the pipeline reported success (CWE-20, integrity
  overwrite).
* **Behaviour after the fix.** ``['play_by_play', 'play_by_play_2',
  'play_by_play_3']``, with cells ``1``, ``2``, ``3`` respectively — one
  frame per upstream table, each holding its own data.
* **The change.** The generated ordinal is a starting point rather than
  a verdict: a ``while`` probe advances it until the candidate key is
  unoccupied. No signature, return type, import, raise, or control-flow
  branch outside the pre-existing duplicate branch was touched, and the
  McCabe complexity of ``normalize_result_sets`` remains far below the
  ceiling of 12.
* **Behaviour deliberately preserved.** Both hand-derived key lists in
  AAP §0.4.2.3 are unchanged by the fix: three tables all named ``X``
  still yield ``['x', 'x_2', 'x_3']`` (asserted by the sibling
  ``test_schema_normalizer.py``), and the near-miss envelope still
  yields ``['play_by_play', 'play_by_play2', 'play_by_play_2']``. Only
  the previously destructive branch behaves differently.
* **The matched test.**
  :func:`test_duplicate_name_whose_generated_key_is_occupied_keeps_every_table`
  fails against the unfixed source and passes against the fixed one;
  :func:`test_duplicate_name_probes_past_consecutive_occupied_keys_without_loss`
  additionally rules out a one-shot retry that would still lose a table.
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
* **Rule 4 is deliberately absent.** Nested-cell rejection and generic
  flatness are owned by
  ``tests/invariants/test_rule4_no_nested_cells.py``; re-asserting them
  here would add no fault detection.

Mutation resistance — not a coverage percentage — is the acceptance bar,
so every test below names the specific change it detects. This project
ships no coverage instrument by design (``pytest-cov`` and ``coverage``
are absent and forbidden by the ``tests/conftest.py`` do-not list), and
no percentage is claimed anywhere.

Authoritative references
------------------------
* ``utils/schema_normalizer.py`` — the production contract; every line
  number cited below was read from that file.
* ``tests/conftest.py`` — the shared ``malformed_result_set_payloads``
  fixture consumed by the parametrized error-case matrix.
"""

from __future__ import annotations

import re
from typing import Any, Dict

import pandas as pd
import pytest

from utils.schema_normalizer import normalize_result_sets


# ---------------------------------------------------------------------------
# Hand-derived expected values (kept separate from fixture INPUT values)
# ---------------------------------------------------------------------------
#
# Every constant in this section is derived independently from the
# production source rather than from a recorded run. The line references
# point at ``utils/schema_normalizer.py`` and each derivation is spelled
# out beside the literal it produces.
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
    # The envelope's row is the dict {"A": 1} against the single header
    # ["A"], so the row TYPE check (L320) is the only guard it can trip:
    # the width check (L325) sees len(row) == expected_width == 1. The
    # rendered type name is therefore "dict" and the row index is 0,
    # keeping this entry a clean single-branch probe of the TYPE guard.
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
MALFORMED_CASES: tuple[tuple[str, str], ...] = tuple(
    sorted(EXPECTED_MESSAGES.items())
)

#: Headers of the inline dtype-probe envelope, in upstream order. The
#: normalizer passes ``headers`` straight through as ``columns`` (L331),
#: so column identity and order must survive verbatim.
EXPECTED_DTYPE_COLUMNS: list[str] = ["PLAYER_ID", "PTS", "NOTE"]

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
EXPECTED_PTS_NULL_MASK: list[bool] = [True, False]
EXPECTED_NOTE_NULL_MASK: list[bool] = [False, True]

#: Keys produced by the duplicate-name NEAR-MISS envelope, whose tables
#: are named ``PlayByPlay``, ``PlayByPlay2``, ``PlayByPlay`` in that
#: order. This envelope is a near miss and NOT a true collision: the key
#: the uniquifier generates for the third table is never occupied. The
#: genuinely occupied-key case lives in
#: :data:`EXPECTED_TRUE_COLLISION_KEYS`. Derivation, traced through both
#: ``_snake_case`` regexes (L396 / L400 / L433-435) and the uniquifier
#: (L142-157):
#:
#: * ``"PlayByPlay"``: ``_CAMEL_BOUNDARY_1`` matches ``"yBy"`` at
#:   offset 3 -> ``"Play_ByPlay"``; ``_CAMEL_BOUNDARY_2`` then matches
#:   ``"yP"`` -> ``"Play_By_Play"``; lower/strip -> ``"play_by_play"``.
#: * ``"PlayByPlay2"``: the same two substitutions give
#:   ``"Play_By_Play2"`` -> ``"play_by_play2"``. There is NO underscore
#:   before the digit, so it is a genuinely DIFFERENT key from the
#:   ``play_by_play_2`` the uniquifier would generate; it takes a bare
#:   key and occupies nothing the uniquifier wants.
#: * The third table snake-cases to ``"play_by_play"``, which is already
#:   a key, so the ordinal starts at ``seen_names.get("play_by_play",
#:   1) + 1 == 2``; the candidate ``"play_by_play_2"`` is FREE (only
#:   ``play_by_play2``, without the underscore, is taken), so the probe
#:   stops immediately and the key becomes ``"play_by_play_2"``.
EXPECTED_NEAR_MISS_KEYS: list[str] = [
    "play_by_play",
    "play_by_play2",
    "play_by_play_2",
]

#: First-column cell of each near-miss frame, in key order. The three
#: distinct values prove no table was lost and none was overwritten.
EXPECTED_NEAR_MISS_CELLS: list[int] = [1, 2, 3]

#: Keys produced by the TRUE duplicate-name collision envelope, whose
#: tables are named ``PlayByPlay``, ``PlayByPlay_2``, ``PlayByPlay`` in
#: that order. Here the upstream name of the SECOND table snake-cases to
#: exactly the key the uniquifier wants to generate for the THIRD, so the
#: generated key is genuinely OCCUPIED. Derivation:
#:
#: * Table 1 ``"PlayByPlay"`` -> ``"play_by_play"``; free, taken as-is.
#: * Table 2 ``"PlayByPlay_2"`` -> ``"play_by_play_2"``; the underscore
#:   is already present in the upstream name and both regexes leave it
#:   alone, so this is a bare key and it too is free.
#: * Table 3 ``"PlayByPlay"`` -> ``"play_by_play"``, already a key. The
#:   ordinal starts at ``2``, whose candidate ``"play_by_play_2"`` is
#:   OCCUPIED by table 2, so the probe advances to ``3`` and the key
#:   becomes ``"play_by_play_3"``.
#:
#: Assigning the first candidate unconditionally — which is what the
#: uniquifier did before the fix recorded in this module's docstring —
#: replaced table 2's DataFrame with table 3's and returned only TWO
#: frames for THREE upstream tables, with no exception raised.
EXPECTED_TRUE_COLLISION_KEYS: list[str] = [
    "play_by_play",
    "play_by_play_2",
    "play_by_play_3",
]

#: First-column cell of each true-collision frame, in key order. These
#: three distinct values are the whole point of the test: a correct key
#: LIST can still hide an overwrite, and only the cells prove that
#: ``play_by_play_2`` still holds table 2's data rather than table 3's.
EXPECTED_TRUE_COLLISION_CELLS: list[int] = [1, 2, 3]

#: Keys produced by the CONSECUTIVE-occupancy envelope ``PlayByPlay``,
#: ``PlayByPlay_2``, ``PlayByPlay_3``, ``PlayByPlay``. The first three
#: names are distinct and take bare keys ``play_by_play``,
#: ``play_by_play_2`` and ``play_by_play_3``; the fourth repeats
#: ``play_by_play``, so the ordinal probe must step over TWO occupied
#: candidates (``_2`` then ``_3``) before landing on the free ``_4``.
#: This is the case that distinguishes a probe from a single retry: a
#: fix that checked the candidate only once would still overwrite here.
EXPECTED_CONSECUTIVE_OCCUPANCY_KEYS: list[str] = [
    "play_by_play",
    "play_by_play_2",
    "play_by_play_3",
    "play_by_play_4",
]

#: First-column cell of each consecutive-occupancy frame, in key order.
EXPECTED_CONSECUTIVE_OCCUPANCY_CELLS: list[int] = [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# Type coercion — dtype inference at the sole DataFrame construction site
# ---------------------------------------------------------------------------
#
# ``_build_dataframe`` ends in a single
# ``pd.DataFrame(row_set, columns=headers)`` call (L331) and performs no
# value repair whatsoever: the helper body contains no ``astype``, no
# ``to_numeric``, no ``fillna``, and no ``convert_dtypes``. Whatever
# pandas infers from the literal rowSet is therefore the normalizer's
# real and intended semantics, and it is what the normalizer HANDS TO its
# caller. The scope of the assertions below is exactly that frame: what
# ultimately lands in a CSV artifact also depends on the pipeline
# transforms applied downstream (for example the ``GAME_ID``
# zero-padding and season-column insertion in the pipelines) and on the
# writer's own serialization, none of which is under test here.
#
# The three tests below pin that inference exactly, because the two
# plausible "helpful" mutations here are silent data corruption rather
# than crashes: a ``fillna(0)`` would turn a player's MISSING points
# into ZERO points, and a ``pd.to_numeric`` would rewrite identifier and
# measurement columns before any consumer ever sees them.
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
    silently convert a player's MISSING points into ZERO points, which the
    dtype and null-mask assertions below are what catch.
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
# Duplicate result-set names — the NEAR MISS and the TRUE COLLISION
# ---------------------------------------------------------------------------
#
# Duplicate names are a duplicate-record concern inside the PARSING
# stage: two tables sharing a name must both survive, because the
# uniquifier (L142-157) is the only thing standing between a repeated
# upstream name and one table's data vanishing from the output.
#
# Two distinct and strictly ordered cases are covered here, and the
# difference between them is the whole point of this section:
#
# 1. NEAR MISS -- ``PlayByPlay``, ``PlayByPlay2``, ``PlayByPlay``.
#    ``PlayByPlay2`` snake-cases to ``play_by_play2`` (no underscore)
#    while the repeated ``PlayByPlay`` generates ``play_by_play_2``
#    (with one). They differ by a single character, so the generated key
#    is NEVER occupied and the occupied-key branch is never reached.
#    This case proves the two spellings stay apart; it proves nothing
#    about occupancy, and its name and docstring say so plainly.
#
# 2. TRUE COLLISION -- ``PlayByPlay``, ``PlayByPlay_2``, ``PlayByPlay``.
#    ``PlayByPlay_2`` snake-cases to exactly ``play_by_play_2``, which
#    IS the key the uniquifier generates for the third table. This is
#    the reachable data-destruction case: a single unconditional
#    assignment replaces the second table's frame with the third's and
#    returns two frames for three upstream tables, silently, with no
#    exception. It is the case the fix in this module's docstring closes.
# ---------------------------------------------------------------------------


def test_duplicate_name_beside_a_digit_suffixed_sibling_keeps_every_table() -> None:
    """A repeated name and a digit-suffixed sibling stay distinct — a NEAR MISS.

    Hand-derived in :data:`EXPECTED_NEAR_MISS_KEYS`: ``PlayByPlay`` ->
    ``play_by_play``, ``PlayByPlay2`` -> ``play_by_play2`` (a different
    upstream name with no underscore, so it keeps a bare key), and the
    repeated ``PlayByPlay`` -> ``play_by_play_2`` from an ordinal that
    starts at ``seen_names.get("play_by_play", 1) + 1`` and stops
    immediately because that candidate is free.

    This envelope is deliberately a NEAR MISS, not a collision: the
    generated key is never occupied, so the occupied-key probe is not
    exercised here. The genuinely occupied case is
    :func:`test_duplicate_name_whose_generated_key_is_occupied_keeps_every_table`
    and the two must not be confused — passing this test alone would
    leave the reachable overwrite undetected.

    Mutations detected: replacing the suffixing with plain assignment --
    the third table would overwrite the first, leaving two keys and the
    cells ``[3, 2]``; or dropping the underscore from the suffix so the
    repeat is keyed ``play_by_play2`` -- it would then overwrite the
    genuinely different ``PlayByPlay2`` table, again leaving two keys and
    replacing that table's cell value. The ordered-key assertion and the
    distinguishing-cell assertion each fail under both changes.
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
    assert list(out.keys()) == EXPECTED_NEAR_MISS_KEYS, (
        f"duplicate names must be uniquified without colliding with a "
        f"digit-suffixed sibling; expected {EXPECTED_NEAR_MISS_KEYS} but got "
        f"{list(out.keys())}"
    )
    assert len(out) == len(EXPECTED_NEAR_MISS_KEYS), (
        f"all three tables must survive; expected "
        f"{len(EXPECTED_NEAR_MISS_KEYS)} frames but got {len(out)}"
    )

    # Assert -- distinguishing cells prove nothing was overwritten.
    observed_cells = [int(out[key].iloc[0, 0]) for key in EXPECTED_NEAR_MISS_KEYS]
    assert observed_cells == EXPECTED_NEAR_MISS_CELLS, (
        f"each key must map to its own table's data; expected cells "
        f"{EXPECTED_NEAR_MISS_CELLS} but got {observed_cells}"
    )


def test_duplicate_name_whose_generated_key_is_occupied_keeps_every_table() -> None:
    """A repeat whose generated ``_2`` key is already TAKEN must not overwrite it.

    This is the reachable data-destruction case, and the failing case
    that motivated the one-statement uniquifier fix recorded in this
    module's docstring. Upstream envelope, in order: ``PlayByPlay``
    (cell ``1``), ``PlayByPlay_2`` (cell ``2``), ``PlayByPlay`` (cell
    ``3``).

    Hand-derived in :data:`EXPECTED_TRUE_COLLISION_KEYS`: table 2's own
    name snake-cases to ``play_by_play_2``, which is EXACTLY the key the
    uniquifier generates for table 3, so the ordinal must advance from
    ``2`` to the free ``3`` and produce ``play_by_play_3``.

    Before the fix this envelope returned only ``['play_by_play',
    'play_by_play_2']`` with ``play_by_play_2`` holding cell ``3`` --
    two frames for three upstream tables, table 2's data destroyed, and
    NO exception raised. A schema-drifting or hostile upstream could
    therefore delete an entire result table from every downstream
    DataFrame and CSV artifact while the pipeline reported success.

    Mutation detected: reverting the probe to a single
    ``seen_names.get(name, 1) + 1`` assignment. The length assertion, the
    key-list assertion and the cell assertion each fail independently,
    and the cell assertion is the one that proves the surviving frame is
    table 2's data rather than table 3's.
    """
    # Arrange -- table 2's upstream name IS table 3's generated key.
    payload: Dict[str, Any] = {
        "resultSets": [
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[1]]},
            {"name": "PlayByPlay_2", "headers": ["A"], "rowSet": [[2]]},
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[3]]},
        ]
    }
    expected_table_count = len(payload["resultSets"])

    # Act
    out = normalize_result_sets(payload)

    # Assert -- loss-free first: three tables in, three frames out.
    assert len(out) == expected_table_count, (
        f"the uniquifier must never drop a table; the envelope declared "
        f"{expected_table_count} result sets but normalize_result_sets "
        f"returned {len(out)} frames ({list(out.keys())}). A missing frame "
        f"means one table silently overwrote another"
    )

    # Assert -- the complete ordered key list, not a membership check.
    assert list(out.keys()) == EXPECTED_TRUE_COLLISION_KEYS, (
        f"the repeated name's generated key was already occupied, so the "
        f"ordinal must advance past it; expected "
        f"{EXPECTED_TRUE_COLLISION_KEYS} but got {list(out.keys())}"
    )

    # Assert -- distinguishing cells prove WHICH table each key holds. A
    # correct key list alone cannot detect an overwrite; this can.
    observed_cells = [int(out[key].iloc[0, 0]) for key in EXPECTED_TRUE_COLLISION_KEYS]
    assert observed_cells == EXPECTED_TRUE_COLLISION_CELLS, (
        f"each key must map to its OWN table's data; expected cells "
        f"{EXPECTED_TRUE_COLLISION_CELLS} but got {observed_cells}. A "
        f"{EXPECTED_TRUE_COLLISION_CELLS[-1]} under key "
        f"{EXPECTED_TRUE_COLLISION_KEYS[1]!r} means the last table "
        f"overwrote the second"
    )


def test_duplicate_name_probes_past_consecutive_occupied_keys_without_loss() -> None:
    """The ordinal probe steps over EVERY occupied candidate, not just one.

    Upstream envelope, in order: ``PlayByPlay`` (cell ``1``),
    ``PlayByPlay_2`` (cell ``2``), ``PlayByPlay_3`` (cell ``3``),
    ``PlayByPlay`` (cell ``4``). Hand-derived in
    :data:`EXPECTED_CONSECUTIVE_OCCUPANCY_KEYS`: the first three names
    are distinct and take bare keys, then the repeat's ordinal must step
    over TWO occupied candidates (``play_by_play_2``, then
    ``play_by_play_3``) before landing on the free ``play_by_play_4``.

    Mutation detected: a one-shot retry — checking the candidate once and
    incrementing a single time instead of looping — which would land back
    on the occupied ``play_by_play_3`` and destroy table 3. That mutation
    passes
    :func:`test_duplicate_name_whose_generated_key_is_occupied_keeps_every_table`
    (where a single step is enough) and is caught only here.
    """
    # Arrange -- two consecutive generated candidates are pre-occupied.
    payload: Dict[str, Any] = {
        "resultSets": [
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[1]]},
            {"name": "PlayByPlay_2", "headers": ["A"], "rowSet": [[2]]},
            {"name": "PlayByPlay_3", "headers": ["A"], "rowSet": [[3]]},
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[4]]},
        ]
    }
    expected_table_count = len(payload["resultSets"])

    # Act
    out = normalize_result_sets(payload)

    # Assert -- loss-free: four tables in, four frames out.
    assert len(out) == expected_table_count, (
        f"the uniquifier must never drop a table; the envelope declared "
        f"{expected_table_count} result sets but normalize_result_sets "
        f"returned {len(out)} frames ({list(out.keys())})"
    )

    # Assert -- the complete ordered key list.
    assert list(out.keys()) == EXPECTED_CONSECUTIVE_OCCUPANCY_KEYS, (
        f"the probe must advance past every occupied candidate; expected "
        f"{EXPECTED_CONSECUTIVE_OCCUPANCY_KEYS} but got {list(out.keys())}"
    )

    # Assert -- every table's own data survives under its own key.
    observed_cells = [
        int(out[key].iloc[0, 0]) for key in EXPECTED_CONSECUTIVE_OCCUPANCY_KEYS
    ]
    assert observed_cells == EXPECTED_CONSECUTIVE_OCCUPANCY_CELLS, (
        f"each key must map to its OWN table's data; expected cells "
        f"{EXPECTED_CONSECUTIVE_OCCUPANCY_CELLS} but got {observed_cells}"
    )


# ---------------------------------------------------------------------------
# Malformed envelopes — the seven exact ValueError messages
# ---------------------------------------------------------------------------
#
# The five guards exercised here -- ``_extract_tables`` (L155),
# ``_require_str`` (L208), ``_require_list`` (L240), the
# non-string-headers branch (L134) and the row-not-a-sequence branch
# (L320) -- are all reached exclusively through the public
# ``normalize_result_sets``; no private helper is imported and no
# production source file is touched.
#
# Guard ORDER inside the per-table loop (L129-140) determines which
# envelope trips which branch, so the shared fixture's shapes are
# minimal but deliberate:
#
#   L130  name    = _snake_case(_require_str(table, "name"))
#   L131  headers = _require_list(table, "headers")
#   L132  row_set = _require_list(table, "rowSet")
#   L134  non-string headers check   (AFTER both _require_list calls)
#   L139  _build_dataframe -> row TYPE check (L320) then WIDTH (L325)
#
# Because ``pytest.raises(match=...)`` runs a ``re.search``, every
# pattern below is wrapped in ``re.escape``: the messages contain ``'``,
# ``[``, ``]``, ``(``, and ``)``, and an unescaped ``[`` or ``(`` would
# become a character class or capture group -- a pattern that still
# matches, but for the wrong reason. Every case is additionally asserted
# with full-string ``==`` equality, which is strictly stronger than a
# containment search, and the row-width branch gets a dedicated boundary
# test that does the same.
# ---------------------------------------------------------------------------


def test_malformed_fixture_supplies_exactly_one_envelope_per_rejection_branch(
    malformed_result_set_payloads: Dict[str, Any],
) -> None:
    """The shared fixture's case names match this module's expected-message table exactly.

    Keeps the fixture and this module's expected-message table in step.
    The parametrized matrix below is driven by :data:`EXPECTED_MESSAGES`,
    so its size is fixed by this module and cannot shrink when the
    fixture changes; instead a renamed or removed envelope makes the
    fixture lookup inside the parametrized test raise a confusing
    ``KeyError``. This test converts that into a clear contract failure by
    asserting exact set equality and an exact count -- not membership and
    not a lower bound.

    Mutation detected: dropping or renaming a rejection-branch envelope in
    ``tests/conftest.py``. The set-equality assertion names the missing or
    renamed case directly, which is what stops a validation guard from
    quietly losing its envelope.
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

    Mutations detected: deleting or loosening any of these validation
    guards, and any drift in the operator-facing message text such as
    dropping the result-set name or the offending row index. Removing a
    guard does not necessarily produce silent acceptance -- some shapes go
    on to fail deeper inside pandas with an unrelated exception type
    instead. Either way the diagnostic contract breaks: this test fails
    because the raised class is no longer ``ValueError`` or the message no
    longer names what was wrong and where.
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
    Full-string equality is asserted, so every component of the message is
    pinned rather than merely its presence.

    Mutation detected: changing the reported row index, the reported
    value count, or the declared-header count; or moving the width check
    somewhere that no longer names the offending row.
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
