"""Malformed-input and type-coercion tests for ``utils.schema_normalizer``.

Covers the **parsing** and **type-coercion** stages of the ingestion
pipeline: the validation gate of
:func:`utils.schema_normalizer.normalize_result_sets` and the pandas
dtype-inference behaviour of the single
``pd.DataFrame(row_set, columns=headers)`` construction site inside
``_build_dataframe``.

Contracts pinned here
---------------------
* **Private helpers are reached ONLY through the public entry point.**
  ``_extract_tables``, ``_require_str``, and ``_require_list`` are
  exercised exclusively by feeding malformed envelopes to the public
  ``normalize_result_sets``. No private helper is imported, promoted, or
  given a test-only hook.
* **Seven rejection outcomes, seven exact messages.** Each envelope in
  the shared ``malformed_result_set_payloads`` fixture is the minimal
  shape that trips exactly one guard, and each expected message in
  :data:`EXPECTED_MESSAGES` is the verbatim production string. Because
  ``pytest.raises(match=...)`` performs a :func:`re.search`, every
  pattern is wrapped in :func:`re.escape` — the messages contain ``'``,
  ``[``, ``]``, ``(``, and ``)``, any of which would silently alter an
  unescaped pattern and make the assertion pass for the wrong reason.
  Every case additionally asserts full-string ``==`` equality alongside
  the containment match, and the row-width branch carries a dedicated
  boundary test that does the same — both strictly stronger than a
  containment search.
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
* **Duplicate names are uniquified, never overwritten — while the
  generated key is free.** A name that repeats within one envelope is
  suffixed with the next ordinal (``PlayByPlay`` twice ->
  ``play_by_play`` and ``play_by_play_2``) instead of the second table
  replacing the first, and the near-miss spelling that merely
  *resembles* a generated key stays distinct from it: an upstream name
  already ending in a digit snake-cases to ``play_by_play2`` (no
  underscore) while the generated suffix is ``play_by_play_2`` (with
  one). The key list
  ``['play_by_play', 'play_by_play2', 'play_by_play_2']`` is pinned
  here.
* **The occupied-generated-suffix case is a KNOWN DEFECT, and is
  characterised rather than glossed over.** When the ordinal the
  uniquifier computes names a key an earlier table already holds, the
  assignment overwrites that earlier frame and one upstream table
  disappears from the mapping with no exception raised. Two tests below
  pin that behaviour exactly — including the frame count against the
  upstream table count — so the suite documents the loss instead of
  masking it. They deliberately encode CURRENT behaviour, which the AAP
  sanctions in §0.4.5 ("leave the source untouched and instead add a
  test documenting the current behavior with a comment naming the
  defect"), because ``utils/schema_normalizer.py`` is frozen: AAP §0.8.2
  places all of ``utils/*.py`` out of scope and §0.10.2 exercises
  exactly one source exception (CD-1, in ``endpoints/schedule.py``).
  Each test names the minimal fix that would remove the defect and states
  that applying it must update the test in the same change.
* **No test doubles at all.** ``normalize_result_sets`` is a pure
  function over a JSON-like envelope with no I/O, so mocking at the
  boundary of the code under test would violate the repository
  convention. Inputs are literal dicts plus the one shared fixture; no
  mock, spy, monkeypatch, filesystem, network, or metrics access
  appears here.

Every test below names the specific change it detects.
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
# Every constant in this section is derived from the production
# contract, and each derivation is spelled out beside the literal it
# produces.
# ---------------------------------------------------------------------------

#: Verbatim ``ValueError`` text for each rejection outcome, keyed by the
#: case names used in ``tests/conftest.py::malformed_result_set_payloads``.
#:
#: Each string is the message the corresponding guard renders for that
#: envelope's values. The result-set name is ``"t"`` throughout because
#: ``_snake_case("t")`` is ``"t"``: neither camel-boundary pattern
#: (``(.)([A-Z][a-z]+)`` and ``([a-z0-9])([A-Z])``) matches a single
#: lowercase character, and ``.lower().strip("_")`` leaves it untouched.
EXPECTED_MESSAGES: Dict[str, str] = {
    "row_width_mismatch": (
        "Result set 't' row 0 has 2 values but 3 headers are declared"
    ),
    "missing_name": (
        "Result set is missing required string field 'name' (got NoneType)"
    ),
    "headers_not_list": (
        "Result set field 'headers' must be a list; got dict"
    ),
    # ``_require_list`` reached for the ``rowSet`` key rather than
    # ``headers``. An EMPTY list is valid here; only a non-list trips the
    # guard, and an omitted key arrives as ``None`` -> "NoneType".
    "missing_row_set": (
        "Result set field 'rowSet' must be a list; got NoneType"
    ),
    # The message interpolates the header list's repr, so ["A", 7]
    # renders as ['A', 7] -- note the single quotes around A and the
    # space after the comma. This guard runs AFTER both _require_list
    # calls, so the envelope must still carry a valid name and a
    # list-typed rowSet.
    "non_string_header": (
        "Result set 't' contains non-string headers: ['A', 7]"
    ),
    # The envelope's row is the dict {"A": 1} against the single header
    # ["A"], so the row TYPE check is the only guard it can trip: the
    # width check sees len(row) == expected_width == 1. The rendered type
    # name is therefore "dict" and the row index is 0, keeping this entry
    # a clean single-branch probe of the TYPE guard.
    "row_not_sequence": (
        "Result set 't' row 0 is dict, expected list/tuple"
    ),
    # The ``_extract_tables`` TYPE guard, distinct from the "Payload
    # contains no result sets" message raised when the extracted table
    # list is empty.
    "result_sets_wrong_type": (
        "'resultSets' must be a list or dict, got str"
    ),
}

#: Number of distinct rejection outcomes the shared fixture must cover.
#: Fixed by the seven-entry table above; asserted against the fixture so
#: a dropped envelope cannot silently shrink the parametrized matrix.
#: Seven, because five guards are exercised and two of them are reached
#: twice under different keys: ``_require_list`` rejects ``headers`` and
#: ``rowSet`` separately, and ``_build_dataframe`` rejects a row on TYPE
#: and on WIDTH.
EXPECTED_REJECTION_BRANCH_COUNT = 7

#: Deterministic ``(case, expected_message)`` pairs for parametrization.
#: A fixture cannot be referenced inside ``@pytest.mark.parametrize``, so
#: the local table drives the matrix and the fixture is indexed by case
#: name inside the test body. Sorted for a stable test-id ordering.
MALFORMED_CASES: tuple[tuple[str, str], ...] = tuple(
    sorted(EXPECTED_MESSAGES.items())
)

#: Headers of the inline dtype-probe envelope, in upstream order. The
#: normalizer passes ``headers`` straight through as the frame's
#: ``columns``, so column identity and order must survive verbatim.
EXPECTED_DTYPE_COLUMNS: list[str] = ["PLAYER_ID", "PTS", "NOTE"]

#: pandas dtype inference for ``pd.DataFrame(row_set, columns=headers)``
#: -- the sole construction site -- over the rowSet
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
#: order. It is a near miss rather than an exact clash: the digit suffix
#: the upstream itself supplies (``play_by_play2``) differs by one
#: character from the suffix the uniquifier generates
#: (``play_by_play_2``), so all three tables keep their own key.
#: Derivation, traced through both ``_snake_case`` camel-boundary
#: substitutions and the uniquifier:
#:
#: * ``"PlayByPlay"``: the first camel boundary matches ``"yBy"`` at
#:   offset 3 -> ``"Play_ByPlay"``; the second then matches ``"yP"`` ->
#:   ``"Play_By_Play"``; lower/strip -> ``"play_by_play"``.
#: * ``"PlayByPlay2"``: the same two substitutions give
#:   ``"Play_By_Play2"`` -> ``"play_by_play2"``. There is NO underscore
#:   before the digit, so it is a genuinely DIFFERENT key from the
#:   ``play_by_play_2`` the uniquifier would generate; it takes a bare
#:   key and occupies nothing the uniquifier wants.
#: * The third table snake-cases to ``"play_by_play"``, which is already
#:   a key, so the ordinal is ``seen_names.get("play_by_play", 1) + 1 ==
#:   2`` and the suffixed key is ``"play_by_play_2"`` — distinct from the
#:   ``play_by_play2`` the second table holds, because that spelling has
#:   no underscore before its digit.
EXPECTED_NEAR_MISS_KEYS: list[str] = [
    "play_by_play",
    "play_by_play2",
    "play_by_play_2",
]

#: First-column cell of each near-miss frame, in key order. The three
#: distinct values prove no table was lost and none was overwritten.
EXPECTED_NEAR_MISS_CELLS: list[int] = [1, 2, 3]

#: Number of upstream tables in the two OCCUPIED-SUFFIX envelopes below.
#: Held as constants so each test can compare the returned frame count
#: against the number of tables sent, which is what quantifies the loss.
OCCUPIED_SUFFIX_TABLE_COUNT = 3
CONSECUTIVE_SUFFIX_TABLE_COUNT = 5

#: Keys produced by the OCCUPIED-generated-suffix envelope, whose tables
#: are named ``PlayByPlay``, ``PlayByPlay_2``, ``PlayByPlay`` in that
#: order. Unlike the near miss above, the second table's own upstream name
#: snake-cases to EXACTLY the key the uniquifier will generate for the
#: third, so the two collide. Derivation, traced through the same
#: camel-boundary substitutions and the uniquifier:
#:
#: * table 1 ``"PlayByPlay"`` -> ``"play_by_play"``; the key is free, so
#:   ``dataframes["play_by_play"] = <table 1>``.
#: * table 2 ``"PlayByPlay_2"``: the first camel boundary gives
#:   ``"Play_ByPlay_2"``, the second gives ``"Play_By_Play_2"``,
#:   lower/strip -> ``"play_by_play_2"``. That name is NOT yet a key, so it
#:   takes the bare form: ``dataframes["play_by_play_2"] = <table 2>``.
#: * table 3 ``"PlayByPlay"`` -> ``"play_by_play"``, which IS a key, so the
#:   ordinal is ``seen_names.get("play_by_play", 1) + 1 == 2`` and the
#:   generated key is ``"play_by_play_2"`` — ALREADY HELD by table 2. The
#:   assignment ``dataframes[deduped] = df`` is unconditional, so table 2's
#:   frame is replaced and vanishes.
#:
#: Result: TWO keys for THREE upstream tables. This is the defect the
#: module docstring names; the constants are its characterisation, not an
#: endorsement.
EXPECTED_OCCUPIED_SUFFIX_KEYS: list[str] = [
    "play_by_play",
    "play_by_play_2",
]

#: First-column cell of each surviving frame, in key order. Table 1 keeps
#: cell ``1``; the ``play_by_play_2`` key holds table 3's cell ``3``, not
#: table 2's cell ``2`` — the missing ``2`` IS the lost table.
EXPECTED_OCCUPIED_SUFFIX_CELLS: list[int] = [1, 3]

#: Keys produced by the CONSECUTIVE-occupancy envelope, whose tables are
#: named ``X``, ``X_2``, ``X_3``, ``X``, ``X`` in that order. It proves the
#: loss compounds: the uniquifier advances its ordinal per repeat without
#: checking occupancy, so each repeat displaces the sibling holding that
#: ordinal. Derivation (``_snake_case`` lowercases each name unchanged —
#: no CamelCase boundary matches a single letter or a letter-digit pair):
#:
#: * table 1 ``X`` -> ``x`` (free) -> ``x`` holds cell 1.
#: * table 2 ``X_2`` -> ``x_2`` (free, a different upstream NAME) -> cell 2.
#: * table 3 ``X_3`` -> ``x_3`` (free) -> cell 3.
#: * table 4 ``X`` repeats ``x``: ordinal ``1 + 1 == 2`` -> ``x_2``, already
#:   held by table 2, which is replaced by cell 4.
#: * table 5 ``X`` repeats ``x``: ordinal ``2 + 1 == 3`` -> ``x_3``, already
#:   held by table 3, which is replaced by cell 5.
#:
#: Result: THREE keys for FIVE upstream tables — two frames lost.
EXPECTED_CONSECUTIVE_SUFFIX_KEYS: list[str] = ["x", "x_2", "x_3"]

#: First-column cell of each surviving frame, in key order: table 1's
#: ``1``, then table 4's ``4`` and table 5's ``5`` in the slots tables 2
#: and 3 originally held. The absent ``2`` and ``3`` are the lost tables.
EXPECTED_CONSECUTIVE_SUFFIX_CELLS: list[int] = [1, 4, 5]


# ---------------------------------------------------------------------------
# Type coercion — dtype inference at the sole DataFrame construction site
# ---------------------------------------------------------------------------
#
# ``_build_dataframe`` ends in a single
# ``pd.DataFrame(row_set, columns=headers)`` call and performs no
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
    value repair: its body contains no ``astype``, no ``to_numeric``, no
    ``fillna``, and no ``convert_dtypes``. A column of two strings
    therefore infers ``object``, and each cell remains the exact ``str``
    the envelope supplied. The neighbouring ``PLAYER_ID`` column proves
    genuine integers are still inferred as ``int64``, so the ``object``
    result is specific to the string input rather than a blanket loss of
    inference.

    Mutation detected: introducing ``pd.to_numeric`` "for convenience",
    which would change the normalized frame's dtype and the Python type
    of every cell in it before any downstream consumer receives the
    frame.
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
# Duplicate result-set names — uniquification instead of overwriting
# ---------------------------------------------------------------------------
#
# Duplicate names are a duplicate-record concern inside the PARSING
# stage: two tables sharing a name must both survive, because the
# uniquifier is the only thing standing between a repeated upstream name
# and one table's data vanishing from the output.
#
# The envelope exercised here is ``PlayByPlay``, ``PlayByPlay2``,
# ``PlayByPlay``. ``PlayByPlay2`` snake-cases to ``play_by_play2`` (no
# underscore) while the repeated ``PlayByPlay`` is suffixed to
# ``play_by_play_2`` (with one), so the two spellings stay one character
# apart and all three tables keep their own key and their own data.
# ---------------------------------------------------------------------------


def test_duplicate_name_beside_a_digit_suffixed_sibling_keeps_every_table() -> None:
    """A repeated name and a digit-suffixed sibling stay distinct, keeping 3 tables.

    Hand-derived in :data:`EXPECTED_NEAR_MISS_KEYS`: ``PlayByPlay`` ->
    ``play_by_play``, ``PlayByPlay2`` -> ``play_by_play2`` (a different
    upstream name with no underscore, so it keeps a bare key), and the
    repeated ``PlayByPlay`` -> ``play_by_play_2`` from the ordinal
    ``seen_names.get("play_by_play", 1) + 1 == 2``. Three tables in,
    three frames out, each holding its own row.

    The key list is asserted as a whole ordered list together with the
    distinguishing first cell of every frame — a correct key list alone
    cannot detect an overwrite, only the cells can.

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


# ---------------------------------------------------------------------------
# Duplicate result-set names — the OCCUPIED-suffix defect, characterised
# ---------------------------------------------------------------------------
#
# The near-miss envelope above is loss-free because the sibling's spelling
# (``play_by_play2``) differs from the generated key (``play_by_play_2``).
# When the two coincide, the uniquifier's ordinal is a verdict rather than
# a starting point: it computes ONE candidate key and assigns to it
# unconditionally, so an earlier table holding that key is replaced and
# disappears from the returned mapping with no exception raised.
#
# The two tests below characterise that behaviour EXACTLY, in the shape AAP
# §0.4.5 prescribes for a defect whose source file is frozen: "leave the
# source untouched and instead add a test documenting the current behavior
# with a comment naming the defect. Constraint C2 outranks the optional
# fix." ``utils/schema_normalizer.py`` is frozen by AAP §0.8.2 (all of
# ``utils/*.py`` out of scope) and §0.10.2 (CD-1 in
# ``endpoints/schedule.py`` is the sole source exception), and this
# module's own file brief repeats the prohibition verbatim.
#
# THE DEFECT, with the failing case that exposes it:
#
#   envelope tables named ``PlayByPlay``, ``PlayByPlay_2``, ``PlayByPlay``
#   -> ``{'play_by_play': <table 1>, 'play_by_play_2': <table 3>}``
#   Table 2's DataFrame is gone: 3 tables in, 2 frames out, silently.
#
# THE MINIMAL FIX (for whoever authorises it) — make the ordinal a
# starting point and probe forward to the first FREE key, replacing the
# ``seen_names``/``deduped`` pair in the ``if name in dataframes`` branch
# of ``normalize_result_sets`` with:
#
#   ordinal = seen_names.get(name, 1) + 1
#   while f"{name}_{ordinal}" in dataframes:
#       ordinal += 1
#   seen_names[name] = ordinal
#   dataframes[f"{name}_{ordinal}"] = df
#
# That yields ``['play_by_play', 'play_by_play_2', 'play_by_play_3']`` with
# cells ``[1, 2, 3]`` and leaves every currently-asserted key sequence
# (``['x','x_2','x_3']`` in the frozen sibling module, and the near miss
# above) untouched. Applying it MUST update the two tests below in the same
# change: they assert today's lossy behaviour on purpose, so they act as a
# tripwire that forces the fix to be deliberate rather than incidental.
# ---------------------------------------------------------------------------


def test_repeat_whose_generated_key_is_occupied_drops_the_occupying_table() -> None:
    """3 tables named PlayByPlay / PlayByPlay_2 / PlayByPlay return only 2 frames.

    Characterises the KNOWN DEFECT named in the section comment above,
    hand-derived in :data:`EXPECTED_OCCUPIED_SUFFIX_KEYS` and
    :data:`EXPECTED_OCCUPIED_SUFFIX_CELLS`: the repeated ``PlayByPlay``
    generates ``play_by_play_2``, which table 2 already holds under its own
    upstream name, and the unconditional ``dataframes[deduped] = df``
    replaces it. The returned mapping therefore has 2 entries for 3
    upstream tables and the cell sequence is ``[1, 3]`` — the missing ``2``
    is the lost table.

    Asserting the frame count against
    :data:`OCCUPIED_SUFFIX_TABLE_COUNT` is what makes the loss explicit
    and quantified rather than implied by a key list.

    Mutations detected: dropping the uniquifier entirely so a repeat
    overwrites the BARE key (cells would read ``[3, 2]``); renumbering the
    ordinal from ``_1`` or changing the separator (the key list changes);
    and any change to how a repeated name is keyed at all. Applying the
    probe fix quoted above also fails this test by design — see the section
    comment: the fix and this test must land together.
    """
    # Arrange -- table 2's upstream name IS the key table 3 will generate.
    payload: Dict[str, Any] = {
        "resultSets": [
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[1]]},
            {"name": "PlayByPlay_2", "headers": ["A"], "rowSet": [[2]]},
            {"name": "PlayByPlay", "headers": ["A"], "rowSet": [[3]]},
        ]
    }

    # Act
    out = normalize_result_sets(payload)

    # Assert -- the complete ordered key list, not a membership check.
    assert list(out.keys()) == EXPECTED_OCCUPIED_SUFFIX_KEYS, (
        f"an occupied generated suffix currently yields "
        f"{EXPECTED_OCCUPIED_SUFFIX_KEYS}; got {list(out.keys())}"
    )

    # Assert -- the loss, quantified: frames returned vs tables supplied.
    assert len(out) == len(EXPECTED_OCCUPIED_SUFFIX_KEYS), (
        f"expected {len(EXPECTED_OCCUPIED_SUFFIX_KEYS)} frames; got {len(out)}"
    )
    assert len(out) < OCCUPIED_SUFFIX_TABLE_COUNT, (
        f"this test exists because one table is LOST: "
        f"{OCCUPIED_SUFFIX_TABLE_COUNT} tables were supplied but only "
        f"{len(out)} frames returned. If this assertion fails because "
        f"len(out) == {OCCUPIED_SUFFIX_TABLE_COUNT}, the loss-free probe fix "
        f"has been applied and this test must be updated to expect "
        f"['play_by_play', 'play_by_play_2', 'play_by_play_3'] with cells "
        f"[1, 2, 3]"
    )

    # Assert -- which table survived under the collided key.
    observed_cells = [
        int(out[key].iloc[0, 0]) for key in EXPECTED_OCCUPIED_SUFFIX_KEYS
    ]
    assert observed_cells == EXPECTED_OCCUPIED_SUFFIX_CELLS, (
        f"the collided key currently holds the LAST writer's data; expected "
        f"cells {EXPECTED_OCCUPIED_SUFFIX_CELLS} (table 2's 2 is missing) "
        f"but got {observed_cells}"
    )


def test_repeats_against_consecutive_occupied_suffixes_drop_two_tables() -> None:
    """5 tables named X / X_2 / X_3 / X / X return only 3 frames.

    The compounding form of the same defect, hand-derived in
    :data:`EXPECTED_CONSECUTIVE_SUFFIX_KEYS` and
    :data:`EXPECTED_CONSECUTIVE_SUFFIX_CELLS`: the ordinal advances once
    per repeat (``2``, then ``3``) without ever checking occupancy, so the
    fourth table displaces ``x_2`` and the fifth displaces ``x_3``. Three
    keys for five upstream tables, with cells ``[1, 4, 5]`` — the absent
    ``2`` and ``3`` are the two lost tables.

    Mutations detected: any change to the per-repeat ordinal progression
    (a fixed suffix would collapse both repeats onto one key, yielding two
    frames and cells ``[1, 5]``); resetting ``seen_names`` per table; and
    keying a repeat off the bare name. As with its sibling above, applying
    the loss-free probe fix fails this test by design.
    """
    # Arrange -- two bare siblings occupy the first two generated ordinals.
    payload: Dict[str, Any] = {
        "resultSets": [
            {"name": "X", "headers": ["A"], "rowSet": [[1]]},
            {"name": "X_2", "headers": ["A"], "rowSet": [[2]]},
            {"name": "X_3", "headers": ["A"], "rowSet": [[3]]},
            {"name": "X", "headers": ["A"], "rowSet": [[4]]},
            {"name": "X", "headers": ["A"], "rowSet": [[5]]},
        ]
    }

    # Act
    out = normalize_result_sets(payload)

    # Assert -- the complete ordered key list.
    assert list(out.keys()) == EXPECTED_CONSECUTIVE_SUFFIX_KEYS, (
        f"consecutive occupied suffixes currently yield "
        f"{EXPECTED_CONSECUTIVE_SUFFIX_KEYS}; got {list(out.keys())}"
    )

    # Assert -- the loss, quantified.
    assert len(out) == len(EXPECTED_CONSECUTIVE_SUFFIX_KEYS), (
        f"expected {len(EXPECTED_CONSECUTIVE_SUFFIX_KEYS)} frames; "
        f"got {len(out)}"
    )
    assert len(out) < CONSECUTIVE_SUFFIX_TABLE_COUNT, (
        f"this test exists because TWO tables are LOST: "
        f"{CONSECUTIVE_SUFFIX_TABLE_COUNT} tables were supplied but only "
        f"{len(out)} frames returned. If this assertion fails because "
        f"len(out) == {CONSECUTIVE_SUFFIX_TABLE_COUNT}, the loss-free probe "
        f"fix has been applied and this test must be updated to expect "
        f"['x', 'x_2', 'x_3', 'x_4', 'x_5'] with cells [1, 2, 3, 4, 5]"
    )

    # Assert -- which tables survived, in key order.
    observed_cells = [
        int(out[key].iloc[0, 0]) for key in EXPECTED_CONSECUTIVE_SUFFIX_KEYS
    ]
    assert observed_cells == EXPECTED_CONSECUTIVE_SUFFIX_CELLS, (
        f"each displaced slot currently holds its LAST writer's data; "
        f"expected cells {EXPECTED_CONSECUTIVE_SUFFIX_CELLS} (tables 2 and 3 "
        f"are missing) but got {observed_cells}"
    )


# ---------------------------------------------------------------------------
# Malformed envelopes — the seven exact ValueError messages
# ---------------------------------------------------------------------------
#
# The five guards exercised here -- ``_extract_tables``,
# ``_require_str``, ``_require_list``, the non-string-headers branch and
# the row-not-a-sequence branch -- are all reached exclusively through
# the public ``normalize_result_sets``; no private helper is imported.
#
# Seven envelopes cover those five guards because two guards are reached
# under two different keys: ``_require_list`` is applied to ``headers``
# and then to ``rowSet``, and ``_build_dataframe`` checks a row's TYPE
# before its WIDTH. The rendered field name and type name are what
# distinguish the resulting messages.
#
# Guard ORDER inside the per-table loop determines which envelope trips
# which branch, so the shared fixture's shapes are minimal but
# deliberate:
#
#   name    = _snake_case(_require_str(table, "name"))
#   headers = _require_list(table, "headers")
#   row_set = _require_list(table, "rowSet")
#   non-string headers check   (AFTER both _require_list calls)
#   _build_dataframe -> row TYPE check then WIDTH check
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
    messages each guard renders for its envelope. ``re.escape`` keeps the
    regex literal so the punctuation in the messages cannot silently
    relax the pattern. The error class is pinned too: a non-dict payload
    raises ``TypeError``, so ``ValueError`` here confirms the DATA-error
    path rather than the programmer-error path.

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

    Hand-derived from the fixture's own numbers: row index ``0``,
    ``len(row) == 2``, and ``expected_width == len(headers) == 3``.
    Full-string equality is asserted, so every component of the message is
    pinned rather than merely its presence.

    Mutation detected: changing the reported row index, the reported
    value count, or the declared-header count; comparing against the wrong
    width (for example ``len(row_set)`` instead of ``len(headers)``); or
    moving the width check somewhere that no longer names the offending
    row.
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
