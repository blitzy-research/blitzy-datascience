"""Malformed-input and type-coercion tests for ``utils.schema_normalizer``.

Covers the **parsing** and **type-coercion** stages of the ingestion
pipeline: the validation gate of
:func:`utils.schema_normalizer.normalize_result_sets` and the pandas
dtype-inference behaviour of the single
``pd.DataFrame(row_set, columns=headers)`` construction site
(``utils/schema_normalizer.py`` line 352).

Contracts pinned here
---------------------
* **Private helpers are reached ONLY through the public entry point.**
  ``_extract_tables``, ``_require_str``, and ``_require_list`` are
  exercised exclusively by feeding malformed envelopes to the public
  ``normalize_result_sets``. No private helper is imported, promoted, or
  given a test-only hook, so no production source file changes.
* **Ten rejection branches, ten exact messages.** Each envelope in
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
* **Duplicate names are uniquified, never overwritten.** A name that
  repeats within one envelope is suffixed with the next free ordinal
  (``PlayByPlay`` twice -> ``play_by_play`` and ``play_by_play_2``)
  instead of the second table
  replacing the first, and the near-miss spelling that merely
  *resembles* a generated key stays distinct from it: an upstream name
  already ending in a digit snake-cases to ``play_by_play2`` (no
  underscore) while the generated suffix is ``play_by_play_2`` (with
  one). Both hand-derived key lists in AAP §0.4.2.3 are pinned —
  ``['x', 'x_2', 'x_3']`` by the sibling ``test_schema_normalizer.py``
  and ``['play_by_play', 'play_by_play2', 'play_by_play_2']`` here.
* **The occupied-generated-suffix case is LOSS-FREE, and that is
  asserted as an equality.** When the ordinal the uniquifier computes
  names a key an earlier table already holds, the uniquifier probes
  upward to the first free ordinal instead of overwriting that frame, so
  ``len(mapping)`` always equals the number of tables in the envelope.
  Two tests below pin that exactly — the ordered key list, the frame
  count against the upstream table count, and one distinguishing cell
  per frame, because a correct key list alone cannot detect an
  overwrite. Both were the failing cases that motivated **CD-2**, the
  minimal three-statement probe fix in ``utils/schema_normalizer.py``
  (L150-166) exercised under Constraint C2's "genuine bug found"
  allowance (AAP §0.1.4) and called out with its failing case in the
  section banner below. Before that fix, ``PlayByPlay`` /
  ``PlayByPlay_2`` / ``PlayByPlay`` returned two frames for three tables
  with cells ``[1, 3]`` — silent upstream data loss with no exception
  raised.
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
#: (``(.)([A-Z][a-z]+)``, L417) nor ``_CAMEL_BOUNDARY_2``
#: (``([a-z0-9])([A-Z])``, L421) matches a single lowercase character,
#: and ``.lower().strip("_")`` leaves it untouched (L454-456).
EXPECTED_MESSAGES: Dict[str, str] = {
    # L347-350: f"Result set '{name}' row {idx} has {len(row)} values "
    #           f"but {expected_width} headers are declared"
    # Envelope: headers ["A", "B", "C"] (width 3) against rowSet [[1, 2]]
    # (row 0, length 2) -> idx=0, len(row)=2, expected_width=3.
    "row_width_mismatch": (
        "Result set 't' row 0 has 2 values but 3 headers are declared"
    ),
    # L254-257: f"Result set is missing required string field '{key}' "
    #           f"(got {type(value).__name__})"
    # Envelope omits "name" entirely, so ``table.get("name")`` (L252) is
    # ``None`` and ``type(None).__name__`` is "NoneType".
    "missing_name": (
        "Result set is missing required string field 'name' (got NoneType)"
    ),
    # L254-257 again, reached through the SECOND clause of the same guard
    # (L253): ``if not isinstance(value, str) or not value``. Here ``name``
    # is present as the int 7, so ``isinstance(value, str)`` is False and
    # ``type(7).__name__`` is "int". Without this case a mutation narrowing
    # the guard to ``if value is None`` stays green: the int would flow into
    # ``_snake_case`` (L138) and raise ``TypeError`` from the regex engine
    # instead of the operator-facing ``ValueError``.
    "name_not_string": (
        "Result set is missing required string field 'name' (got int)"
    ),
    # L254-257 once more, reached through the ``or not value`` clause alone:
    # ``name`` is present AND is a str, so only emptiness can reject it, and
    # ``type("").__name__`` is "str". Without this case a mutation dropping
    # ``or not value`` stays green: an empty name would be accepted and keyed
    # as the empty string, silently producing a nameless artifact stem.
    "name_empty": (
        "Result set is missing required string field 'name' (got str)"
    ),
    # L288-291: f"Result set field '{key}' must be a list; "
    #           f"got {type(value).__name__}"
    # Envelope supplies headers as the dict {"A": 1} -> "dict".
    "headers_not_list": (
        "Result set field 'headers' must be a list; got dict"
    ),
    # L288-291 again, reached for the ``rowSet`` key on the next line
    # (L140). The envelope omits "rowSet", so ``table.get("rowSet")`` is
    # ``None`` -> "NoneType". An EMPTY list would be valid here
    # (L265-267): only a non-list trips this guard.
    "missing_row_set": (
        "Result set field 'rowSet' must be a list; got NoneType"
    ),
    # L288-291 again for the ``rowSet`` key, this time with the key PRESENT
    # and holding the dict ``{}`` -> "dict". The absent-key case above can
    # only prove the guard rejects ``None``; this one proves it rejects a
    # concrete wrong TYPE, so a mutation narrowing the check to
    # ``if value is None`` is caught. The two together also pin that the
    # message reports the offending type rather than a fixed word.
    "row_set_not_list": (
        "Result set field 'rowSet' must be a list; got dict"
    ),
    # L143-145: f"Result set '{name}' contains non-string headers: {headers}"
    # ``{headers}`` interpolates the list's repr, so ["A", 7] renders as
    # ['A', 7] -- note the single quotes around A and the space after the
    # comma. This guard (L142) runs AFTER both _require_list calls, so the
    # envelope must still carry a valid name and a list-typed rowSet.
    "non_string_header": (
        "Result set 't' contains non-string headers: ['A', 7]"
    ),
    # L342-345: f"Result set '{name}' row {idx} is {type(row).__name__}, "
    #           f"expected list/tuple"
    # The envelope's row is the dict {"A": 1} against the single header
    # ["A"], so the row TYPE check (L341) is the only guard it can trip:
    # the width check (L346) sees len(row) == expected_width == 1. The
    # rendered type name is therefore "dict" and the row index is 0,
    # keeping this entry a clean single-branch probe of the TYPE guard.
    "row_not_sequence": (
        "Result set 't' row 0 is dict, expected list/tuple"
    ),
    # L214-216: f"'resultSets' must be a list or dict, got {type(raw).__name__}"
    # The envelope supplies the str "not-a-list" -> "str". This is the
    # _extract_tables TYPE guard, distinct from the "Payload contains no
    # result sets" message (L124-127) raised when the extracted table
    # list is empty.
    "result_sets_wrong_type": (
        "'resultSets' must be a list or dict, got str"
    ),
}

#: One malformed envelope per rejection branch. Fixed by the ten-entry
#: table above; asserted against the shared fixture so a dropped envelope
#: cannot silently shrink the parametrized matrix. Ten, not seven, because
#: the two multi-condition guards are covered on every condition: three
#: ``name`` rows for ``_require_str``'s absent / non-string / empty
#: conditions, and two ``rowSet`` rows for ``_require_list``'s absent-key
#: and wrong-type conditions.
EXPECTED_REJECTION_BRANCH_COUNT = 10

#: Deterministic ``(case, expected_message)`` pairs for parametrization.
#: A fixture cannot be referenced inside ``@pytest.mark.parametrize``, so
#: the local table drives the matrix and the fixture is indexed by case
#: name inside the test body. Sorted for a stable test-id ordering.
MALFORMED_CASES: tuple[tuple[str, str], ...] = tuple(
    sorted(EXPECTED_MESSAGES.items())
)

#: Headers of the inline dtype-probe envelope, in upstream order. The
#: normalizer passes ``headers`` straight through as ``columns`` (L352),
#: so column identity and order must survive verbatim.
EXPECTED_DTYPE_COLUMNS: list[str] = ["PLAYER_ID", "PTS", "NOTE"]

#: pandas dtype inference for ``pd.DataFrame(row_set, columns=headers)``
#: -- the sole construction site (L352) -- over the rowSet
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
#: order. It is the second of the two key lists AAP §0.4.2.3 derives by
#: hand, and it is a near miss rather than an exact clash: the digit
#: suffix the upstream itself supplies (``play_by_play2``) differs by one
#: character from the suffix the uniquifier generates
#: (``play_by_play_2``), so all three tables keep their own key.
#: Derivation, traced through both ``_snake_case`` regexes (L417 / L421 /
#: L454-456) and the uniquifier (L150-166):
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
#: against the number of tables sent, which is what makes the loss-free
#: guarantee an equality rather than a key-list coincidence.
OCCUPIED_SUFFIX_TABLE_COUNT = 3
CONSECUTIVE_SUFFIX_TABLE_COUNT = 5

#: Keys produced by the OCCUPIED-generated-suffix envelope, whose tables
#: are named ``PlayByPlay``, ``PlayByPlay_2``, ``PlayByPlay`` in that
#: order. Unlike the near miss above, the second table's own upstream name
#: snake-cases to EXACTLY the key the uniquifier's first candidate would
#: use for the third, so the two contend for one key and the probe loop is
#: what keeps both tables. Derivation, traced through the same regexes
#: (L417 / L421 / L454-456) and the uniquifier (L150-166):
#:
#: * table 1 ``"PlayByPlay"`` -> ``"play_by_play"``; the key is free, so
#:   ``dataframes["play_by_play"] = <table 1>``.
#: * table 2 ``"PlayByPlay_2"``: ``_CAMEL_BOUNDARY_1`` gives
#:   ``"Play_ByPlay_2"``, ``_CAMEL_BOUNDARY_2`` gives ``"Play_By_Play_2"``,
#:   lower/strip -> ``"play_by_play_2"``. That name is NOT yet a key, so it
#:   takes the bare form: ``dataframes["play_by_play_2"] = <table 2>``.
#: * table 3 ``"PlayByPlay"`` -> ``"play_by_play"``, which IS a key, so the
#:   starting ordinal is ``seen_names.get("play_by_play", 1) + 1 == 2``.
#:   ``"play_by_play_2"`` is already held by table 2, so the ``while``
#:   probe advances the ordinal to ``3`` and the free key
#:   ``"play_by_play_3"`` receives table 3.
#:
#: Result: THREE keys for THREE upstream tables — nothing is overwritten
#: and nothing is lost.
EXPECTED_OCCUPIED_SUFFIX_KEYS: list[str] = [
    "play_by_play",
    "play_by_play_2",
    "play_by_play_3",
]

#: First-column cell of each frame, in key order. Each key holds its OWN
#: table's data: table 1's ``1``, table 2's ``2`` under the key that table
#: 2 named itself, and table 3's ``3`` under the probed-forward key. Under
#: the pre-fix single-ordinal assignment this list read ``[1, 3]`` — the
#: missing ``2`` was table 2's frame, silently replaced.
EXPECTED_OCCUPIED_SUFFIX_CELLS: list[int] = [1, 2, 3]

#: Keys produced by the CONSECUTIVE-occupancy envelope, whose tables are
#: named ``X``, ``X_2``, ``X_3``, ``X``, ``X`` in that order. It proves the
#: probe advances past a RUN of occupied ordinals rather than only one.
#: Derivation (``_snake_case`` lowercases each name unchanged — no
#: CamelCase boundary matches a single letter or a letter-digit pair):
#:
#: * table 1 ``X`` -> ``x`` (free) -> ``x`` holds cell 1.
#: * table 2 ``X_2`` -> ``x_2`` (free, a different upstream NAME) -> cell 2.
#: * table 3 ``X_3`` -> ``x_3`` (free) -> cell 3.
#: * table 4 ``X`` repeats ``x``: the starting ordinal ``1 + 1 == 2`` names
#:   the occupied ``x_2``, so the probe advances to ``3`` (also occupied)
#:   and then to ``4`` -> ``x_4`` holds cell 4, and ``seen_names["x"]``
#:   becomes ``4``.
#: * table 5 ``X`` repeats ``x``: the starting ordinal ``4 + 1 == 5`` names
#:   the free ``x_5`` -> cell 5.
#:
#: Result: FIVE keys for FIVE upstream tables. Under the pre-fix ordinal
#: this envelope returned only THREE frames, with tables 2 and 3 replaced.
EXPECTED_CONSECUTIVE_SUFFIX_KEYS: list[str] = ["x", "x_2", "x_3", "x_4", "x_5"]

#: First-column cell of each frame, in key order. The identity mapping
#: ``[1, 2, 3, 4, 5]`` is the loss-free property stated as data: every
#: upstream table's own row is reachable under its own key. Under the
#: pre-fix ordinal this list read ``[1, 4, 5]``.
EXPECTED_CONSECUTIVE_SUFFIX_CELLS: list[int] = [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# Type coercion — dtype inference at the sole DataFrame construction site
# ---------------------------------------------------------------------------
#
# ``_build_dataframe`` ends in a single
# ``pd.DataFrame(row_set, columns=headers)`` call (L352) and performs no
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
    value repair: its body (L295-352) contains no ``astype``, no
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
# Duplicate result-set names — uniquification instead of overwriting
# ---------------------------------------------------------------------------
#
# Duplicate names are a duplicate-record concern inside the PARSING
# stage: two tables sharing a name must both survive, because the
# uniquifier (L150-166) is the only thing standing between a repeated
# upstream name and one table's data vanishing from the output.
#
# The envelope exercised here is the second of the two key lists AAP
# §0.4.2.3 derives by hand: ``PlayByPlay``, ``PlayByPlay2``,
# ``PlayByPlay``. ``PlayByPlay2`` snake-cases to ``play_by_play2`` (no
# underscore) while the repeated ``PlayByPlay`` is suffixed to
# ``play_by_play_2`` (with one), so the two spellings stay one character
# apart and all three tables keep their own key and their own data. The
# first of the two lists — three tables all named ``X`` yielding
# ``['x', 'x_2', 'x_3']`` — is owned by the sibling module
# ``tests/unit/utils/test_schema_normalizer.py`` and is deliberately not
# duplicated here.
# ---------------------------------------------------------------------------


def test_duplicate_name_beside_a_digit_suffixed_sibling_keeps_every_table() -> None:
    """A repeated name and a digit-suffixed sibling stay distinct, keeping 3 tables.

    Hand-derived in :data:`EXPECTED_NEAR_MISS_KEYS`: ``PlayByPlay`` ->
    ``play_by_play``, ``PlayByPlay2`` -> ``play_by_play2`` (a different
    upstream name with no underscore, so it keeps a bare key), and the
    repeated ``PlayByPlay`` -> ``play_by_play_2`` from the ordinal
    ``seen_names.get("play_by_play", 1) + 1 == 2``. Three tables in,
    three frames out, each holding its own row.

    This is the exact key list AAP §0.4.2.3 derives by hand for this
    envelope, and it is asserted as a whole ordered list together with
    the distinguishing first cell of every frame — a correct key list
    alone cannot detect an overwrite, only the cells can.

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
# Duplicate result-set names — the OCCUPIED-suffix case, loss-free (CD-2)
# ---------------------------------------------------------------------------
#
# The near-miss envelope above never contends for a key because the
# sibling's spelling (``play_by_play2``) differs from the generated key
# (``play_by_play_2``). When the two coincide, the uniquifier's ordinal
# must be a STARTING POINT rather than a verdict, and the two tests below
# pin exactly that: every upstream table survives under its own key.
#
# CD-2 — THE DEFECT THESE TESTS MOTIVATED, with its failing case:
#
#   Before the fix, the uniquifier computed ONE candidate key and assigned
#   to it unconditionally. For an envelope naming its tables
#   ``PlayByPlay``, ``PlayByPlay_2``, ``PlayByPlay`` the third table's
#   candidate key ``play_by_play_2`` was already held by the SECOND table,
#   whose own upstream name snake-cases to that exact string. The
#   assignment replaced table 2's DataFrame, so the mapping held
#   ``{'play_by_play': <table 1>, 'play_by_play_2': <table 3>}`` — THREE
#   tables in, TWO frames out, cells ``[1, 3]``, and no exception raised.
#   A whole upstream result set disappeared silently, which contradicts
#   the function's own ``Returns`` contract (faithful structural
#   translation of every table in the envelope) and is data loss rather
#   than a cosmetic naming quirk.
#
# THE MINIMAL FIX (``utils/schema_normalizer.py`` L150-166, three added
# statements) — probe forward to the first FREE ordinal:
#
#   ordinal = seen_names.get(name, 1) + 1
#   while f"{name}_{ordinal}" in dataframes:
#       ordinal += 1
#   seen_names[name] = ordinal
#   dataframes[f"{name}_{ordinal}"] = df
#
# It changes no signature, adds no import and alters no control flow other
# than the probe, and it leaves every previously asserted key sequence
# intact: three tables all named ``X`` still yield ``['x', 'x_2', 'x_3']``
# (frozen sibling module ``tests/unit/utils/test_schema_normalizer.py``)
# and the near-miss envelope above still yields
# ``['play_by_play', 'play_by_play2', 'play_by_play_2']``. Both facts are
# re-proved by the suite rather than asserted here.
#
# Scope note: the fix is exercised under Constraint C2, which permits a
# non-test source change "to fix a genuine bug found" provided it is
# minimal and called out explicitly with the failing case that motivated
# it (AAP §0.1.4). The failing case is the envelope above; the call-out is
# this banner plus the resolution report.
# ---------------------------------------------------------------------------


def test_repeat_whose_generated_key_is_occupied_keeps_every_table() -> None:
    """3 tables named PlayByPlay / PlayByPlay_2 / PlayByPlay return 3 distinct frames.

    Hand-derived in :data:`EXPECTED_OCCUPIED_SUFFIX_KEYS` and
    :data:`EXPECTED_OCCUPIED_SUFFIX_CELLS`: the repeated ``PlayByPlay``
    starts at ordinal ``2``, finds ``play_by_play_2`` occupied by table 2's
    own upstream name, and probes on to the free ``play_by_play_3``. Three
    tables in, three frames out, cells ``[1, 2, 3]``.

    Asserting the frame count against
    :data:`OCCUPIED_SUFFIX_TABLE_COUNT` is what makes the loss-free
    guarantee an equality — ``len(mapping) == len(tables)`` — rather than
    something merely implied by a key list.

    Mutations detected: deleting the ``while`` probe so the single ordinal
    is assigned unconditionally (CD-2 itself — two frames for three tables,
    cells ``[1, 3]``); dropping the uniquifier entirely so a repeat
    overwrites the BARE key (cells ``[3, 2]``); renumbering the ordinal
    from ``_1``; and changing the ``_`` separator (the key list changes).
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
        f"an occupied generated suffix must be probed past, yielding "
        f"{EXPECTED_OCCUPIED_SUFFIX_KEYS}; got {list(out.keys())}"
    )

    # Assert -- loss-free, quantified: frames returned == tables supplied.
    assert len(out) == OCCUPIED_SUFFIX_TABLE_COUNT, (
        f"every upstream table must survive: {OCCUPIED_SUFFIX_TABLE_COUNT} "
        f"tables were supplied but {len(out)} frames were returned. A count "
        f"below {OCCUPIED_SUFFIX_TABLE_COUNT} means a generated key "
        f"overwrote a sibling table's frame (CD-2)"
    )

    # Assert -- each key holds its OWN table's data, so nothing was
    # displaced into another table's slot.
    observed_cells = [
        int(out[key].iloc[0, 0]) for key in EXPECTED_OCCUPIED_SUFFIX_KEYS
    ]
    assert observed_cells == EXPECTED_OCCUPIED_SUFFIX_CELLS, (
        f"each key must hold its own table's row; expected cells "
        f"{EXPECTED_OCCUPIED_SUFFIX_CELLS} but got {observed_cells}. A "
        f"missing 2 means table 2's frame was replaced by table 3's"
    )


def test_repeats_against_consecutive_occupied_suffixes_keep_every_table() -> None:
    """5 tables named X / X_2 / X_3 / X / X return 5 distinct frames.

    The compounding form of the same contention, hand-derived in
    :data:`EXPECTED_CONSECUTIVE_SUFFIX_KEYS` and
    :data:`EXPECTED_CONSECUTIVE_SUFFIX_CELLS`: the fourth table's starting
    ordinal ``2`` names the occupied ``x_2``, so the probe walks the whole
    run — ``2`` occupied, ``3`` occupied, ``4`` free — and the fifth table
    then starts from the recorded ``4`` and takes ``x_5``. Five keys for
    five upstream tables with cells ``[1, 2, 3, 4, 5]``.

    This is the case a single-step probe (``if`` instead of ``while``) would
    still lose, which is why the run of occupied ordinals is two long.

    Mutations detected: replacing the ``while`` probe with a single ``if``
    (table 4 would advance only to the occupied ``x_3`` and displace table
    3, yielding four frames and cells ``[1, 2, 4, 5]``); deleting the probe
    altogether (CD-2 — three frames, cells ``[1, 4, 5]``); and keying a
    repeat off the bare name (one frame). Deliberately NOT claimed:
    dropping the ``seen_names[name] = ordinal`` bookkeeping is invisible
    here, because the probe would simply re-walk the occupied run and reach
    the same free key — that statement is an optimisation, not a
    correctness guarantee, and this test does not pretend otherwise.
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
        f"the probe must walk the whole run of occupied suffixes, yielding "
        f"{EXPECTED_CONSECUTIVE_SUFFIX_KEYS}; got {list(out.keys())}"
    )

    # Assert -- loss-free, quantified: frames returned == tables supplied.
    assert len(out) == CONSECUTIVE_SUFFIX_TABLE_COUNT, (
        f"every upstream table must survive: {CONSECUTIVE_SUFFIX_TABLE_COUNT} "
        f"tables were supplied but {len(out)} frames were returned. A count "
        f"below {CONSECUTIVE_SUFFIX_TABLE_COUNT} means at least one repeat "
        f"displaced a sibling table's frame (CD-2)"
    )

    # Assert -- the identity mapping of key order to upstream table order.
    observed_cells = [
        int(out[key].iloc[0, 0]) for key in EXPECTED_CONSECUTIVE_SUFFIX_KEYS
    ]
    assert observed_cells == EXPECTED_CONSECUTIVE_SUFFIX_CELLS, (
        f"each key must hold its own table's row; expected cells "
        f"{EXPECTED_CONSECUTIVE_SUFFIX_CELLS} but got {observed_cells}. Any "
        f"absent value names the upstream table whose frame was replaced"
    )


# ---------------------------------------------------------------------------
# Malformed envelopes — the ten exact ValueError messages
# ---------------------------------------------------------------------------
#
# The five guards exercised here -- ``_extract_tables`` (L176),
# ``_require_str`` (L229), ``_require_list`` (L261), the
# non-string-headers branch (L142) and the row-not-a-sequence branch
# (L341) -- are all reached exclusively through the public
# ``normalize_result_sets``; no private helper is imported and no
# production source file is touched.
#
# Ten envelopes cover those five guards because two of them are
# multi-condition: ``_require_str`` (L253) rejects an absent, a non-string
# AND an empty ``name`` in one ``or``-joined predicate, and
# ``_require_list`` (L286) rejects both an absent key and a wrong TYPE.
# Each condition is independently mutable, so each gets its own envelope
# and its own exact message — the type name in the message is what
# distinguishes them (``NoneType`` / ``int`` / ``str`` for ``name``;
# ``NoneType`` / ``dict`` for ``rowSet``).
#
# Guard ORDER inside the per-table loop (L137-148) determines which
# envelope trips which branch, so the shared fixture's shapes are
# minimal but deliberate:
#
#   L138  name    = _snake_case(_require_str(table, "name"))
#   L139  headers = _require_list(table, "headers")
#   L140  row_set = _require_list(table, "rowSet")
#   L142  non-string headers check   (AFTER both _require_list calls)
#   L147  _build_dataframe -> row TYPE check (L341) then WIDTH (L346)
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

    The ten expected strings in :data:`EXPECTED_MESSAGES` are the
    production f-strings rendered by hand, each annotated with its source
    line. ``re.escape`` keeps the regex literal so the punctuation in the
    messages cannot silently relax the pattern. The error class is pinned
    too: a non-dict payload raises ``TypeError`` at L117-120, so
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

    Hand-derived from L347-350 with the fixture's own numbers: row index
    ``0``, ``len(row) == 2``, and ``expected_width == len(headers) == 3``.
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
