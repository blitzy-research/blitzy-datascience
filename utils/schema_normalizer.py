"""Flatten NBA Stats ``resultSets`` JSON envelopes into pandas DataFrames.

The NBA Stats API returns a JSON envelope whose data section is either
``resultSets`` (a list of ``{name, headers, rowSet}`` table dicts) or,
for a small number of endpoints, a single ``resultSet`` object with the
same shape. This module converts such envelopes into a mapping of
result-set name (snake_cased for CSV-friendly filenames) to a flat
``pandas.DataFrame`` suitable for emission by
``storage/csv_writer.CSVWriter`` (Rule 4).

Design properties
-----------------
* **Pure function** — ``normalize_result_sets`` has no side effects, no
  module-level state other than two compiled regex patterns, and no
  I/O. It is trivially unit-testable with no mocks.
* **Single enforcement point for Rule 4** — every DataFrame produced
  here is validated against the "no nested cells" invariant before it
  is returned. Callers (pipelines) do not need to re-verify.
* **pandas 2.0 / 2.1 / 2.3 compatibility** — the Rule 4 post-condition
  uses ``DataFrame.map`` on pandas >= 2.1 and falls back to
  ``DataFrame.applymap`` on pandas 2.0.x, avoiding the deprecation
  warning introduced in pandas 2.1 (Gate 2: zero-warning build).

Error-type discipline
---------------------
* ``TypeError`` is raised when the *kind* of the input object is wrong
  (e.g., the caller passed a list instead of a dict).
* ``ValueError`` is raised when the input *contains* invalid data — a
  missing key, a malformed result-set entry, a shape mismatch between
  headers and rows, non-string headers, or a Rule 4 violation.

Authoritative references
------------------------
* Agent Action Plan §0.1.3, §0.4.1.1, §0.5.1.2, §0.7.2.4
* Product brief ``docs/New_Product_Prompt_20260418.md`` §5 Rule 4

Public API
----------
``normalize_result_sets(payload: dict) -> Dict[str, pandas.DataFrame]``
"""

import re
from typing import Any, Dict, Iterable, List  # noqa: F401

import pandas as pd


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def normalize_result_sets(payload: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """Flatten an NBA Stats API JSON envelope into a dict of DataFrames.

    Iterates every table entry under ``payload['resultSets']`` (or the
    singular ``payload['resultSet']`` variant) and constructs one flat
    ``pandas.DataFrame`` per entry via
    ``pd.DataFrame(rowSet, columns=headers)``. Each DataFrame is then
    validated against Rule 4: no cell may contain a ``dict`` or
    ``list``. The result-set names returned by NBA Stats use
    CamelCase (e.g. ``"LeagueDashPlayerStats"``); they are converted
    to snake_case (``"league_dash_player_stats"``) so callers can pass
    the key directly to ``CSVWriter.write`` as the ``name`` argument,
    producing CSV filenames that match the project convention in
    ``output/``.

    Parameters
    ----------
    payload : dict
        The raw JSON-decoded dict returned by
        :py:meth:`api.nba_client.NBAClient.get`. Must contain either
        a ``resultSets`` key (list or single dict) or a
        ``resultSet`` key (same shape).

    Returns
    -------
    dict[str, pandas.DataFrame]
        Mapping of snake_cased result-set name to a flat DataFrame.
        Every returned DataFrame is guaranteed to satisfy Rule 4: no
        cell contains a ``dict`` or ``list``. Empty result sets
        (``rowSet`` is ``[]``) are returned as DataFrames with the
        correct columns and zero rows — this is legitimate upstream
        behaviour for rare filter combinations.

    Raises
    ------
    TypeError
        If ``payload`` is not a ``dict``.
    ValueError
        If ``payload`` is missing both ``resultSets`` and
        ``resultSet`` keys, if the envelope is empty, if any
        table entry is malformed (wrong type, missing ``name``,
        ``headers``, or ``rowSet``; non-string headers; row-length
        mismatch), or if the Rule 4 post-condition fails.

    Examples
    --------
    >>> payload = {
    ...     "resultSets": [
    ...         {
    ...             "name": "LeagueDashPlayerStats",
    ...             "headers": ["PLAYER_ID", "PTS"],
    ...             "rowSet": [[2544, 25.7], [1629029, 32.4]],
    ...         }
    ...     ]
    ... }
    >>> frames = normalize_result_sets(payload)
    >>> list(frames.keys())
    ['league_dash_player_stats']
    >>> frames['league_dash_player_stats'].shape
    (2, 2)
    """
    if not isinstance(payload, dict):
        raise TypeError(
            f"payload must be a dict; got {type(payload).__name__}"
        )

    tables = _extract_tables(payload)
    if not tables:
        raise ValueError(
            "Payload contains no result sets (neither 'resultSets' nor "
            "'resultSet' present, or both were empty)."
        )

    dataframes: Dict[str, pd.DataFrame] = {}
    # Track duplicate names so collisions are suffixed deterministically
    # (e.g. two tables both named "PlayByPlay" -> "play_by_play",
    # "play_by_play_2") rather than silently overwriting.
    seen_names: Dict[str, int] = {}
    for table in tables:
        name = _snake_case(_require_str(table, "name"))
        headers = _require_list(table, "headers")
        row_set = _require_list(table, "rowSet")

        if any(not isinstance(h, str) for h in headers):
            raise ValueError(
                f"Result set '{name}' contains non-string headers: {headers}"
            )

        df = _build_dataframe(name, headers, row_set)
        _assert_rule4_flat(df, name)

        if name in dataframes:
            seen_names[name] = seen_names.get(name, 1) + 1
            deduped = f"{name}_{seen_names[name]}"
            dataframes[deduped] = df
        else:
            dataframes[name] = df

    return dataframes


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------
def _extract_tables(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the list of table dicts from the payload envelope.

    Accepts three observed upstream shapes:

    * ``payload['resultSets']`` is a ``list`` of table dicts
      (canonical; used by the majority of endpoints).
    * ``payload['resultSets']`` is a single ``dict`` (rare; treated
      as a one-element list for consistency).
    * ``payload['resultSet']`` (singular) is a ``dict`` or ``list``
      (older endpoints such as ``playbyplayv2`` variants).

    Non-dict entries inside a list are filtered out silently; this
    tolerates upstream envelopes that occasionally embed non-table
    metadata (e.g. ``None`` placeholders) alongside real tables.

    Parameters
    ----------
    payload : dict
        The raw payload dict.

    Returns
    -------
    list[dict[str, Any]]
        The extracted table dicts, possibly empty.

    Raises
    ------
    ValueError
        If ``resultSets`` or ``resultSet`` is present but its value is
        neither a list nor a dict.
    """
    if "resultSets" in payload:
        raw = payload["resultSets"]
        if isinstance(raw, list):
            return [t for t in raw if isinstance(t, dict)]
        if isinstance(raw, dict):
            return [raw]
        raise ValueError(
            f"'resultSets' must be a list or dict, got {type(raw).__name__}"
        )
    if "resultSet" in payload:
        raw = payload["resultSet"]
        if isinstance(raw, list):
            return [t for t in raw if isinstance(t, dict)]
        if isinstance(raw, dict):
            return [raw]
        raise ValueError(
            f"'resultSet' must be a list or dict, got {type(raw).__name__}"
        )
    return []


def _require_str(table: Dict[str, Any], key: str) -> str:
    """Return ``table[key]`` as a non-empty string, or raise ValueError.

    Used to validate the mandatory ``name`` field on every result set.

    Parameters
    ----------
    table : dict
        The result-set dict.
    key : str
        The key expected to map to a non-empty string.

    Returns
    -------
    str
        The string value.

    Raises
    ------
    ValueError
        If ``key`` is absent, maps to a non-string value, or maps to
        an empty string.
    """
    value = table.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"Result set is missing required string field '{key}' "
            f"(got {type(value).__name__})"
        )
    return value


def _require_list(table: Dict[str, Any], key: str) -> List[Any]:
    """Return ``table[key]`` as a list, or raise ValueError.

    Used to validate the mandatory ``headers`` and ``rowSet`` fields
    on every result set. An empty list is considered valid —
    ``rowSet`` of ``[]`` signals an upstream query with no matching
    rows, not a malformed envelope.

    Parameters
    ----------
    table : dict
        The result-set dict.
    key : str
        The key expected to map to a list.

    Returns
    -------
    list
        The list value (may be empty).

    Raises
    ------
    ValueError
        If ``key`` is absent or maps to a non-list value.
    """
    value = table.get(key)
    if not isinstance(value, list):
        raise ValueError(
            f"Result set field '{key}' must be a list; "
            f"got {type(value).__name__}"
        )
    return value


def _build_dataframe(
    name: str,
    headers: List[str],
    row_set: List[Any],
) -> pd.DataFrame:
    """Construct a flat DataFrame from an NBA Stats table entry.

    Three cases are handled:

    * ``row_set`` is empty — returns an empty DataFrame with the
      supplied column names (zero rows, ``len(headers)`` columns).
    * Every row is a list or tuple of exactly ``len(headers)`` values
      — returns ``pd.DataFrame(row_set, columns=headers)``.
    * Any row is not a list/tuple or has a length mismatch — raises
      ``ValueError`` naming the offending result set and row index.

    Parameters
    ----------
    name : str
        The snake_cased result-set name (used only for error messages).
    headers : list[str]
        The column names for the output DataFrame.
    row_set : list
        The list of rows; each row must itself be a list or tuple
        whose length matches ``len(headers)``.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with ``columns == headers`` and one row per
        entry in ``row_set``.

    Raises
    ------
    ValueError
        If any row is not a list/tuple, or if any row's length does
        not match ``len(headers)``.
    """
    if not row_set:
        return pd.DataFrame(columns=headers)

    # Validate every row's type and shape before construction so
    # errors reference the offending row index explicitly rather
    # than surfacing as opaque pandas exceptions downstream.
    expected_width = len(headers)
    for idx, row in enumerate(row_set):
        if not isinstance(row, (list, tuple)):
            raise ValueError(
                f"Result set '{name}' row {idx} is {type(row).__name__}, "
                f"expected list/tuple"
            )
        if len(row) != expected_width:
            raise ValueError(
                f"Result set '{name}' row {idx} has {len(row)} values "
                f"but {expected_width} headers are declared"
            )

    return pd.DataFrame(row_set, columns=headers)


def _assert_rule4_flat(df: pd.DataFrame, name: str) -> None:
    """Assert Rule 4 on ``df``: no cell may contain a ``dict`` or ``list``.

    Rule 4 (``docs/New_Product_Prompt_20260418.md`` §5): "CSV columns
    MUST NOT contain nested JSON, dicts, or lists." This function is
    the single enforcement point for that invariant; pipelines and
    the writer do not re-check.

    Implementation detail: pandas 2.1 introduced ``DataFrame.map`` as
    the element-wise equivalent of the deprecated ``DataFrame.applymap``
    (which emits a ``FutureWarning`` on every call in pandas >= 2.1).
    We prefer ``map`` when it is available and fall back to
    ``applymap`` only on pandas 2.0.x, so the Gate 2 "zero-warning
    build" requirement holds across the full supported pandas range.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to validate.
    name : str
        The snake_cased result-set name (used only in the error
        message to pinpoint which table is offending).

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If any cell of ``df`` is a ``dict`` or ``list``.
    """
    # Empty frames trivially satisfy the invariant — skip the check
    # to avoid needless work and to sidestep the pandas edge case
    # where ``.any().any()`` on a zero-row frame returns ``False``
    # but is semantically a no-op we would rather make explicit.
    if df.empty:
        return

    # pandas 2.1+ exposes ``DataFrame.map`` (element-wise) and marks
    # ``applymap`` as deprecated; pandas 2.0.x has only ``applymap``.
    # ``getattr(df, "map", None)`` returns the bound ``map`` method on
    # 2.1+ (truthy) or ``None`` on 2.0.x (falsy), selecting the right
    # implementation without triggering a deprecation warning.
    checker = getattr(df, "map", None) or df.applymap
    offending = checker(lambda x: isinstance(x, (dict, list))).any().any()
    if offending:
        raise ValueError(
            f"Rule 4 violation: result set '{name}' contains cells with "
            f"dict or list values (nested JSON in CSV is forbidden)."
        )


# Compiled CamelCase -> snake_case boundary patterns. Declared at module
# scope so the regexes are compiled exactly once per process, not on
# every call — ``_snake_case`` is invoked once per result-set entry and
# a pipeline run may produce thousands of entries (e.g. per-game
# iteration in ``pipelines/ingest_games.py``).
#
# Pattern 1: insert an underscore between any character and a trailing
# CamelCase block starting with an uppercase letter followed by one or
# more lowercase letters (e.g. ``"ABCd" -> "A_BCd"``).
_CAMEL_BOUNDARY_1 = re.compile(r"(.)([A-Z][a-z]+)")

# Pattern 2: insert an underscore between a lowercase letter or digit
# and an immediately following uppercase letter (e.g. ``"aB" -> "a_B"``).
_CAMEL_BOUNDARY_2 = re.compile(r"([a-z0-9])([A-Z])")


def _snake_case(name: str) -> str:
    """Convert a CamelCase identifier to snake_case.

    NBA Stats result-set names such as ``"LeagueDashPlayerStats"`` or
    ``"PlayByPlay"`` are converted to lowercase, underscore-separated
    identifiers suitable for direct use as CSV filename stems
    (``"league_dash_player_stats"``, ``"play_by_play"``). Leading and
    trailing underscores are stripped so purely-uppercase inputs or
    single-letter prefixes do not produce stray edge underscores.

    Examples
    --------
    >>> _snake_case("LeagueDashPlayerStats")
    'league_dash_player_stats'
    >>> _snake_case("PlayByPlay")
    'play_by_play'
    >>> _snake_case("GameHeader")
    'game_header'

    Parameters
    ----------
    name : str
        The CamelCase identifier.

    Returns
    -------
    str
        The snake_case equivalent, lowercased, with no leading or
        trailing underscores.
    """
    s = _CAMEL_BOUNDARY_1.sub(r"\1_\2", name)
    s = _CAMEL_BOUNDARY_2.sub(r"\1_\2", s)
    return s.lower().strip("_")
