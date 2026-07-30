"""Schedule domain endpoint wrapper (Feature F-013).

This module wraps the single NBA Stats endpoint that backs the Schedule
data domain (``leaguegamefinder``) plus a derived helper that enumerates
the unique ``GAME_ID`` values for a season. The helper is the SOLE
cross-domain dependency expressed in this codebase: the Games pipeline
(:mod:`pipelines.ingest_games`) calls :func:`enumerate_game_ids` at the
top of its ``run()`` to determine which games to iterate.

Endpoint
--------

``leaguegamefinder`` is a league-wide game finder that returns one row
per (team, game) pair for the configured ``Season``, ``SeasonType``,
``LeagueID``, and ``PlayerOrTeam`` filter. This module pins
``PlayerOrTeam="T"`` (team-level rows — one row per team per game) so
the upstream envelope returns roughly 2 * number-of-games rows. The
:func:`enumerate_game_ids` helper dedupes to the canonical
one-row-per-game ``GAME_ID`` list (roughly 1,230 for a full regular
season) while preserving first-seen ordering for deterministic
iteration, which is important for Gate 8 resume determinism.

Rule compliance
---------------

This module contains no direct HTTP transport (Rule 1 — all traffic
routes through :class:`api.nba_client.NBAClient`) and no CSV emission
(Rule 7 — only :class:`storage.csv_writer.CSVWriter` calls
``DataFrame.to_csv``). It does NOT import ``requests``, ``pandas``,
``json``, or filesystem I/O modules. The :func:`enumerate_game_ids`
helper walks the raw JSON envelope structure with pure-Python list and
dict primitives — pandas/normalizer composition is deferred to the
Schedule pipeline.

Cross-domain dependency
-----------------------

Per AAP §0.4.5 and :doc:`docs/api/endpoints_catalog.md` §9,
:mod:`pipelines.ingest_games` depends on :func:`enumerate_game_ids` —
not on ``output/schedule.csv``. This keeps standalone
``python run.py games --season <season>`` functional even when no
schedule pipeline has run previously: the Games pipeline re-enumerates
``GAME_ID`` values on demand against the live API.

Envelope resilience
-------------------

Both functions are defensive about the ``resultSets`` envelope shape:
``fetch_leaguegamefinder`` returns the raw dict unmodified, while
:func:`enumerate_game_ids` returns an empty list (and logs a WARNING)
when the envelope is missing or lacks a ``GAME_ID`` column — never
raises a :class:`KeyError` on payload shape issues. Exceptions from
the HTTP transport (``HTTPError``, ``Timeout``, ``RequestException``)
after tenacity retry exhaustion still propagate.

Observability
-------------

A DEBUG-level log record is emitted before each delegation to
``client.get``; an INFO-level game-count summary follows each
successful enumeration; a WARNING is emitted when the upstream
envelope is shaped unexpectedly. All log calls use ``%s`` placeholders
so the stdlib formatter only renders parameter values when the
corresponding log level is actually enabled. Request bodies and
response payloads are never logged from this module.
"""

from typing import Any, Dict, List, Optional  # noqa: F401  (Optional reserved for type-annotation flexibility)

from api.nba_client import NBAClient
from utils.logger import get_logger

import config

logger = get_logger(__name__)


# ----------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------


def fetch_leaguegamefinder(
    client: NBAClient,
    season: str,
    season_type: str = config.DEFAULT_SEASON_TYPE,
    league_id: str = config.DEFAULT_LEAGUE_ID,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Fetch the league-wide game finder result set for a season.

    The wrapper pins ``PlayerOrTeam="T"`` — team-level rows, one row
    per team per game — as the Schedule/Games pipeline convention. The
    upstream envelope therefore returns two rows per game (home and
    away), which is why :func:`enumerate_game_ids` must deduplicate.

    The upstream ``leaguegamefinder`` endpoint accepts a large optional
    filter surface; the full set is populated with empty strings below
    because the NBA Stats API returns HTTP 400 on entirely-missing
    optional fields while accepting empty strings as "no filter".
    Callers who want to narrow the window can pass filter overrides
    via ``**kwargs`` — e.g., ``DateFrom="2025-10-01"``,
    ``DateTo="2025-10-31"``, ``TeamID="1610612747"``.

    Args:
        client: Shared :class:`api.nba_client.NBAClient` instance. This
            is the sole HTTP transport in the pipeline (Rule 1); the
            wrapper delegates to ``client.get(endpoint, params)``.
        season: Season string in NBA format, e.g. ``"2025-26"``. See
            :data:`config.DEFAULT_SEASON` for the configured default
            season.
        season_type: Season type filter. Defaults to
            :data:`config.DEFAULT_SEASON_TYPE` (``"Regular Season"``).
            Other accepted values include ``"Playoffs"``,
            ``"Pre Season"``, and ``"All Star"``.
        league_id: NBA League ID. Defaults to
            :data:`config.DEFAULT_LEAGUE_ID` (``"00"``, the NBA itself).
            ``"10"`` (WNBA) and ``"20"`` (G-League) are also accepted by
            the upstream but are out of scope for this pipeline.
        **kwargs: Additional NBA Stats filters that override the
            defaults below. Applied via ``params.update(kwargs)`` after
            the base dict is built. Recognized filters include
            ``DateFrom``, ``DateTo``, ``TeamID``, ``Outcome``,
            ``Location``, ``VsConference``, ``VsDivision``,
            ``Conference``, ``Division``, ``SeasonSegment``, and
            ``GameID``.

    Returns:
        Raw JSON envelope returned by the upstream endpoint. The
        response carries a ``resultSets`` array whose
        ``LeagueGameFinderResults`` table is keyed by
        ``(GAME_ID, TEAM_ID)`` — one row per team per game. Downstream
        normalization produces ``schedule.csv``.

    Raises:
        requests.exceptions.HTTPError: Non-transient 4xx from the API.
        requests.exceptions.RequestException: Connection-level failure
            after tenacity retry exhaustion.
    """
    params: Dict[str, Any] = {
        "Season": season,
        "SeasonType": season_type,
        "LeagueID": league_id,
        "PlayerOrTeam": "T",
        "PlayerID": "",
        "TeamID": "",
        "Outcome": "",
        "Location": "",
        "DateFrom": "",
        "DateTo": "",
        "VsConference": "",
        "VsDivision": "",
        "Conference": "",
        "Division": "",
        "SeasonSegment": "",
        "GameID": "",
    }
    params.update(kwargs)
    logger.debug(
        "endpoints.schedule.leaguegamefinder season=%s season_type=%s",
        season,
        season_type,
    )
    return client.get("leaguegamefinder", params)


def enumerate_game_ids(
    client: NBAClient,
    season: str,
    season_type: str = config.DEFAULT_SEASON_TYPE,
    league_id: str = config.DEFAULT_LEAGUE_ID,
    **kwargs: Any,
) -> List[str]:
    """Return the deduplicated, first-seen-ordered list of ``GAME_ID`` values.

    This is the canonical ``GAME_ID`` enumerator used by
    :mod:`pipelines.ingest_games` (Feature F-011). It issues a single
    ``leaguegamefinder`` call (with ``PlayerOrTeam="T"`` pinned inside
    :func:`fetch_leaguegamefinder`, producing two rows per game — one
    for each team) and walks the raw envelope to extract and
    deduplicate ``GAME_ID`` values.

    Envelope handling is deliberately defensive: when the payload is
    empty or lacks a ``GAME_ID`` column (e.g., upstream schema change,
    invalid season string, or a season for which no games have yet been
    played), the function returns an empty list AFTER emitting a
    WARNING — it never raises on payload shape. HTTP-level exceptions
    from the transport still propagate (after tenacity retry
    exhaustion), because they signal a genuine availability problem
    that the caller must see.

    The helper manually walks the envelope rather than composing with
    :mod:`utils.schema_normalizer` to keep the endpoint layer
    independent of the normalizer. A single-column extraction is
    inexpensive in pure Python; DataFrame construction is reserved for
    pipelines that actually need the full rowset.

    Table discovery is header-based: the helper scans the
    ``resultSets`` array for the first entry whose ``headers`` list
    contains ``"GAME_ID"``. This is resilient to changes in the
    upstream table name (``LeagueGameFinderResults`` today) or table
    ordering, and it tolerates additional tables in the envelope
    without breaking the extraction.

    Deduplication uses insertion-ordered dict keys (CPython 3.7+
    language guarantee) so the returned list preserves the order in
    which each ``GAME_ID`` first appears in the raw ``rowSet`` — a
    precondition for Gate 8 resume determinism (``pipelines.ingest_games``
    checkpoints per ``GAME_ID`` and relies on stable iteration order to
    make an interrupted run reproducibly resume where it left off).

    Args:
        client: Shared :class:`api.nba_client.NBAClient` instance.
        season: Season string in NBA format, e.g. ``"2025-26"``.
        season_type: Season type filter. Defaults to
            :data:`config.DEFAULT_SEASON_TYPE` (``"Regular Season"``).
        league_id: NBA League ID. Defaults to
            :data:`config.DEFAULT_LEAGUE_ID` (``"00"``).
        **kwargs: Additional NBA Stats filters passed through to
            :func:`fetch_leaguegamefinder` and applied as
            ``params.update(kwargs)`` on the upstream dict. Note that
            ``PlayerOrTeam`` is NOT exposed here because the dedup
            logic assumes team-level rows (two rows per game).

    Returns:
        List of ``GAME_ID`` strings (typically 10-character
        zero-padded identifiers such as ``"0022500001"``) with
        duplicates removed, preserving the order in which each
        ``GAME_ID`` first appears in the raw ``rowSet``. An empty list
        is returned (never an exception) when the envelope is empty or
        the ``GAME_ID`` column is absent — a WARNING log record
        documents the unexpected envelope shape.

    Raises:
        requests.exceptions.HTTPError: Non-transient 4xx from the API
            (propagated through :func:`fetch_leaguegamefinder`).
        requests.exceptions.RequestException: Connection-level failure
            after tenacity retry exhaustion (propagated through
            :func:`fetch_leaguegamefinder`).
    """
    payload = fetch_leaguegamefinder(
        client=client,
        season=season,
        season_type=season_type,
        league_id=league_id,
        **kwargs,
    )

    # Walk the raw envelope structure. We intentionally avoid
    # utils.schema_normalizer here — normalizer composition belongs to
    # the Schedule pipeline, and this helper is invoked from the Games
    # pipeline directly during run() enumeration. Empty or malformed
    # envelopes are logged at WARNING and yield an empty list rather
    # than raising, so the Games pipeline can surface a "nothing to
    # iterate" signal to the operator without the exception noise.
    result_sets = payload.get("resultSets") or []
    if not result_sets:
        logger.warning(
            "endpoints.schedule.enumerate_game_ids empty payload season=%s",
            season,
        )
        return []

    # Find the first result-set table whose headers declare GAME_ID.
    # Searching by header (rather than by table name) is resilient to
    # upstream renames and to the presence of auxiliary tables that
    # happen to precede the LeagueGameFinderResults table.
    target_table: Optional[Dict[str, Any]] = None
    for entry in result_sets:
        if not isinstance(entry, dict):
            continue
        entry_headers = entry.get("headers") or []
        if "GAME_ID" in entry_headers:
            target_table = entry
            break

    if target_table is None:
        logger.warning(
            "endpoints.schedule.enumerate_game_ids no GAME_ID column season=%s",
            season,
        )
        return []

    headers: List[str] = list(target_table.get("headers") or [])
    rows: List[List[Any]] = list(target_table.get("rowSet") or [])
    game_id_index = headers.index("GAME_ID")

    # Order-preserving deduplication. A dict is used (rather than a set)
    # because CPython 3.7+ guarantees insertion-ordered iteration over
    # dict keys, which is the determinism guarantee Gate 8 resume
    # semantics rely on. Each GAME_ID value is coerced via str() because
    # the upstream occasionally returns numeric types for IDs that look
    # numeric; downstream box-score calls expect the string form.
    seen: Dict[str, None] = {}
    for row in rows:
        if not row or game_id_index >= len(row):
            continue
        game_id = row[game_id_index]
        if game_id is None:
            continue
        key = str(game_id)
        # Normalize to the 10-character zero-padded canonical form this
        # function's docstring promises. The upstream can send the same
        # game as int 22500001 and as str "0022500001"; without this the
        # dedupe below would count them as two games and the Games
        # pipeline would fetch it twice. Mirrors the .str.zfill(10)
        # normalization in pipelines/ingest_games.py.
        key = key.zfill(10)
        if key not in seen:
            seen[key] = None
    ordered_ids: List[str] = list(seen.keys())

    logger.info(
        "endpoints.schedule.enumerate_game_ids season=%s game_count=%d",
        season,
        len(ordered_ids),
    )
    return ordered_ids
