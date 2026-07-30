"""Unit tests for `utils.checkpoint`.

Verifies the CheckpointManager enforcing Rule 5 — Checkpoint After Every
Pull. Covers load/persist semantics, atomicity under OSError, malformed
JSON recovery, type/empty-string guards, reset scoping, snapshot
immutability, thread safety, and ISO-8601 UTC timestamp format.

All tests are filesystem-isolated via tmp_path or the tmp_output_dir
fixture; no real output/checkpoint.json is touched.
"""

from __future__ import annotations

import json
import re  # noqa: F401  # Canonical import template; retained for sibling-test consistency.
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List  # noqa: F401  # Dict used in type hints resolved via __future__ annotations.

import pytest

import config
from utils.checkpoint import CheckpointManager


# ---------------------------------------------------------------------------
# Phase 2.1 — Fresh state (no file)
# ---------------------------------------------------------------------------


def test_fresh_state_is_completed_returns_false(tmp_path: Path) -> None:
    """A fresh CheckpointManager pointed at a non-existent path reports
    no keys completed."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    assert cp.is_completed("games", "0022500001") is False


def test_fresh_state_snapshot_is_empty_dict(tmp_path: Path) -> None:
    """A fresh manager's snapshot is an empty dict, not None or missing keys."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    assert cp.snapshot() == {}


def test_fresh_state_file_not_created_until_mark(tmp_path: Path) -> None:
    """Construction and is_completed() are read-only — they must not
    prematurely create the checkpoint file on disk."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    # Merely constructing does not persist anything
    assert not path.exists()
    assert cp.is_completed("games", "0022500001") is False
    # is_completed must not create the file either
    assert not path.exists()


# ---------------------------------------------------------------------------
# Phase 2.2 — Default path reads `config.CHECKPOINT_PATH`
# ---------------------------------------------------------------------------


def test_default_path_reads_config_checkpoint_path(tmp_output_dir: Path) -> None:
    """CheckpointManager() with no path argument must resolve the default
    path from config.CHECKPOINT_PATH. The tmp_output_dir fixture
    monkeypatches that attribute to a tmp-scoped location."""
    # tmp_output_dir fixture redirects config.CHECKPOINT_PATH to tmp_path
    cp = CheckpointManager()
    # Mark something so we can verify the file lands at the configured path
    cp.mark_completed("games", "probe")
    assert Path(config.CHECKPOINT_PATH).exists()


# ---------------------------------------------------------------------------
# Phase 2.3 — Rule 5: mark → persist → reload round-trip
# ---------------------------------------------------------------------------


def test_round_trip_mark_completed_persists_and_new_instance_sees_completion(tmp_path: Path) -> None:
    """The core Rule 5 contract: a mark_completed call made by one
    CheckpointManager instance must be visible to a freshly-constructed
    instance loading the same path. This is the exact behavior a pipeline
    relies on to resume after a crash."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    cp.mark_completed(config.DOMAIN_GAMES, "0022500001")

    # Physical artifact exists
    assert path.exists(), f"Expected checkpoint at {path}"

    # A fresh instance loading the SAME path reports the key as completed
    cp2 = CheckpointManager(path=path)
    assert cp2.is_completed(config.DOMAIN_GAMES, "0022500001") is True


def test_round_trip_preserves_multiple_domains(tmp_path: Path) -> None:
    """Every canonical domain constant from config must round-trip through
    the checkpoint file independently."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    cp.mark_completed(config.DOMAIN_GAMES, "0022500001")
    cp.mark_completed(config.DOMAIN_PLAYERS, "leaguedashplayerstats:2025-26")
    cp.mark_completed(config.DOMAIN_TEAMS, "leaguedashteamstats:2025-26")

    cp2 = CheckpointManager(path=path)
    assert cp2.is_completed(config.DOMAIN_GAMES, "0022500001") is True
    assert cp2.is_completed(config.DOMAIN_PLAYERS, "leaguedashplayerstats:2025-26") is True
    assert cp2.is_completed(config.DOMAIN_TEAMS, "leaguedashteamstats:2025-26") is True


def test_round_trip_unrelated_keys_not_marked(tmp_path: Path) -> None:
    """Marking one key must not cause adjacent keys to appear completed.
    Different domains holding the same key string are independent."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    cp.mark_completed("games", "present")
    cp2 = CheckpointManager(path=path)
    assert cp2.is_completed("games", "absent") is False
    assert cp2.is_completed("players", "present") is False  # different domain


# ---------------------------------------------------------------------------
# Phase 2.4 — Timestamp format (ISO-8601 UTC)
# ---------------------------------------------------------------------------


def test_mark_completed_stores_iso_8601_utc_timestamp(tmp_path: Path) -> None:
    """The value associated with every (domain, key) must be a string
    containing an ISO-8601 timestamp in UTC with seconds precision. This
    locks in the timespec='seconds' contract — changing to microseconds
    would alter operator tooling expectations."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    cp.mark_completed("games", "0022500001")
    state = cp.snapshot()
    ts = state["games"]["0022500001"]

    # Must be a string
    assert isinstance(ts, str)
    # Parseable as ISO-8601 with timezone
    parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None
    assert parsed.utcoffset().total_seconds() == 0.0
    # Timespec seconds → no fractional seconds
    assert "." not in ts


# ---------------------------------------------------------------------------
# Phase 2.5 — On-disk JSON format (indent=2, sort_keys=True)
# ---------------------------------------------------------------------------


def test_on_disk_json_is_indented_and_sorted(tmp_path: Path) -> None:
    """The on-disk manifest must be pretty-printed (indent=2) with
    deterministic key ordering (sort_keys=True). This makes diffs stable
    across runs and operator-friendly."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    cp.mark_completed("zulu", "1")
    cp.mark_completed("alpha", "1")

    raw = path.read_text(encoding="utf-8")
    # indent=2 → there must be "  " indentation
    assert "  " in raw
    # sort_keys=True → alpha appears before zulu
    assert raw.index('"alpha"') < raw.index('"zulu"')
    # JSON parses successfully
    parsed = json.loads(raw)
    assert "alpha" in parsed
    assert "zulu" in parsed


# ---------------------------------------------------------------------------
# Phase 2.6 — `get_pending` preserves order
# ---------------------------------------------------------------------------


def test_get_pending_preserves_order_of_input_keys(tmp_path: Path) -> None:
    """get_pending must return the non-completed keys in the same order
    they were supplied — pipelines rely on this to preserve upstream
    ordering (e.g., GAME_ID chronology)."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    cp.mark_completed("games", "b")
    pending = cp.get_pending("games", ["a", "b", "c", "d"])
    assert pending == ["a", "c", "d"]


def test_get_pending_all_keys_complete_returns_empty(tmp_path: Path) -> None:
    """When every input key has been completed, get_pending returns an
    empty list — a signal to the caller that no further work is needed."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    for k in ("a", "b", "c"):
        cp.mark_completed("games", k)
    assert cp.get_pending("games", ["a", "b", "c"]) == []


def test_get_pending_no_keys_complete_returns_all(tmp_path: Path) -> None:
    """When nothing has been completed, get_pending returns every input
    key (in order), making the first-run case trivially correct."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    assert cp.get_pending("games", ["a", "b", "c"]) == ["a", "b", "c"]


def test_get_pending_accepts_iterable_generator(tmp_path: Path) -> None:
    """get_pending must accept any Iterable — not just a list. Pipelines
    may pass generator expressions from upstream enumeration."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    cp.mark_completed("games", "b")
    # Pass a generator (exhausted on iteration)
    pending = cp.get_pending("games", (k for k in ["a", "b", "c"]))
    assert pending == ["a", "c"]


def test_get_pending_returns_list_not_generator(tmp_path: Path) -> None:
    """The return type must be a concrete list — callers should be able
    to len() and index into the result without surprises."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    pending = cp.get_pending("games", ["a"])
    assert isinstance(pending, list)


# ---------------------------------------------------------------------------
# Phase 2.7 — Atomicity under OSError during persist
# ---------------------------------------------------------------------------


def test_mark_completed_rolls_back_in_memory_state_on_persist_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If _persist fails (e.g., the atomic rename raises OSError),
    mark_completed MUST roll back its in-memory state change AND re-raise.
    This is the Rule 5 crash-safety invariant: the in-memory state must
    always match the on-disk state."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    # Pre-seed with a known-good write
    cp.mark_completed("games", "stable")
    assert path.exists()
    original_content = path.read_text(encoding="utf-8")

    # Inject OSError into Path.replace (the atomic rename step)
    def _boom(self: Path, target: Path) -> Path:  # signature: Path.replace(self, target)
        raise OSError("simulated replace failure")

    monkeypatch.setattr(Path, "replace", _boom, raising=True)

    with pytest.raises(OSError):
        cp.mark_completed("games", "new-key-that-should-roll-back")

    # On-disk file must be unchanged
    assert path.read_text(encoding="utf-8") == original_content
    # In-memory state must NOT contain the rolled-back key
    assert cp.is_completed("games", "new-key-that-should-roll-back") is False
    # Prior key remains
    assert cp.is_completed("games", "stable") is True


def test_persist_writes_to_tmp_suffix_first_then_replaces(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Capture the sequence of write_text + replace calls to verify
    the two-phase atomic write pattern: write to a `.tmp` sibling file,
    then atomically rename into place. This guards against torn-write
    corruption if the process crashes mid-write."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)

    write_text_calls: List[Path] = []
    original_write_text = Path.write_text

    def _tracking_write_text(self: Path, data: str, *args, **kwargs):
        write_text_calls.append(Path(self))
        return original_write_text(self, data, *args, **kwargs)

    replace_targets: List[Path] = []
    original_replace = Path.replace

    def _tracking_replace(self: Path, target: Path) -> Path:
        replace_targets.append(Path(target))
        return original_replace(self, target)

    monkeypatch.setattr(Path, "write_text", _tracking_write_text, raising=True)
    monkeypatch.setattr(Path, "replace", _tracking_replace, raising=True)

    cp.mark_completed("games", "0022500001")

    # At least one write_text call landed on a path with a ".tmp" suffix
    assert any(p.name.endswith(".tmp") or ".tmp" in p.name for p in write_text_calls), (
        f"Expected write_text to a .tmp path; saw {write_text_calls}"
    )
    # Replace target was the final path
    assert any(Path(t).resolve() == path.resolve() for t in replace_targets)


# ---------------------------------------------------------------------------
# Phase 2.8 — Malformed JSON recovery
# ---------------------------------------------------------------------------


def test_load_with_malformed_json_returns_empty_state_and_does_not_raise(tmp_path: Path) -> None:
    """If an operator hand-edited the checkpoint and introduced a JSON
    typo, the manager MUST start with empty state rather than crashing.
    The alternative (raising) would cascade into unrecoverable pipeline
    startup failures."""
    path = tmp_path / "cp.json"
    path.write_text("{not valid json", encoding="utf-8")
    # Must NOT raise
    cp = CheckpointManager(path=path)
    assert cp.snapshot() == {}
    # Subsequent mark works normally
    cp.mark_completed("games", "0022500001")
    assert cp.is_completed("games", "0022500001") is True


def test_load_with_non_dict_top_level_returns_empty_state(tmp_path: Path) -> None:
    """A syntactically valid JSON list at the top level is semantically
    wrong for the checkpoint format; the manager must recover with
    empty state rather than attempting to normalize a list."""
    path = tmp_path / "cp.json"
    path.write_text(json.dumps(["games", "players"]), encoding="utf-8")
    cp = CheckpointManager(path=path)
    assert cp.snapshot() == {}


def test_load_with_top_level_string_returns_empty_state(tmp_path: Path) -> None:
    """A top-level JSON string is the other common shape of malformed
    input that must trigger graceful recovery."""
    path = tmp_path / "cp.json"
    path.write_text('"stringy"', encoding="utf-8")
    cp = CheckpointManager(path=path)
    assert cp.snapshot() == {}


def test_load_with_empty_file_returns_empty_state(tmp_path: Path) -> None:
    """An empty file (zero bytes) is indistinguishable in practice from a
    crashed write that left a stub — treat it as empty state, not an
    error."""
    path = tmp_path / "cp.json"
    path.write_text("", encoding="utf-8")
    cp = CheckpointManager(path=path)
    assert cp.snapshot() == {}


# ---------------------------------------------------------------------------
# Phase 2.9 — Type enforcement
# ---------------------------------------------------------------------------


def test_is_completed_rejects_non_str_domain_with_type_error(tmp_path: Path) -> None:
    """is_completed validates its domain argument type — passing a
    non-string must raise TypeError, not silently coerce."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    with pytest.raises(TypeError):
        cp.is_completed(123, "key")


def test_is_completed_rejects_non_str_key_with_type_error(tmp_path: Path) -> None:
    """is_completed validates its key argument type — passing a non-string
    (int) must raise TypeError."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    with pytest.raises(TypeError):
        cp.is_completed("games", 42)


def test_mark_completed_rejects_non_str_domain_with_type_error(tmp_path: Path) -> None:
    """mark_completed validates its domain argument type; a non-string
    input is a programming error, not a data error — raise TypeError."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    with pytest.raises(TypeError):
        cp.mark_completed(123, "key")


def test_mark_completed_rejects_non_str_key_with_type_error(tmp_path: Path) -> None:
    """mark_completed validates its key argument type; None is a common
    accidental input (missed argument) and must raise TypeError."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    with pytest.raises(TypeError):
        cp.mark_completed("games", None)


# ---------------------------------------------------------------------------
# Phase 2.10 — Empty-string rejection
# ---------------------------------------------------------------------------


def test_mark_completed_rejects_empty_domain_with_value_error(tmp_path: Path) -> None:
    """An empty-string domain would produce an empty-string-keyed top-level
    entry that is_completed could never find — defense-in-depth rejection
    at the mark site prevents silent data loss."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    with pytest.raises(ValueError):
        cp.mark_completed("", "key")


def test_mark_completed_rejects_empty_key_with_value_error(tmp_path: Path) -> None:
    """An empty-string key is equally invalid and must be rejected with
    ValueError rather than persisted."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    with pytest.raises(ValueError):
        cp.mark_completed("games", "")


# ---------------------------------------------------------------------------
# Phase 2.11 — `reset()` scoping
# ---------------------------------------------------------------------------


def test_reset_single_domain_clears_only_that_domain(tmp_path: Path) -> None:
    """reset(domain) must clear exactly that domain's entries and leave
    every other domain untouched. The reset must also be persisted so a
    fresh instance loading the same path observes the post-reset state."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    cp.mark_completed("games", "g1")
    cp.mark_completed("games", "g2")
    cp.mark_completed("players", "p1")

    cp.reset("games")

    assert cp.is_completed("games", "g1") is False
    assert cp.is_completed("games", "g2") is False
    assert cp.is_completed("players", "p1") is True

    # Persistence reflects the reset
    cp2 = CheckpointManager(path=path)
    assert cp2.is_completed("games", "g1") is False
    assert cp2.is_completed("players", "p1") is True


def test_reset_all_domains_clears_everything(tmp_path: Path) -> None:
    """reset() with no argument clears every domain. The post-reset
    snapshot must be an empty dict, and the on-disk file must reflect
    the cleared state."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    cp.mark_completed("games", "g1")
    cp.mark_completed("players", "p1")

    cp.reset()

    assert cp.snapshot() == {}
    cp2 = CheckpointManager(path=path)
    assert cp2.snapshot() == {}


def test_reset_unknown_domain_is_no_op(tmp_path: Path) -> None:
    """reset(unknown_domain) must be a silent no-op. The contract is
    idempotent — callers should not need to check whether a domain
    exists before resetting it."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    cp.mark_completed("games", "g1")
    # Resetting a domain never marked does not raise
    cp.reset("nonexistent-domain")
    assert cp.is_completed("games", "g1") is True


# ---------------------------------------------------------------------------
# Phase 2.12 — `snapshot()` returns deep copy
# ---------------------------------------------------------------------------


def test_snapshot_returns_deep_copy_not_live_reference(tmp_path: Path) -> None:
    """snapshot() must return a fresh outer dict AND fresh inner dicts so
    the caller cannot accidentally corrupt the manager's internal state
    by mutating the returned object. A shallow copy would share inner
    dicts; a live reference would be disastrous."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    cp.mark_completed("games", "g1")
    snap1 = cp.snapshot()

    # Mutate the snapshot — must not affect internal state
    snap1["games"]["g1"] = "MUTATED"
    snap1["games"]["bogus_key"] = "INJECTED"
    snap1["new_domain"] = {"x": "y"}

    snap2 = cp.snapshot()
    # Manager returns a fresh copy that reflects real state, not the mutation
    assert snap2.get("new_domain") is None
    assert "bogus_key" not in snap2.get("games", {})
    assert snap2["games"]["g1"] != "MUTATED"


# ---------------------------------------------------------------------------
# Phase 2.13 — Thread safety
# ---------------------------------------------------------------------------


def test_concurrent_mark_completed_different_keys_preserves_all(tmp_path: Path) -> None:
    """5 threads × 10 distinct keys each = 50 total marks. All must be
    durably persisted. The production RLock serializes writes so there
    is no lost update — verified by reloading with a fresh instance and
    confirming every key is present."""
    path = tmp_path / "cp.json"
    cp = CheckpointManager(path=path)
    errors: List[BaseException] = []

    def _worker(thread_index: int) -> None:
        try:
            for i in range(10):
                cp.mark_completed("games", f"t{thread_index}-k{i}")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert all(not t.is_alive() for t in threads)
    assert errors == []

    # All 50 (5 threads × 10 keys) keys should be present and persisted
    cp2 = CheckpointManager(path=path)
    for thread_index in range(5):
        for i in range(10):
            key = f"t{thread_index}-k{i}"
            assert cp2.is_completed("games", key) is True, f"Missing {key}"


def test_concurrent_mark_and_read_is_consistent(tmp_path: Path) -> None:
    """A reader thread calling is_completed and snapshot in a tight loop
    while a writer thread performs 50 mark_completed calls must never
    observe torn state or raise exceptions. The RLock guards both sides
    of the read/write boundary."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    stop = threading.Event()

    def _writer() -> None:
        for i in range(50):
            cp.mark_completed("games", f"k{i}")

    def _reader() -> None:
        while not stop.is_set():
            cp.is_completed("games", "k0")
            cp.snapshot()

    writer = threading.Thread(target=_writer)
    reader = threading.Thread(target=_reader)
    writer.start()
    reader.start()
    writer.join(timeout=10.0)
    stop.set()
    reader.join(timeout=2.0)

    assert not writer.is_alive()
    assert not reader.is_alive()


# ---------------------------------------------------------------------------
# Phase 2.14 — Implicit F-007 — Rule 5 aliases for pytest -k matching
# ---------------------------------------------------------------------------


def test_rule5_checkpoint_round_trip_alias(tmp_path: Path) -> None:
    """Named so that pytest -k 'rule5' matches this file. Acts as a
    sentinel test that a operator can use to quickly verify Rule 5
    compliance without running the full suite."""
    cp = CheckpointManager(path=tmp_path / "cp.json")
    cp.mark_completed("games", "0022500001")
    cp2 = CheckpointManager(path=tmp_path / "cp.json")
    assert cp2.is_completed("games", "0022500001") is True


# ---------------------------------------------------------------------------
# Phase 2.15 — `get_pending` duplicate / element-type / order guarantees
# ---------------------------------------------------------------------------
#
# The four tests below pin the three semantics of
# :meth:`utils.checkpoint.CheckpointManager.get_pending` that the five
# pre-existing ``get_pending`` tests in Phase 2.6 above leave unasserted.
# Every expected value is derived BY HAND from the two statements that
# constitute the method's body in ``utils/checkpoint.py`` (lines 542-548)::
#
#     with self._lock:
#         completed = set(self._state.get(domain, {}).keys())
#     return [k for k in all_keys if str(k) not in completed]
#
# Three facts follow directly from those two statements, and each is
# pinned by the tests that follow:
#   1. The ``set()`` is applied to the COMPLETED keys, never to
#      ``all_keys`` — so duplicates in the input survive verbatim.
#   2. ``str(k)`` is evaluated ONLY inside the membership test and its
#      result is then discarded — so the returned list holds the
#      ORIGINAL element objects with their original types, exactly as
#      the method docstring (lines 516-520) promises: "the returned
#      list contains the original elements verbatim".
#   3. The predicate is evaluated independently for EVERY element — so
#      marking one key filters every duplicate of it, not just the
#      first occurrence.
#
# The fault these four tests exist to catch is one plausible mutation:
# wrapping ``all_keys`` in ``set()`` "for efficiency". That would
# silently drop legitimate duplicate keys AND silently reorder the fetch
# sequence, destroying the chronological ``GAME_ID`` replay determinism
# the pipelines depend on. The sibling ``[str(k) for k in all_keys ...]``
# mutation would additionally break the element-type guarantee.
#
# Each test below is paired with exactly one mutant that it detects
# DETERMINISTICALLY:
#   * ``set(all_keys)``            -> the duplicate-input test, because
#     the three-element input collapses to two elements regardless of
#     the set's iteration order, so the length assertion always fires.
#   * remove-first-occurrence-only -> the every-duplicate test.
#   * ``[str(k) for k in ...]``    -> the element-type test.
#   * ``sorted(...)``              -> the input-order test.
#
# None of those four mutants is reliably caught by the pre-existing
# Phase 2.6 tests: ``sorted(...)``, ``[str(k) ...]`` and
# remove-first-occurrence leave all five of them green, and ``set()`` is
# caught by the Phase 2.6 order test only incidentally — only when
# CPython's per-process string-hash randomization happens to produce a
# set ordering that differs from the input ordering, which is not a
# dependable signal.
# ---------------------------------------------------------------------------


def test_get_pending_does_not_deduplicate_duplicate_input_keys(tmp_path: Path) -> None:
    """``get_pending`` must NOT deduplicate its own ``all_keys`` input.

    Hand derivation — nothing is marked completed, so ``completed`` is
    the empty set; the comprehension at ``utils/checkpoint.py`` line 548
    then evaluates ``str(k) not in completed`` independently for each of
    the three elements of ``["a", "a", "b"]`` and every one passes, so
    all three survive in input order — including the repeated ``"a"``.

    Deterministically detects the ``set(all_keys)`` /
    ``dict.fromkeys(all_keys)`` "efficiency" mutation: either one
    collapses the three-element input to two elements, so the length
    assertion below fires no matter which order the collapsed container
    happens to iterate in.
    """
    # Arrange — a fresh manager with nothing marked completed.
    cp = CheckpointManager(path=tmp_path / "cp.json")

    # Act — supply the same key twice.
    pending = cp.get_pending("games", ["a", "a", "b"])

    # Assert — the whole ordered list, duplicate included.
    assert pending == ["a", "a", "b"], (
        "get_pending must not deduplicate its own input (utils/checkpoint.py L548 is a plain "
        f"order-preserving comprehension); expected ['a', 'a', 'b'] but got {pending!r}"
    )
    assert len(pending) == 3, (
        "get_pending must return all 3 supplied keys when none is completed; got "
        f"{len(pending)} element(s): {pending!r}"
    )


def test_get_pending_filters_every_duplicate_of_a_completed_key(tmp_path: Path) -> None:
    """A completed key filters EVERY duplicate of itself, not just the first.

    Hand derivation — ``mark_completed("games", "a")`` makes
    ``completed == {"a"}``; the comprehension at ``utils/checkpoint.py``
    line 548 then evaluates the predicate independently for each element
    of ``["a", "a", "b"]``, so BOTH ``"a"`` entries are dropped and only
    ``"b"`` survives.

    Deterministically detects a ``list.remove``-style
    first-occurrence-only filter, or any early-exit mutation: either
    would leave the second ``"a"`` behind and return ``["a", "b"]``.
    This test deliberately does NOT claim to catch the ``set(all_keys)``
    mutation — collapsing the input first and then filtering yields the
    same ``["b"]`` here, which is precisely why the duplicate-input test
    above exists as the dedicated detector for that mutant.
    """
    # Arrange — mark the duplicated key completed.
    cp = CheckpointManager(path=tmp_path / "cp.json")
    cp.mark_completed("games", "a")

    # Act — the completed key appears twice in the input.
    pending = cp.get_pending("games", ["a", "a", "b"])

    # Assert — the whole ordered list; both copies of "a" are gone.
    assert pending == ["b"], (
        "get_pending must filter every duplicate of a completed key, not only its first "
        f"occurrence; expected ['b'] but got {pending!r}"
    )
    assert len(pending) == 1, (
        "marking 'a' completed must remove BOTH 'a' entries from ['a', 'a', 'b']; got "
        f"{len(pending)} element(s): {pending!r}"
    )


def test_get_pending_returns_original_element_objects_not_string_coercions(tmp_path: Path) -> None:
    """``get_pending`` returns the original element objects, not ``str`` copies.

    Hand derivation — nothing is marked completed, so ``completed`` is
    empty and every element passes the predicate. The comprehension at
    ``utils/checkpoint.py`` line 548 emits ``k`` itself; ``str(k)`` is
    evaluated only inside the membership test and its result is
    discarded. So ``[1, 2]`` comes back as ``[1, 2]`` — genuine ``int``
    objects — exactly as the method docstring (lines 516-520) promises:
    "the returned list contains the original elements verbatim".

    Both assertions are required and neither alone suffices. Equality
    alone catches the blunt ``[str(k) for k in all_keys ...]`` mutation
    (because ``["1", "2"] != [1, 2]``), but Python's numeric tower makes
    ``[True, 2] == [1, 2]`` evaluate ``True`` and ``isinstance(True,
    int)`` is ``True`` as well — only ``type(k) is int`` rules out
    ``bool`` and NumPy-integer look-alikes.
    """
    # Arrange — a fresh manager with nothing marked completed.
    cp = CheckpointManager(path=tmp_path / "cp.json")

    # Act — non-string keys; get_pending coerces only for the lookup.
    pending = cp.get_pending("games", [1, 2])

    # Assert — the same values ...
    assert pending == [1, 2], (
        "get_pending must return the original elements, not their str() forms "
        f"(utils/checkpoint.py L548 emits k, never str(k)); expected [1, 2] but got {pending!r}"
    )
    # ... and the same concrete types (type identity, deliberately not isinstance).
    assert all(type(k) is int for k in pending), (
        "get_pending must preserve each element's exact type; expected every element to be a "
        f"genuine int but got types {[type(k).__name__ for k in pending]!r} for {pending!r}"
    )


def test_get_pending_preserves_unsorted_input_order_with_middle_key_completed(tmp_path: Path) -> None:
    """Surviving keys keep INPUT order, which here is deliberately not sorted order.

    Hand derivation — ``mark_completed("games", "0022500002")`` makes
    ``completed == {"0022500002"}``; the comprehension at
    ``utils/checkpoint.py`` line 548 walks
    ``["0022500003", "0022500002", "0022500001", "0022500004"]`` left to
    right and emits every element whose ``str`` form is absent from
    ``completed``, so the three survivors appear in input order:
    ``["0022500003", "0022500001", "0022500004"]``.

    Deterministically detects a ``sorted(...)`` mutation, which would
    instead yield ``["0022500001", "0022500003", "0022500004"]``. It also
    catches the ``set(all_keys)`` mutation whenever that mutant's
    arbitrary iteration order differs from the input order, though the
    duplicate-input test above is the deterministic detector for that one.

    The pre-existing Phase 2.6 order test supplies an already-sorted
    ``["a", "b", "c", "d"]``, so its expected ``["a", "c", "d"]`` is
    indistinguishable from a sorted result; the unsorted input used here
    is what makes the order guarantee genuinely falsifiable, and it is
    what protects the pipelines' chronological ``GAME_ID`` fetch order.
    """
    # Arrange — mark ONE key sitting in the middle of the input sequence.
    cp = CheckpointManager(path=tmp_path / "cp.json")
    cp.mark_completed("games", "0022500002")

    # Act — canonical 10-character GAME_IDs supplied out of sorted order.
    pending = cp.get_pending(
        "games",
        ["0022500003", "0022500002", "0022500001", "0022500004"],
    )

    # Assert — the whole ordered list, which is NOT the sorted list.
    assert pending == ["0022500003", "0022500001", "0022500004"], (
        "get_pending must preserve input order (utils/checkpoint.py L503-506); expected "
        f"['0022500003', '0022500001', '0022500004'] but got {pending!r}"
    )
    assert len(pending) == 3, (
        "exactly one of the four supplied GAME_IDs was completed, so 3 must remain pending; got "
        f"{len(pending)} element(s): {pending!r}"
    )
