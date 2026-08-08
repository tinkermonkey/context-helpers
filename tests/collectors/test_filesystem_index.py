"""Unit tests for the filesystem FileIndex (SQLite change-sequence index)."""


import pytest

from context_helpers.collectors.filesystem.index import FileIndex


@pytest.fixture
def index(tmp_path) -> FileIndex:
    idx = FileIndex(tmp_path / "idx.db", failure_threshold=3)
    yield idx
    idx.close()


def _add(index, source_id, *, size=10, mtime=1000.0, is_text=1, gen=1):
    return index.index_file(
        source_id=source_id, abspath=f"/abs/{source_id}", root_label="r",
        rel_path=source_id, size=size, mtime=mtime, is_text=is_text, gen=gen,
    )


class TestSequence:
    def test_new_files_get_increasing_seq(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        _add(index, "b", gen=gen)
        cands = index.fetch_candidates(0, 10)
        assert [c.source_id for c in cands] == ["a", "b"]
        assert cands[0].seq < cands[1].seq

    def test_max_seq_tracks_counter(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        _add(index, "b", gen=gen)
        assert index.max_seq() == 2

    def test_unchanged_file_keeps_seq(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        seq1 = index.lookup("a").seq
        gen2 = index.begin_scan()
        changed = _add(index, "a", gen=gen2)  # identical size/mtime
        assert changed is False
        assert index.lookup("a").seq == seq1

    def test_changed_file_bumps_seq(self, index):
        gen = index.begin_scan()
        _add(index, "a", mtime=1000.0, gen=gen)
        seq1 = index.lookup("a").seq
        gen2 = index.begin_scan()
        changed = _add(index, "a", mtime=2000.0, gen=gen2)
        assert changed is True
        assert index.lookup("a").seq > seq1


class TestDeletion:
    def test_missing_file_tombstoned(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        _add(index, "b", gen=gen)
        gen2 = index.begin_scan()
        _add(index, "a", gen=gen2)  # b not seen this scan
        deleted = index.finalize_scan(gen2)
        assert deleted == 1
        b = index.lookup("b")
        assert b.state == "deleted"

    def test_tombstone_delivered_then_pruned(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        gen2 = index.begin_scan()
        index.finalize_scan(gen2)  # a missing → tombstoned
        tomb = index.lookup("a")
        assert tomb.state == "deleted"
        index.prune_deleted(tomb.seq)
        assert index.lookup("a") is None

    def test_prune_respects_seq_boundary(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        gen2 = index.begin_scan()
        index.finalize_scan(gen2)
        tomb_seq = index.lookup("a").seq
        index.prune_deleted(tomb_seq - 1)  # below the tombstone seq → not pruned
        assert index.lookup("a") is not None

    def test_resurrected_file_marked_present(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        gen2 = index.begin_scan()
        index.finalize_scan(gen2)
        assert index.lookup("a").state == "deleted"
        gen3 = index.begin_scan()
        _add(index, "a", gen=gen3)  # file came back
        assert index.lookup("a").state == "present"


class TestFailures:
    def test_transient_failure_requeues(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        old_seq = index.lookup("a").seq
        retryable = index.record_transient_failure("a", "locked")
        assert retryable is True
        assert index.lookup("a").seq > old_seq  # moved to the back of the queue

    def test_transient_failure_gives_up_at_threshold(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        assert index.record_transient_failure("a", "x") is True   # 1
        assert index.record_transient_failure("a", "x") is True   # 2
        assert index.record_transient_failure("a", "x") is False  # 3 == threshold → give up
        assert index.lookup("a").is_text == 0

    def test_mark_binary_then_excluded_from_text(self, index):
        gen = index.begin_scan()
        _add(index, "a", is_text=None, gen=gen)
        index.mark_binary("a", "NUL")
        assert index.lookup("a").is_text == 0

    def test_mark_text_stores_hash_and_clears_failures(self, index):
        gen = index.begin_scan()
        _add(index, "a", is_text=None, gen=gen)
        index.record_transient_failure("a", "x")
        index.mark_text("a", "f" * 64)
        row = index.lookup("a")
        assert row.is_text == 1
        assert row.content_hash == "f" * 64
        assert row.failures == 0


class TestHasChanges:
    def test_has_changes_after_cursor(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        assert index.has_changes(0) is True
        assert index.has_changes(index.max_seq()) is False

    def test_reset_clears_everything(self, index):
        gen = index.begin_scan()
        _add(index, "a", gen=gen)
        index.reset()
        assert index.max_seq() == 0
        assert index.fetch_candidates(0, 10) == []
