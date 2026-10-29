from datetime import datetime, timezone

import pytest

from aster import lake


def manifest(timestamp, identifier="id"):
    return {"cursor": {"end": [timestamp, identifier]}}


def test_cursor_end_normalizes_offsets_to_utc():
    utc_instant = datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert lake.cursor_end(manifest("2024-06-01T03:00:00+03:00"))[0] == utc_instant
    assert lake.cursor_end(manifest("2024-05-31T20:00:00-04:00"))[0] == utc_instant
    assert lake.cursor_end(manifest("2024-06-01T00:00:00+00:00", "b"))[1] == "b"


def test_cursor_ordering_compares_instants_not_string_offsets():
    earlier = manifest("2024-06-01T03:00:00+03:00", "a")
    later = manifest("2024-06-01T01:00:00+00:00", "b")
    assert earlier["cursor"]["end"][0] > later["cursor"]["end"][0]
    assert lake.cursor_end(earlier) < lake.cursor_end(later)
    assert [
        item["cursor"]["end"][1]
        for item in sorted([later, earlier], key=lake.cursor_end)
    ] == ["a", "b"]


def test_cursor_ties_break_on_identifier_across_offsets():
    left = manifest("2024-06-01T03:00:00+03:00", "a")
    right = manifest("2024-06-01T00:00:00+00:00", "b")
    assert lake.cursor_end(left) < lake.cursor_end(right)


def test_cursor_end_rejects_naive_timestamps():
    with pytest.raises(ValueError, match="no UTC offset"):
        lake.cursor_end(manifest("2024-06-01T00:00:00"))
    with pytest.raises(ValueError, match="no UTC offset"):
        lake.cursor_end(manifest("2024-06-01 00:00:00"))


def test_cursor_end_rejects_timestamps_that_are_not_instants():
    with pytest.raises(ValueError):
        lake.cursor_end(manifest("2024-06-01"))
