from experiments.simulator.splits import chronological_splits, sequence_splits


def test_sequence_splits_are_contiguous_and_exclusive():
    train, validation, test = sequence_splits(1, 61, 81)
    assert (train.sequence_start, train.sequence_end) == (1, 61)
    assert (validation.sequence_start, validation.sequence_end) == (61, 81)
    assert (test.sequence_start, test.sequence_end) == (81, None)


def test_sequence_splits_require_nonempty_periods():
    try:
        sequence_splits(1, 1, 81)
    except ValueError:
        pass
    else:
        raise AssertionError("empty train split was accepted")


def test_chronological_splits_use_elapsed_time_and_capture_order(tmp_path):
    import polars as pl

    (tmp_path / "dataset-metadata.json").write_text(
        '{"source_capture_ids":["fixture"],"source_capture_statuses":["complete"]}'
    )
    books = tmp_path / "event_type=order_book_events"
    trades = tmp_path / "event_type=market_trades"
    books.mkdir()
    trades.mkdir()
    pl.DataFrame({"capture_sequence": [1, 3, 5], "local_receive_time": [0, 60, 80]}).write_parquet(books / "part.parquet")
    pl.DataFrame({"capture_sequence": [2, 4, 6], "local_receive_time": [20, 70, 100]}).write_parquet(trades / "part.parquet")

    assert chronological_splits(tmp_path) == sequence_splits(1, 3, 5)
