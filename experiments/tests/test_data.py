import polars as pl

from experiments.simulator.data import load_events


def write(root, table, venue, frame):
    directory = root / "version=3" / "date=1970-01-01" / f"event_type={table}" / f"venue={venue}" / "market=BTC"
    directory.mkdir(parents=True)
    frame.write_parquet(directory / "part.parquet")


def test_loader_preserves_availability_stream_in_capture_order(tmp_path):
    write(tmp_path, "order_book_events", "aster", pl.DataFrame({
        "capture_sequence": [1], "local_receive_time": [10],
    }))
    write(tmp_path, "order_book_levels", "aster", pl.DataFrame({
        "capture_sequence": [1, 1], "side": ["bid", "ask"], "position": [0, 0],
        "price": [99.0, 100.0], "quantity": [1.0, 2.0],
    }))
    write(tmp_path, "availability_events", "aster", pl.DataFrame({
        "capture_sequence": [2, 3], "stream": ["trade_stream", "order_book"],
        "transition": ["unavailable", "unavailable"], "observed_at": [20, 30],
    }))

    events = load_events(tmp_path)

    assert [event.sequence for event in events] == [1, 2, 3]
    assert events[0].book.best_bid.price == 99.0
    assert [event.availability_stream for event in events[1:]] == ["trade_stream", "order_book"]
    assert [event.invalidates_book for event in events] == [False, False, True]
