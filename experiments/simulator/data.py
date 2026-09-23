from collections import defaultdict
from pathlib import Path

import polars as pl

from .models import Level, MarketEvent, MarketTrade, OrderBook


def _files(root: Path, event_type: str) -> list[str]:
    return [str(path) for path in root.glob(f"**/event_type={event_type}/**/*.parquet")]


def load_events(dataset: str | Path, max_depth: int = 10) -> list[MarketEvent]:
    """Load a bounded top-N L2 event stream.

    Parquet DECIMAL values are converted to float only at this research adapter
    boundary. Canonical Rust/replay data remains exact and unchanged.
    """
    root = Path(dataset)
    level_files = _files(root, "order_book_levels")
    book_files = _files(root, "order_book_events")
    trade_files = _files(root, "market_trades")
    availability_files = _files(root, "availability_events")
    if not level_files or not book_files:
        raise FileNotFoundError(f"missing order-book tables under {root}")

    books = (
        pl.scan_parquet(book_files, hive_partitioning=True)
        .select(["venue", "capture_sequence", "local_receive_time"])
    )
    levels = (
        pl.scan_parquet(level_files, hive_partitioning=True)
        .filter(pl.col("position") < max_depth)
        .select(["venue", "capture_sequence", "side", "position", "price", "quantity"])
        .join(books, on=["venue", "capture_sequence"])
        .collect()
    )

    grouped: dict[tuple[str, int], dict] = {}
    for venue, sequence, side, position, price, quantity, local_time in levels.iter_rows():
        key = (venue, sequence)
        item = grouped.setdefault(key, {"venue": venue, "sequence": sequence,
                                        "local_time": local_time, "bids": [], "asks": []})
        item["bids" if side == "bid" else "asks"].append(
            (int(position), Level(float(price), float(quantity)))
        )

    events: list[MarketEvent] = []
    for item in grouped.values():
        book = OrderBook(
            venue=item["venue"], sequence=item["sequence"], local_time_ns=item["local_time"],
            bids=[level for _, level in sorted(item["bids"])],
            asks=[level for _, level in sorted(item["asks"])],
        )
        events.append(MarketEvent(item["sequence"], item["local_time"], "book", item["venue"], book=book))

    if trade_files:
        trades = pl.scan_parquet(trade_files, hive_partitioning=True).select([
            "venue", "capture_sequence", "local_receive_time", "price", "quantity", "aggressor_side"
        ]).collect()
        for venue, sequence, local_time, price, quantity, side in trades.iter_rows():
            trade = MarketTrade(venue, sequence, local_time, float(price), float(quantity), side)
            events.append(MarketEvent(sequence, local_time, "trade", venue, trade=trade))

    if availability_files:
        availability = pl.scan_parquet(availability_files, hive_partitioning=True).select([
            "venue", "capture_sequence", "transition", "observed_at"
        ]).collect()
        for venue, sequence, transition, observed_at in availability.iter_rows():
            events.append(MarketEvent(
                sequence, observed_at, "availability", venue,
                availability_transition=transition,
            ))

    priority = {"availability": 0, "book": 1, "trade": 2}
    return sorted(events, key=lambda event: (event.sequence, priority[event.kind]))
