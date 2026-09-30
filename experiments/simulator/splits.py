"""Contiguous, time-based research splits in canonical capture order."""

import json
from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True)
class Split:
    name: str
    sequence_start: int
    sequence_end: int | None


def sequence_splits(first_sequence: int, validation_start: int, test_start: int) -> tuple[Split, ...]:
    if not first_sequence < validation_start < test_start:
        raise ValueError("train, validation, and test must each contain events")
    return (
        Split("train", first_sequence, validation_start),
        Split("validation", validation_start, test_start),
        Split("test", test_start, None),
    )


def chronological_splits(dataset: str | Path) -> tuple[Split, ...]:
    """Use local receive time for cut points and Capture Sequence for membership.

    The first event at or after 60% and 80% of elapsed receive time starts the
    next split. Sequence ranges keep all event types in canonical order, even
    when two receive timestamps straddle a boundary out of sequence.
    """
    root = Path(dataset)
    metadata = root / "dataset-metadata.json"
    if not metadata.is_file():
        raise FileNotFoundError(metadata)
    dataset_metadata = json.loads(metadata.read_text())
    if len(dataset_metadata.get("source_capture_ids", [])) != 1:
        raise ValueError("research splits require exactly one Capture Run")
    if dataset_metadata.get("source_capture_statuses") != ["complete"]:
        raise ValueError("research splits require a complete Capture Run")

    tables = []
    for kind in ("order_book_events", "market_trades"):
        files = [str(path) for path in root.glob(f"**/event_type={kind}/**/*.parquet")]
        if files:
            tables.append(
                pl.scan_parquet(files, hive_partitioning=True).select(
                    "capture_sequence", "local_receive_time"
                )
            )
    if not tables:
        raise FileNotFoundError(f"no market events under {root}")
    events = pl.concat(tables)
    bounds = events.select(
        pl.min("capture_sequence").alias("first_sequence"),
        pl.min("local_receive_time").alias("first_time"),
        pl.max("local_receive_time").alias("last_time"),
    ).collect().row(0, named=True)
    first_time = bounds["first_time"]
    elapsed = bounds["last_time"] - first_time
    if elapsed <= 0:
        raise ValueError("capture has no positive elapsed receive time")
    cut_times = (first_time + elapsed * 3 // 5, first_time + elapsed * 4 // 5)
    cut_sequences = [
        events.filter(pl.col("local_receive_time") >= cut_time)
        .select(pl.min("capture_sequence"))
        .collect().item()
        for cut_time in cut_times
    ]
    return sequence_splits(bounds["first_sequence"], *cut_sequences)
