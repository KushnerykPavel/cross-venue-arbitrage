"""Binance → lag-venue lead/lag measurement on top-of-book quotes.

All times are Local Receive Time in nanoseconds on the shared capture clock, so
a measured lead is the lead observed at our collector, not exchange-level
causality. Prices are converted to float only at this research boundary.

A quote frame has columns ``venue, t, bid, ask, mid``. An Order Book outage is
represented as a marker row with null prices at the outage time, so a backward
as-of lookup that lands on it returns no quote until the next snapshot.
"""

from pathlib import Path

import polars as pl

BPS = 10_000.0
QUOTE_COLUMNS = ("bid", "ask", "mid")


def _files(dataset: Path, kind: str) -> list[str]:
    return sorted(str(p) for p in dataset.glob(f"**/event_type={kind}/**/*.parquet"))


def top_of_book(dataset: str | Path, sequence_start: int, sequence_end: int | None) -> pl.DataFrame:
    """Position-0 quotes plus Order Book outage markers for one sequence range."""
    root = Path(dataset)

    def within(frame: pl.LazyFrame) -> pl.LazyFrame:
        frame = frame.filter(pl.col("capture_sequence") >= sequence_start)
        if sequence_end is not None:
            frame = frame.filter(pl.col("capture_sequence") < sequence_end)
        return frame

    books = within(pl.scan_parquet(_files(root, "order_book_events"), hive_partitioning=True)).select(
        "venue", "capture_sequence", pl.col("local_receive_time").cast(pl.Int64).alias("t")
    )
    levels = within(pl.scan_parquet(_files(root, "order_book_levels"), hive_partitioning=True)).filter(
        pl.col("position") == 0
    )
    sides = [
        levels.filter(pl.col("side") == side).select(
            "venue", "capture_sequence", pl.col("price").cast(pl.Float64).alias(side)
        )
        for side in ("bid", "ask")
    ]
    quotes = (
        books.join(sides[0], on=["venue", "capture_sequence"], how="left")
        .join(sides[1], on=["venue", "capture_sequence"], how="left")
        .with_columns(((pl.col("bid") + pl.col("ask")) / 2).alias("mid"))
    )
    outages = within(pl.scan_parquet(_files(root, "availability_events"), hive_partitioning=True)).filter(
        (pl.col("stream") == "order_book") & (pl.col("transition") == "unavailable")
    ).select(
        "venue", "capture_sequence", pl.col("observed_at").cast(pl.Int64).alias("t"),
        *(pl.lit(None, dtype=pl.Float64).alias(c) for c in QUOTE_COLUMNS),
    )
    return pl.concat([quotes, outages]).sort("venue", "t", "capture_sequence").collect()


def asof_quote(
    quotes: pl.DataFrame, queries: pl.DataFrame, time_col: str, stale_ns: int, prefix: str
) -> pl.DataFrame:
    """Attach the last quote at or before ``time_col``; null if stale or in an outage."""
    right = quotes.select(pl.col("t").alias("_quote_t"), *QUOTE_COLUMNS).sort("_quote_t")
    joined = queries.sort(time_col).join_asof(
        right, left_on=time_col, right_on="_quote_t", strategy="backward"
    )
    unusable = pl.col("_quote_t").is_null() | ((pl.col(time_col) - pl.col("_quote_t")) > stale_ns)
    return joined.with_columns(
        pl.when(unusable).then(None).otherwise(pl.col(c)).alias(prefix + c) for c in QUOTE_COLUMNS
    ).drop("_quote_t", *QUOTE_COLUMNS)


def binance_move_events(
    binance: pl.DataFrame, threshold_bps: float, window_ns: int, cooldown_ns: int, stale_ns: int
) -> pl.DataFrame:
    """Binance mid moves of at least ``threshold_bps`` over a trailing window.

    Returns ``event_id, t0, direction, lead_move_bps``. After an event, later
    candidates within ``cooldown_ns`` are skipped so responses do not overlap.
    """
    current = binance.filter(pl.col("mid").is_not_null()).select("t", pl.col("mid").alias("now_mid"))
    past = asof_quote(
        binance, current.with_columns((pl.col("t") - window_ns).alias("t_past")), "t_past", stale_ns, "past_"
    )
    candidates = (
        past.with_columns(((pl.col("now_mid") / pl.col("past_mid") - 1) * BPS).alias("move_bps"))
        .filter(pl.col("move_bps").abs() >= threshold_bps)
        .sort("t")
    )
    kept: list[tuple[int, int, float]] = []
    last_t: int | None = None
    for t, move in candidates.select("t", "move_bps").iter_rows():
        if last_t is None or t - last_t >= cooldown_ns:
            kept.append((t, 1 if move > 0 else -1, abs(move)))
            last_t = t
    return pl.DataFrame(
        kept, schema={"t0": pl.Int64, "direction": pl.Int8, "lead_move_bps": pl.Float64}, orient="row"
    ).with_row_index("event_id")


def responses(
    events: pl.DataFrame, lag: pl.DataFrame, horizons_ns: list[int], stale_ns: int
) -> pl.DataFrame:
    """Signed lag-venue mid change from t0 to t0+h, one row per event and horizon."""
    base = asof_quote(lag, events, "t0", stale_ns, "start_")
    rows = []
    for horizon in horizons_ns:
        at = asof_quote(lag, base.with_columns((pl.col("t0") + horizon).alias("t_h")), "t_h", stale_ns, "end_")
        rows.append(at.select(
            "event_id", pl.lit(horizon, dtype=pl.Int64).alias("horizon_ns"), "lead_move_bps",
            ((pl.col("end_mid") / pl.col("start_mid") - 1) * BPS * pl.col("direction")).alias("lag_move_bps"),
        ))
    return pl.concat(rows).with_columns(
        (pl.col("lag_move_bps") / pl.col("lead_move_bps")).alias("capture_ratio")
    ).sort("event_id", "horizon_ns")


def first_lag_move(events: pl.DataFrame, lag: pl.DataFrame, max_wait_ns: int, stale_ns: int) -> pl.DataFrame:
    """Delay to the first lag mid change after t0, and whether it follows the lead direction."""
    changes = (
        lag.filter(pl.col("mid").is_not_null())
        .with_columns(pl.col("mid").diff().alias("delta"))
        .filter(pl.col("delta").is_not_null() & (pl.col("delta") != 0))
        .select(pl.col("t").alias("t_move"), pl.col("delta").sign().cast(pl.Int8).alias("move_sign"))
        .sort("t_move")
    )
    base = asof_quote(lag, events, "t0", stale_ns, "start_").filter(pl.col("start_mid").is_not_null())
    joined = base.with_columns((pl.col("t0") + 1).alias("t_after")).join_asof(
        changes, left_on="t_after", right_on="t_move", strategy="forward"
    )
    return joined.select(
        "event_id",
        pl.when((pl.col("t_move") - pl.col("t0")) <= max_wait_ns).then(pl.col("t_move") - pl.col("t0")).alias("delay_ns"),
        pl.when((pl.col("t_move") - pl.col("t0")) <= max_wait_ns)
        .then(pl.col("move_sign") == pl.col("direction")).alias("same_direction"),
    ).sort("event_id")


def round_trip_edge(
    events: pl.DataFrame,
    lag: pl.DataFrame,
    latency_ns: int,
    horizons_ns: list[int],
    fee_bps: float,
    stale_ns: int,
) -> pl.DataFrame:
    """Taker round trip on the lag venue: enter at t0+L, exit at t0+L+h.

    Up moves buy at the ask and sell at the bid; down moves sell at the bid and
    buy back at the ask. ``gross_bps`` excludes fees; ``net_bps`` subtracts the
    taker fee on both legs. Events without a usable quote at either end are
    returned with null edge rather than dropped.
    """
    entry = asof_quote(
        lag, events.with_columns((pl.col("t0") + latency_ns).alias("t_entry")), "t_entry", stale_ns, "in_"
    )
    entry_price = pl.when(pl.col("direction") > 0).then(pl.col("in_ask")).otherwise(pl.col("in_bid"))
    rows = []
    for horizon in horizons_ns:
        exit_ = asof_quote(
            lag, entry.with_columns((pl.col("t_entry") + horizon).alias("t_exit")), "t_exit", stale_ns, "out_"
        )
        exit_price = pl.when(pl.col("direction") > 0).then(pl.col("out_bid")).otherwise(pl.col("out_ask"))
        rows.append(exit_.select(
            "event_id", pl.lit(horizon, dtype=pl.Int64).alias("horizon_ns"),
            ((exit_price / entry_price - 1) * BPS * pl.col("direction")).alias("gross_bps"),
        ))
    return pl.concat(rows).with_columns(
        pl.lit(latency_ns, dtype=pl.Int64).alias("latency_ns"),
        pl.lit(fee_bps).alias("fee_bps"),
        (pl.col("gross_bps") - 2 * fee_bps).alias("net_bps"),
    ).sort("event_id", "horizon_ns")


def bar_returns(quotes: pl.DataFrame, bar_ns: int, stale_ns: int) -> pl.DataFrame:
    """Log return of the as-of mid sampled at each bar boundary.

    Venues publish books every ~50-100 ms, so most short bars contain no
    update; the book state at a boundary is the last accepted snapshot. A
    boundary inside an outage or past ``stale_ns`` has no state, and returns
    touching it are dropped rather than bridged.
    """
    observed = quotes.filter(pl.col("mid").is_not_null())
    first = observed["t"].min() // bar_ns + 1
    last = observed["t"].max() // bar_ns
    grid = pl.DataFrame({"bar": pl.int_range(first, last + 1, eager=True)}).with_columns(
        (pl.col("bar") * bar_ns).alias("t_bar")
    )
    sampled = asof_quote(quotes, grid, "t_bar", stale_ns, "s_").sort("bar")
    return sampled.select(
        "bar", pl.col("s_mid").log().diff().alias("ret")
    ).drop_nulls()


def cross_correlation(
    lead: pl.DataFrame, lag: pl.DataFrame, bar_ns: int, lags: list[int], stale_ns: int
) -> pl.DataFrame:
    """corr(lead return at bar b, lag return at bar b+k). A peak at k>0 means the lead venue leads."""
    lead_ret = bar_returns(lead, bar_ns, stale_ns).rename({"ret": "lead_ret"})
    lag_ret = bar_returns(lag, bar_ns, stale_ns).rename({"ret": "lag_ret"})
    rows = []
    for k in lags:
        pairs = lead_ret.join(lag_ret.with_columns(pl.col("bar") - k), on="bar")
        rows.append({
            "lag_bars": k,
            "lag_ms": k * bar_ns / 1e6,
            "corr": pairs.select(pl.corr("lead_ret", "lag_ret")).item() if pairs.height > 2 else None,
            "pairs": pairs.height,
        })
    return pl.DataFrame(rows)
