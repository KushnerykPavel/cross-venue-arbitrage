"""Binance book-quality checks and a trade-triggered event variant on the Sept 27 capture.
Train and validation only; the test split is never loaded."""
import sys
from pathlib import Path

import polars as pl

ROOT = next(p for p in (Path.cwd(), *Path.cwd().parents) if (p / "backend").is_dir())
sys.path.insert(0, str(ROOT))
from experiments.analysis.lead_lag import binance_move_events, round_trip_edge, top_of_book  # noqa: E402
from experiments.simulator.splits import chronological_splits  # noqa: E402

DATASET = ROOT / "experiments/data/parquet/6b556515-5156-4181-8a6f-7403fa9078fe"
MS = 1_000_000
STALE_NS, WINDOW_NS, COOLDOWN_NS = 1000 * MS, 200 * MS, 2000 * MS
TICK = 0.1
train, validation, test = chronological_splits(DATASET)
SPLITS = {"train": train, "validation": validation}
assert all(s.sequence_end <= test.sequence_start for s in SPLITS.values())


def files(kind):
    return [str(p) for p in DATASET.glob(f"**/event_type={kind}/**/*.parquet")]


def within(lf, s):
    return lf.filter(pl.col("capture_sequence").is_between(s.sequence_start, s.sequence_end, closed="left"))


def exch(s, kind):
    return within(pl.scan_parquet(files("exchange_times"), hive_partitioning=True), s).filter(
        (pl.col("venue") == "binance") & (pl.col("kind") == kind)
    ).select("capture_sequence", pl.col("raw_value").cast(pl.Int64).alias(kind))


def binance_trades(s):
    t = within(pl.scan_parquet(files("market_trades"), hive_partitioning=True), s).filter(pl.col("venue") == "binance")
    t = t.select("capture_sequence", pl.col("local_receive_time").cast(pl.Int64).alias("t"),
                 pl.col("price").cast(pl.Float64), pl.col("quantity").cast(pl.Float64), "aggressor_side")
    return t.join(exch(s, "TradeTime"), on="capture_sequence").join(exch(s, "EventTime"), on="capture_sequence").collect().sort("capture_sequence")


q = lambda col, p: pl.col(col).quantile(p)
for name, s in SPLITS.items():
    tr = binance_trades(s)
    tob = top_of_book(DATASET, s.sequence_start, s.sequence_end)
    matches = tr.group_by("TradeTime").agg(pl.col("t").min(), ((pl.col("price") * pl.col("quantity")).sum() / pl.col("quantity").sum()).alias("mid")).sort("t")
    tq = matches.select(pl.lit("binance").alias("venue"), "t", pl.col("mid").alias("bid"), pl.col("mid").alias("ask"), "mid")
    for X in (2.0, 5.0):
        eb = binance_move_events(tob.filter(pl.col("venue") == "binance"), X, WINDOW_NS, COOLDOWN_NS, STALE_NS)
        et = binance_move_events(tq, X, WINDOW_NS, COOLDOWN_NS, STALE_NS)
        m = eb.sort("t0").join_asof(et.sort("t0").select(pl.col("t0").alias("t0_trade"), pl.col("direction").alias("dir_trade")),
                                    left_on="t0", right_on="t0_trade", strategy="nearest", tolerance=500 * MS).filter(pl.col("dir_trade") == pl.col("direction"))
        early = m.select("event_id", pl.min_horizontal("t0", "t0_trade").alias("t0"), "direction", "lead_move_bps")
        late = m.select("event_id", "t0", "direction", "lead_move_bps")
        for v, L, fee in (("aster", 50, 0.0), ("lighter", 50, 0.0), ("lighter", 350, 0.0)):
            lag = tob.filter(pl.col("venue") == v)
            r = []
            for ev in (late, early):
                e = round_trip_edge(ev, lag, L * MS, [1000 * MS, 2000 * MS], fee, STALE_NS)
                r.append(e.group_by("horizon_ns").agg(pl.col("gross_bps").mean().round(2)).sort("horizon_ns")["gross_bps"].to_list())
            print(f"{name:10s} X={X} n={m.height:3d} {v:7s} L={L}: gross (1s,2s) book-t0 {r[0]}  earliest(book,trade)-t0 {r[1]}")
