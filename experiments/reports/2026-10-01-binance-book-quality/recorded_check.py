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
    print(f"\n######## {name}")
    tr = binance_trades(s)
    tob = top_of_book(DATASET, s.sequence_start, s.sequence_end)
    book = tob.filter((pl.col("venue") == "binance") & pl.col("mid").is_not_null())
    bE = exch(s, "EventTime").collect()
    book = book.join(bE, on="capture_sequence").sort("EventTime")

    # 1. server-side delay of aggTrade frames
    print("[1] aggTrade E - T ms:",
          tr.with_columns((pl.col("EventTime") - pl.col("TradeTime")).alias("d")).select(
              *(q("d", p).alias(f"p{int(p*100)}") for p in (.5, .9, .99)), pl.col("d").max().alias("max")).row(0, named=True))
    # local receive gap trade vs book, both relative to exchange time (offset cancels in the difference)
    tl = tr.select(((pl.col("t") / MS) - pl.col("TradeTime")).median()).item()
    bl = book.select(((pl.col("t") / MS) - pl.col("EventTime")).median()).item()
    print(f"    median (recv - exchange time): trades vs T {tl:.1f} ms, book vs E {bl:.1f} ms -> trades arrive {tl-bl:+.1f} ms later relative to their match")

    # 2. strict prev/next classification on exchange time with aggressor side
    prev = book.select(pl.col("EventTime").alias("E_prev"), pl.col("bid").alias("pb"), pl.col("ask").alias("pa"))
    nxt = book.select(pl.col("EventTime").alias("E_next"), pl.col("bid").alias("nb"), pl.col("ask").alias("na"))
    j = (tr.sort("TradeTime").with_columns((pl.col("TradeTime") - 1).alias("T_before"), (pl.col("TradeTime") + 4).alias("T_after"))
         .join_asof(prev.sort("E_prev"), left_on="T_before", right_on="E_prev", strategy="backward")
         .join_asof(nxt.sort("E_next"), left_on="T_after", right_on="E_next", strategy="forward")
         .drop_nulls(["pb", "nb"]))
    buy = pl.col("aggressor_side") == "Buy"
    inside = (pl.col("price") >= pl.col("pb")) & (pl.col("price") <= pl.col("pa"))
    sweep = (buy & (pl.col("price") > pl.col("pa"))) | (~buy & (pl.col("price") < pl.col("pb")))
    j = j.with_columns(pl.when(inside).then(pl.lit("inside_prev")).when(sweep).then(pl.lit("aggressor_sweep"))
                       .otherwise(pl.lit("wrong_direction")).alias("cls"))
    print("[2] trades vs strictly-previous book (exchange time):")
    print(j.group_by("cls").len().with_columns((pl.col("len") / j.height).alias("share")).sort("len", descending=True))
    print("    aggressor_side values:", tr["aggressor_side"].unique().to_list())

    # 3. trade-price changes decomposition
    secs = (tr["t"].max() - tr["t"].min()) / 1e9
    pc = tr.with_columns(pl.col("price").diff().alias("dp"), pl.col("TradeTime").diff().alias("dT")).filter(pl.col("dp") != 0)
    print(f"[3] trade price changes/s {pc.height/secs:.2f}; within one match (same T) {(pc['dT']==0).mean():.0%}; "
          f"distinct match times/s {tr['TradeTime'].n_unique()/secs:.2f}; book mid changes/s {(book['mid'].diff()!=0).sum()/secs:.2f}")

    # 4. trade-triggered events: per-match VWAP keyed on first local receive of the match
    matches = tr.group_by("TradeTime").agg(pl.col("t").min(), ((pl.col("price") * pl.col("quantity")).sum() / pl.col("quantity").sum()).alias("mid")).sort("t")
    trade_quotes = matches.select(pl.lit("binance").alias("venue"), "t", pl.col("mid").alias("bid"), pl.col("mid").alias("ask"), "mid")
    book_quotes = tob.filter(pl.col("venue") == "binance")
    for X in (2.0, 5.0):
        eb = binance_move_events(book_quotes, X, WINDOW_NS, COOLDOWN_NS, STALE_NS)
        et = binance_move_events(trade_quotes, X, WINDOW_NS, COOLDOWN_NS, STALE_NS)
        m = eb.sort("t0").join_asof(et.sort("t0").select(pl.col("t0").alias("t0_trade"), pl.col("direction").alias("dir_trade")),
                                    left_on="t0", right_on="t0_trade", strategy="nearest", tolerance=500 * MS)
        m = m.filter(pl.col("dir_trade") == pl.col("direction")).with_columns(((pl.col("t0") - pl.col("t0_trade")) / MS).alias("book_minus_trade_ms"))
        stats = m.select(pl.len().alias("matched"), *(q("book_minus_trade_ms", p).alias(f"p{int(p*100)}") for p in (.1, .5, .9))).row(0, named=True)
        print(f"[4] X={X}: events book {eb.height}, trade {et.height}; matched {stats}")
        for v, L, fee in (("aster", 50, 4.0), ("lighter", 350, 0.0), ("aster", 0, 0.0), ("lighter", 0, 0.0)):
            lag = tob.filter(pl.col("venue") == v)
            row = {}
            for label, ev in (("book", eb), ("trade", et)):
                if ev.height:
                    e = round_trip_edge(ev, lag, L * MS, [1000 * MS, 2000 * MS], fee, STALE_NS)
                    row[label] = e.group_by("horizon_ns").agg(pl.len(), pl.col("net_bps").mean().round(2)).sort("horizon_ns").rows()
            print(f"    {v:7s} L={L:3d} fee={fee}: net mean by h(1s,2s): book {row.get('book')} | trade {row.get('trade')}")
