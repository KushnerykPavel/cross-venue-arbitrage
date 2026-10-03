import math

import polars as pl
import pytest

from experiments.analysis.lead_lag import (
    asof_quote,
    binance_move_events,
    cross_correlation,
    first_lag_move,
    responses,
    round_trip_edge,
)

MS = 1_000_000
STALE = 1_000 * MS


def quotes(venue, rows):
    """rows: (t_ms, bid, ask); bid=None marks an Order Book outage."""
    return pl.DataFrame(
        [
            (venue, t * MS, bid, ask, None if bid is None else (bid + ask) / 2)
            for t, bid, ask in rows
        ],
        schema={"venue": pl.String, "t": pl.Int64, "bid": pl.Float64, "ask": pl.Float64, "mid": pl.Float64},
        orient="row",
    )


def events(rows):
    """rows: (t0_ms, direction, lead_move_bps)."""
    return pl.DataFrame(
        [(t * MS, d, m) for t, d, m in rows],
        schema={"t0": pl.Int64, "direction": pl.Int8, "lead_move_bps": pl.Float64},
        orient="row",
    ).with_row_index("event_id")


def at(times_ms):
    return pl.DataFrame({"q": [t * MS for t in times_ms]})


def test_asof_never_uses_a_future_quote():
    lag = quotes("aster", [(100, 99.0, 101.0), (200, 109.0, 111.0)])
    out = asof_quote(lag, at([50, 100, 199, 200]), "q", STALE, "x_")
    assert out["x_mid"].to_list() == [None, 100.0, 100.0, 110.0]


def test_asof_drops_stale_quotes():
    lag = quotes("aster", [(0, 99.0, 101.0)])
    out = asof_quote(lag, at([1_000, 1_001]), "q", STALE, "x_")
    assert out["x_mid"].to_list() == [100.0, None]


def test_asof_returns_nothing_during_an_outage_until_next_snapshot():
    lag = quotes("lighter", [(0, 99.0, 101.0), (100, None, None), (300, 104.0, 106.0)])
    out = asof_quote(lag, at([50, 150, 299, 300]), "q", STALE, "x_")
    assert out["x_mid"].to_list() == [100.0, None, None, 105.0]


def test_move_events_respect_threshold_direction_and_cooldown():
    # Mid: 100 → 100.1 (+10 bps) → 100.2 (+10 bps, inside cooldown) → 99.9 (−~30 bps over window).
    binance = quotes("binance", [
        (0, 99.99, 100.01),
        (100, 100.09, 100.11),
        (200, 100.19, 100.21),
        (1_000, 99.89, 99.91),
        (1_100, 99.89, 99.91),
    ])
    found = binance_move_events(binance, threshold_bps=5.0, window_ns=100 * MS, cooldown_ns=500 * MS, stale_ns=STALE)
    assert found["t0"].to_list() == [100 * MS, 1_000 * MS]
    assert found["direction"].to_list() == [1, -1]
    assert found["lead_move_bps"][0] == pytest.approx(10.0, rel=1e-6)


def test_responses_are_signed_by_lead_direction():
    lag = quotes("aster", [(0, 99.0, 101.0), (150, 98.0, 100.0)])  # mid 100 → 99
    out = responses(events([(100, -1, 20.0)]), lag, [10 * MS, 100 * MS], STALE)
    assert out["lag_move_bps"].to_list() == pytest.approx([0.0, 100.0])
    assert out["capture_ratio"][1] == pytest.approx(5.0)


def test_first_lag_move_reports_delay_and_direction():
    lag = quotes("aster", [(0, 99.0, 101.0), (40, 99.0, 101.0), (70, 100.0, 102.0)])
    out = first_lag_move(events([(10, 1, 10.0), (10, -1, 10.0)]), lag, max_wait_ns=1_000 * MS, stale_ns=STALE)
    assert out["delay_ns"].to_list() == [60 * MS, 60 * MS]
    assert out["same_direction"].to_list() == [True, False]


def test_round_trip_crosses_the_spread_and_pays_two_fees():
    # Long: buy ask 101 at t0+L, sell bid 102 at t0+L+h → +99.0099 bps gross.
    lag = quotes("lighter", [(0, 100.0, 101.0), (500, 102.0, 103.0)])
    long_edge = round_trip_edge(events([(0, 1, 10.0)]), lag, latency_ns=50 * MS,
                                horizons_ns=[500 * MS], fee_bps=5.0, stale_ns=STALE)
    expected = (102.0 / 101.0 - 1) * 10_000
    assert long_edge["gross_bps"][0] == pytest.approx(expected)
    assert long_edge["net_bps"][0] == pytest.approx(expected - 10.0)

    # Short on the same path: sell bid 100, buy back ask 103 → loss.
    short_edge = round_trip_edge(events([(0, -1, 10.0)]), lag, latency_ns=50 * MS,
                                 horizons_ns=[500 * MS], fee_bps=0.0, stale_ns=STALE)
    assert short_edge["gross_bps"][0] == pytest.approx(-(103.0 / 100.0 - 1) * 10_000)


def test_round_trip_keeps_events_without_quotes_as_null():
    lag = quotes("lighter", [(0, 100.0, 101.0), (100, None, None)])
    out = round_trip_edge(events([(0, 1, 10.0)]), lag, latency_ns=0, horizons_ns=[200 * MS],
                          fee_bps=5.0, stale_ns=STALE)
    assert out.height == 1
    assert out["net_bps"][0] is None


def test_cross_correlation_recovers_an_injected_lag():
    bar = 10 * MS
    steps = [((i * 7919) % 13) - 6 for i in range(400)]  # deterministic pseudo-random walk
    lead_prices, price = [], 100.0
    for step in steps:
        price *= math.exp(step * 1e-4)
        lead_prices.append(price)
    k = 3
    lag_prices = [lead_prices[0]] * k + lead_prices[:-k]
    lead = quotes("binance", [(i * 10, p - 0.01, p + 0.01) for i, p in enumerate(lead_prices)])
    lag = quotes("aster", [(i * 10, p - 0.01, p + 0.01) for i, p in enumerate(lag_prices)])
    out = cross_correlation(lead, lag, bar, list(range(-5, 6)), STALE)
    best = out.sort("corr", descending=True).row(0, named=True)
    assert best["lag_bars"] == k
    assert best["corr"] > 0.99


def test_bar_returns_carry_book_state_between_sparse_updates():
    from experiments.analysis.lead_lag import bar_returns

    lag = quotes("aster", [(0, 99.0, 101.0), (100, 101.0, 103.0)])  # one update per 100 ms
    out = bar_returns(lag, 10 * MS, STALE)
    assert out.height == 9  # boundaries 10..100 ms all have a state → 9 adjacent returns
    assert out.filter(pl.col("ret") != 0)["bar"].to_list() == [10]  # move lands in the bar ending at 100 ms


def test_bar_returns_do_not_bridge_an_outage():
    from experiments.analysis.lead_lag import bar_returns

    lag = quotes("lighter", [(0, 99.0, 101.0), (15, None, None), (35, 104.0, 106.0), (50, 104.0, 106.0)])
    out = bar_returns(lag, 10 * MS, STALE)
    # Boundaries 20 and 30 ms fall in the outage, so returns at bars 2, 3 and 4 are dropped;
    # only 40 → 50 ms remains, and the 100 → 105 jump across the outage is never reported.
    assert out["bar"].to_list() == [5]
    assert out["ret"].to_list() == [0.0]
