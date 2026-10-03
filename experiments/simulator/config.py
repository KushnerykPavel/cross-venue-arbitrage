from dataclasses import dataclass, field


@dataclass(frozen=True)
class SimulationConfig:
    # ADR 0010: Binance is a lead/signal venue only. Its books are visible to the
    # strategy, but no order or position may be created there.
    signal_only_venues: frozenset[str] = frozenset({"binance"})
    initial_cash: float = 1_000_000.0
    order_latency_ns: dict[str, int] = field(default_factory=lambda: {
        "aster": 50_000_000,
        "binance": 50_000_000,
        "lighter": 50_000_000,
    })
    market_data_latency_ns: dict[str, int] = field(default_factory=lambda: {
        "aster": 0,
        "binance": 0,
        "lighter": 0,
    })
    # Venue-imposed delay on taker (market) orders, added after order latency.
    # Lighter Standard accounts delay taker orders by 300 ms; makers are not
    # delayed (docs.lighter.xyz/trading/trading-fees, checked 2026-10-01).
    taker_speed_bump_ns: dict[str, int] = field(default_factory=lambda: {
        "lighter": 300_000_000,
    })
    # Venue-imposed delay on cancels, added after order latency. Lighter
    # Standard delays cancel/modify by 300 ms (same source). Until a cancel
    # takes effect, the target order can still fill.
    cancel_speed_bump_ns: dict[str, int] = field(default_factory=lambda: {
        "lighter": 300_000_000,
    })
    stale_book_ns: int = 500_000_000
    max_quote_skew_ns: int = 100_000_000
    # Account tiers in use (checked 2026-10-01): Aster base tier (4 bps taker)
    # and Lighter Standard (0 bps). Binance is signal-only and never charged.
    taker_fee_bps: dict[str, float] = field(default_factory=lambda: {
        "aster": 4.0,
        "binance": 5.0,
        "lighter": 0.0,
    })
    slippage_bps: dict[str, float] = field(default_factory=lambda: {
        "aster": 0.0,
        "binance": 0.0,
        "lighter": 0.0,
    })
    max_position: float = 1.0
    max_order_quantity: float = 0.1
    max_book_depth: int = 10
    minimum_net_edge_bps: float = 1.0
    latency_buffer_bps: float = 1.0
    # Backward-compatible alias for older notebooks/configuration.
    minimum_edge_bps: float | None = None
    cooldown_ns: int = 100_000_000
    default_order_ttl_ns: int | None = None
