from dataclasses import dataclass, field


@dataclass(frozen=True)
class SimulationConfig:
    initial_cash: float = 1_000_000.0
    order_latency_ns: dict[str, int] = field(default_factory=lambda: {
        "aster": 50_000_000,
        "hyperliquid": 50_000_000,
        "lighter": 50_000_000,
    })
    market_data_latency_ns: dict[str, int] = field(default_factory=lambda: {
        "aster": 0,
        "hyperliquid": 0,
        "lighter": 0,
    })
    stale_book_ns: int = 500_000_000
    max_quote_skew_ns: int = 100_000_000
    taker_fee_bps: dict[str, float] = field(default_factory=lambda: {
        "aster": 5.0,
        "hyperliquid": 5.0,
        "lighter": 5.0,
    })
    slippage_bps: dict[str, float] = field(default_factory=lambda: {
        "aster": 0.0,
        "hyperliquid": 0.0,
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
