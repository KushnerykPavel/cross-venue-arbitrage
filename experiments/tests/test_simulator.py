from experiments.simulator.config import SimulationConfig
from experiments.simulator.engine import Simulator
from experiments.simulator.models import Level, MarketEvent, OrderBook, OrderIntent
from experiments.simulator.strategies import CrossVenueArbitrageStrategy


def book(venue, sequence, bid, ask, time_ns, quantity=1.0):
    return MarketEvent(
        sequence=sequence,
        local_time_ns=time_ns,
        kind="book",
        venue=venue,
        book=OrderBook(
            venue=venue,
            sequence=sequence,
            local_time_ns=time_ns,
            bids=[Level(bid, quantity)],
            asks=[Level(ask, quantity)],
        ),
    )


class AlwaysBuy:
    def __init__(self, venue):
        self.venue = venue
        self.sent = False

    def on_event(self, event, state, portfolio):
        if self.sent or self.venue not in state:
            return []
        self.sent = True
        return [OrderIntent(self.venue, "buy", 1.0)]


class OneShotArbitrage:
    def __init__(self):
        self.sent = False

    def on_event(self, event, state, portfolio):
        if self.sent or len(state) < 2:
            return []
        self.sent = True
        return [
            OrderIntent("a", "buy", 1.0),
            OrderIntent("b", "sell", 1.0),
        ]


def test_market_orders_respect_latency_and_fill_visible_liquidity():
    config = SimulationConfig(
        initial_cash=1_000.0,
        order_latency_ns={"a": 0, "b": 0},
        stale_book_ns=1_000,
        taker_fee_bps={"a": 0.0, "b": 0.0},
        slippage_bps={"a": 0.0, "b": 0.0},
        max_position=2.0,
        max_order_quantity=1.0,
    )
    events = [book("a", 1, 99.0, 100.0, 10), book("b", 2, 101.0, 102.0, 20), book("a", 3, 99.0, 100.0, 30)]
    result = Simulator(config).run(events, OneShotArbitrage())
    assert len(result.fills) == 2
    assert result.final_portfolio.inventory == {"a": 1.0, "b": -1.0}


def test_repeated_runs_are_deterministic():
    config = SimulationConfig(order_latency_ns={"a": 0, "b": 0}, stale_book_ns=1_000)
    events = [book("a", 1, 99.0, 100.0, 10), book("b", 2, 101.0, 102.0, 20)]
    first = Simulator(config).run(events, OneShotArbitrage()).summary()
    second = Simulator(config).run(events, OneShotArbitrage()).summary()
    assert first == second


def test_market_data_latency_delays_strategy_visibility():
    config = SimulationConfig(
        order_latency_ns={"a": 0, "b": 0},
        market_data_latency_ns={"a": 100, "b": 100},
        stale_book_ns=1_000,
    )
    events = [book("a", 1, 99.0, 100.0, 10), book("b", 2, 101.0, 102.0, 20)]
    result = Simulator(config).run(events, OneShotArbitrage())
    assert result.orders == []


def test_limit_order_can_remain_active_until_book_crosses():
    class LimitStrategy:
        sent = False

        def on_event(self, event, state, portfolio):
            if self.sent or event.venue != "a":
                return []
            self.sent = True
            return [OrderIntent("a", "buy", 1.0, order_type="limit", limit_price=100.0)]

    config = SimulationConfig(
        order_latency_ns={"a": 0},
        market_data_latency_ns={"a": 0},
        stale_book_ns=1_000,
        max_position=2.0,
    )
    events = [book("a", 1, 99.0, 101.0, 10), book("a", 2, 100.0, 100.0, 20)]
    result = Simulator(config).run(events, LimitStrategy())
    assert len(result.fills) == 1
    assert result.fills[0].price == 100.0


def test_strategy_uses_net_edge_after_both_fees():
    config = SimulationConfig(
        order_latency_ns={"a": 0, "b": 0},
        market_data_latency_ns={"a": 0, "b": 0},
        taker_fee_bps={"a": 5.0, "b": 5.0},
        minimum_net_edge_bps=1.0,
        max_order_quantity=1.0,
    )
    events = [book("a", 1, 99.0, 100.0, 10), book("b", 2, 100.08, 101.0, 20)]
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    assert result.orders == []


def test_strategy_requires_latency_buffer_after_net_edge():
    config = SimulationConfig(
        order_latency_ns={"a": 100, "b": 100},
        market_data_latency_ns={"a": 0, "b": 0},
        taker_fee_bps={"a": 0.0, "b": 0.0},
        minimum_net_edge_bps=1.0,
        latency_buffer_bps=2.0,
        max_order_quantity=1.0,
    )
    events = [book("a", 1, 99.0, 100.0, 10), book("b", 2, 100.015, 101.0, 20)]
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    assert result.orders == []


def test_binance_is_a_signal_only_venue_by_default():
    config = SimulationConfig()

    assert config.signal_only_venues == frozenset({"binance"})
    assert "hyperliquid" not in config.order_latency_ns
    assert config.market_data_latency_ns["binance"] == 0


def free_config():
    return SimulationConfig(
        taker_fee_bps={"aster": 0.0, "binance": 0.0, "lighter": 0.0},
        minimum_net_edge_bps=0.0,
        latency_buffer_bps=0.0,
        max_order_quantity=1.0,
    )


def test_strategy_never_orders_on_binance():
    config = free_config()
    events = [book("binance", 1, 99.0, 100.0, 10), book("aster", 2, 101.0, 102.0, 20)]
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    assert all(order.intent.venue != "binance" for order in result.orders)


def test_exchange_rejects_orders_on_signal_only_venue():
    config = free_config()
    events = [book("binance", 1, 99.0, 100.0, 10), book("aster", 2, 99.0, 100.0, 20)]
    result = Simulator(config).run(events, AlwaysBuy("binance"))
    assert [order.status for order in result.orders] == ["rejected"]
    assert result.fills == []


def unavailable(venue, sequence, time_ns, stream):
    return MarketEvent(
        sequence, time_ns, "availability", venue,
        availability_transition="unavailable", availability_stream=stream,
    )


def test_unavailable_book_is_not_used_until_next_snapshot():
    config = free_config()
    events = [
        book("aster", 1, 99.0, 100.0, 10),
        unavailable("aster", 2, 20, "order_book"),
        book("lighter", 3, 101.0, 102.0, 30),
    ]
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    assert result.orders == []

    events.append(book("aster", 4, 99.0, 100.0, 40))
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    assert {order.intent.venue for order in result.orders} == {"aster", "lighter"}


def test_trade_stream_outage_does_not_invalidate_the_book():
    config = free_config()
    events = [
        book("aster", 1, 99.0, 100.0, 10),
        unavailable("aster", 2, 20, "trade_stream"),
        book("lighter", 3, 101.0, 102.0, 30),
    ]
    result = Simulator(config).run(events, CrossVenueArbitrageStrategy(config))
    assert {order.intent.venue for order in result.orders} == {"aster", "lighter"}
