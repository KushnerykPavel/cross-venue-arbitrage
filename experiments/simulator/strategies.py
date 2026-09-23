from .config import SimulationConfig
from .models import MarketEvent, OrderBook, OrderIntent, PortfolioState


def _available_quantity(book: OrderBook, side: str) -> float:
    return sum(level.quantity for level in (book.asks if side == "buy" else book.bids))


def _notional(book: OrderBook, side: str, quantity: float) -> float | None:
    """Return executable notional across L2 levels, or None if insufficient depth."""
    remaining = quantity
    total = 0.0
    for level in book.asks if side == "buy" else book.bids:
        take = min(remaining, level.quantity)
        total += take * level.price
        remaining -= take
        if remaining <= 1e-12:
            return total
    return None


class CrossVenueArbitrageStrategy:
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.last_submission_ns = -config.cooldown_ns

    def on_event(self, event: MarketEvent, state: dict[str, OrderBook], portfolio: PortfolioState) -> list[OrderIntent]:
        if event.kind != "book" or event.book is None:
            return []
        if event.local_time_ns - self.last_submission_ns < self.config.cooldown_ns:
            return []
        books = {
            venue: book for venue, book in state.items()
            if event.local_time_ns - book.local_time_ns <= self.config.stale_book_ns
            and book.best_bid and book.best_ask
        }
        best: tuple[float, float, str, str, int] | None = None
        for buy_venue, buy_book in books.items():
            for sell_venue, sell_book in books.items():
                if buy_venue == sell_venue:
                    continue
                quote_skew_ns = abs(buy_book.local_time_ns - sell_book.local_time_ns)
                if quote_skew_ns > self.config.max_quote_skew_ns:
                    continue
                quantity = min(
                    self.config.max_order_quantity,
                    _available_quantity(buy_book, "buy"),
                    _available_quantity(sell_book, "sell"),
                )
                if quantity <= 0:
                    continue
                buy_notional = _notional(buy_book, "buy", quantity)
                sell_notional = _notional(sell_book, "sell", quantity)
                if buy_notional is None or sell_notional is None:
                    continue
                buy_fee = buy_notional * self.config.taker_fee_bps.get(buy_venue, 0.0) / 10_000
                sell_fee = sell_notional * self.config.taker_fee_bps.get(sell_venue, 0.0) / 10_000
                buy_slippage = buy_notional * self.config.slippage_bps.get(buy_venue, 0.0) / 10_000
                sell_slippage = sell_notional * self.config.slippage_bps.get(sell_venue, 0.0) / 10_000
                net_profit = sell_notional - sell_fee - sell_slippage - buy_notional - buy_fee - buy_slippage
                net_edge_bps = net_profit / buy_notional * 10_000
                threshold = self.config.minimum_net_edge_bps + self.config.latency_buffer_bps
                if self.config.minimum_edge_bps is not None:
                    threshold = self.config.minimum_edge_bps + self.config.latency_buffer_bps
                if net_edge_bps > threshold and (best is None or net_edge_bps > best[0]):
                    best = (net_edge_bps, quantity, buy_venue, sell_venue, quote_skew_ns)
        if best is None:
            return []
        net_edge_bps, quantity, buy_venue, sell_venue, quote_skew_ns = best
        latency_ns = max(
            self.config.market_data_latency_ns.get(buy_venue, 0) + self.config.order_latency_ns.get(buy_venue, 0),
            self.config.market_data_latency_ns.get(sell_venue, 0) + self.config.order_latency_ns.get(sell_venue, 0),
        )
        self.last_submission_ns = event.local_time_ns
        return [
            OrderIntent(
                buy_venue, "buy", quantity,
                reason=f"net_edge={net_edge_bps:.3f}bps latency={latency_ns / 1e6:.1f}ms skew={quote_skew_ns / 1e6:.1f}ms",
                ttl_ns=self.config.default_order_ttl_ns,
            ),
            OrderIntent(
                sell_venue, "sell", quantity,
                reason=f"net_edge={net_edge_bps:.3f}bps latency={latency_ns / 1e6:.1f}ms skew={quote_skew_ns / 1e6:.1f}ms",
                ttl_ns=self.config.default_order_ttl_ns,
            ),
        ]
