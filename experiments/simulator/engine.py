import heapq
from dataclasses import replace

from .config import SimulationConfig
from .models import (
    ExecutionEvent,
    Fill,
    MarketEvent,
    OrderBook,
    OrderIntent,
    OrderRecord,
    PortfolioState,
    SimulationResult,
)


class ExchangeSimulator:
    """Exchange-side order lifecycle and matching model."""

    def __init__(self, config: SimulationConfig):
        self.config = config
        self.books: dict[str, OrderBook] = {}
        self.orders: list[OrderRecord] = []
        self.pending: list[OrderRecord] = []
        self.next_order_id = 1

    def submit(self, intent: OrderIntent, decision_time_ns: int) -> OrderRecord:
        order = OrderRecord(
            order_id=self.next_order_id,
            intent=intent,
            decision_time_ns=decision_time_ns,
            activation_time_ns=decision_time_ns + self.config.order_latency_ns.get(intent.venue, 0),
            remaining_quantity=intent.quantity,
        )
        self.next_order_id += 1
        self.orders.append(order)
        if intent.order_type == "cancel":
            self.cancel(intent.order_id, decision_time_ns)
            order.status = "cancel_requested"
        else:
            self.pending.append(order)
        return order

    def cancel(self, order_id: int | None, time_ns: int) -> ExecutionEvent | None:
        if order_id is None:
            return None
        for order in self.pending:
            if order.order_id == order_id and order.status in {"pending", "partially_filled", "active"}:
                order.status = "cancelled"
                return ExecutionEvent(order_id, order.intent.venue, "cancelled", time_ns)
        return None

    def on_market_event(self, event: MarketEvent) -> tuple[list[Fill], list[ExecutionEvent]]:
        if event.book is not None:
            self.books[event.venue] = event.book
        fills: list[Fill] = []
        reports: list[ExecutionEvent] = []
        for order in self.pending:
            if order.status not in {"pending", "partially_filled", "active"}:
                continue
            if order.status == "partially_filled" and order.intent.order_type == "market":
                continue
            if order.activation_time_ns > event.local_time_ns:
                continue
            if order.intent.ttl_ns is not None and event.local_time_ns > order.decision_time_ns + order.intent.ttl_ns:
                order.status = "expired"
                reports.append(ExecutionEvent(order.order_id, order.intent.venue, "expired", event.local_time_ns))
                continue
            book = self.books.get(order.intent.venue)
            if book is None or event.local_time_ns - book.local_time_ns > self.config.stale_book_ns:
                continue
            fill = self._match(order, book, event)
            if fill is None:
                order.status = "active"
                continue
            fills.append(fill)
            reports.append(ExecutionEvent(order.order_id, order.intent.venue, order.status, event.local_time_ns, fill=fill))
        return fills, reports

    def _match(self, order: OrderRecord, book: OrderBook, event: MarketEvent) -> Fill | None:
        intent = order.intent
        if intent.order_type == "limit" and intent.limit_price is None:
            order.status = "rejected"
            order.rejection_reason = "limit order has no limit_price"
            return None
        levels = book.asks if intent.side == "buy" else book.bids
        if not levels:
            return None
        if intent.order_type == "limit":
            crossed = levels[0].price <= intent.limit_price if intent.side == "buy" else levels[0].price >= intent.limit_price
            if not crossed and event.kind != "trade":
                return None
            if event.kind == "trade" and event.trade is not None:
                trade = event.trade
                touched = trade.price <= intent.limit_price if intent.side == "buy" else trade.price >= intent.limit_price
                if not touched:
                    return None
        remaining = order.remaining_quantity
        gross = 0.0
        filled = 0.0
        for level in levels:
            if intent.order_type == "limit":
                usable = level.price <= intent.limit_price if intent.side == "buy" else level.price >= intent.limit_price
                if not usable:
                    break
            take = min(remaining, level.quantity)
            gross += take * level.price
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break
        if filled <= 0:
            return None
        average = gross / filled
        fee_rate = self.config.taker_fee_bps.get(intent.venue, 0.0) / 10_000
        slip_rate = self.config.slippage_bps.get(intent.venue, 0.0) / 10_000
        fee = gross * fee_rate
        slip = gross * slip_rate
        order.remaining_quantity = remaining
        if remaining <= 1e-12:
            order.status = "filled"
        elif intent.order_type == "market":
            order.status = "partially_filled_ioc"
        else:
            order.status = "partially_filled"
        return Fill(order.order_id, intent.venue, intent.side, filled, average, fee, slip, event.sequence, event.local_time_ns)


class TraderSimulator:
    """Trader-side market-data visibility, strategy, portfolio, and PnL."""

    def __init__(self, config: SimulationConfig, strategy):
        self.config = config
        self.strategy = strategy
        self.exchange = ExchangeSimulator(config)
        self.portfolio = PortfolioState(
            initial_cash={venue: config.initial_cash for venue in config.order_latency_ns},
            cash={venue: config.initial_cash for venue in config.order_latency_ns},
            inventory={venue: 0.0 for venue in config.order_latency_ns},
            average_cost={venue: 0.0 for venue in config.order_latency_ns},
        )
        self.visible_books: dict[str, OrderBook] = {}
        self.market_queue: list[tuple[int, int, MarketEvent]] = []

    def run(self, events: list[MarketEvent]) -> SimulationResult:
        fills: list[Fill] = []
        equity: list[dict] = []
        for event in events:
            event_fills, reports = self.exchange.on_market_event(event)
            fills.extend(event_fills)
            for report in reports:
                self._apply_report(report)
                callback = getattr(self.strategy, "on_execution", None)
                if callback:
                    callback(report, self.portfolio)

            delivery_time = event.local_time_ns + self.config.market_data_latency_ns.get(event.venue, 0)
            heapq.heappush(self.market_queue, (delivery_time, event.sequence, event))
            while self.market_queue and self.market_queue[0][0] <= event.local_time_ns:
                delivered_time, _, source_event = heapq.heappop(self.market_queue)
                visible_event = replace(source_event, local_time_ns=delivered_time)
                if visible_event.book is not None:
                    self.visible_books[visible_event.venue] = visible_event.book
                intents = self.strategy.on_event(visible_event, self.visible_books, self.portfolio)
                for intent in intents:
                    self.exchange.submit(intent, visible_event.local_time_ns)
            equity.append(self._equity(event.local_time_ns))

        equity.append(self._equity(events[-1].local_time_ns if events else 0))
        return SimulationResult(self.exchange.orders, fills, equity, self.portfolio)

    def _apply_report(self, report: ExecutionEvent) -> None:
        if report.fill is None:
            return
        fill = report.fill
        signed_quantity = fill.quantity if fill.side == "buy" else -fill.quantity
        old_position = self.portfolio.inventory[fill.venue]
        old_cost = self.portfolio.average_cost[fill.venue]
        gross = fill.quantity * fill.price
        cash_delta = -(gross + fill.fee + fill.slippage) if signed_quantity > 0 else gross - fill.fee - fill.slippage
        self.portfolio.cash[fill.venue] += cash_delta
        if old_position == 0 or old_position * signed_quantity > 0:
            total = abs(old_position) + abs(signed_quantity)
            self.portfolio.average_cost[fill.venue] = (abs(old_position) * old_cost + abs(signed_quantity) * fill.price) / total
        else:
            closing = min(abs(old_position), abs(signed_quantity))
            self.portfolio.realized_pnl += closing * ((fill.price - old_cost) if old_position > 0 else (old_cost - fill.price))
            new_position = old_position + signed_quantity
            self.portfolio.average_cost[fill.venue] = 0.0 if new_position == 0 else fill.price if old_position * new_position < 0 else old_cost
        self.portfolio.inventory[fill.venue] = old_position + signed_quantity

    def _equity(self, time_ns: int) -> dict:
        value = sum(self.portfolio.cash.values())
        unrealized = 0.0
        for venue, position in self.portfolio.inventory.items():
            book = self.exchange.books.get(venue)
            if not book:
                continue
            mark = book.best_bid.price if position > 0 and book.best_bid else book.best_ask.price if book.best_ask else 0.0
            value += position * mark
            if position:
                unrealized += position * (mark - self.portfolio.average_cost[venue])
        return {"time_ns": time_ns, "equity": value, "realized_pnl": self.portfolio.realized_pnl, "unrealized_pnl": unrealized}


class Simulator:
    """Compatibility facade for the research notebooks and CLI."""

    def __init__(self, config: SimulationConfig):
        self.config = config

    def run(self, events: list[MarketEvent], strategy) -> SimulationResult:
        return TraderSimulator(self.config, strategy).run(events)
