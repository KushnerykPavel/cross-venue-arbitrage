import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

Side = Literal["buy", "sell"]


@dataclass(frozen=True)
class Level:
    price: float
    quantity: float


@dataclass
class OrderBook:
    venue: str
    sequence: int
    local_time_ns: int
    bids: list[Level] = field(default_factory=list)
    asks: list[Level] = field(default_factory=list)

    @property
    def best_bid(self) -> Level | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Level | None:
        return self.asks[0] if self.asks else None


@dataclass(frozen=True)
class MarketTrade:
    venue: str
    sequence: int
    local_time_ns: int
    price: float
    quantity: float
    aggressor_side: str


@dataclass(frozen=True)
class MarketEvent:
    sequence: int
    local_time_ns: int
    kind: str
    venue: str
    book: OrderBook | None = None
    trade: MarketTrade | None = None
    availability_transition: str | None = None


@dataclass(frozen=True)
class OrderIntent:
    venue: str
    side: Side
    quantity: float = 0.0
    order_type: str = "market"
    limit_price: float | None = None
    reason: str = ""
    order_id: int | None = None
    ttl_ns: int | None = None


@dataclass
class OrderRecord:
    order_id: int
    intent: OrderIntent
    decision_time_ns: int
    activation_time_ns: int
    remaining_quantity: float = 0.0
    status: str = "pending"
    rejection_reason: str | None = None


@dataclass(frozen=True)
class Fill:
    order_id: int
    venue: str
    side: Side
    quantity: float
    price: float
    fee: float
    slippage: float
    sequence: int
    time_ns: int


@dataclass(frozen=True)
class ExecutionEvent:
    order_id: int
    venue: str
    status: str
    time_ns: int
    fill: Fill | None = None
    reason: str | None = None


@dataclass
class PortfolioState:
    initial_cash: dict[str, float] = field(default_factory=dict)
    cash: dict[str, float] = field(default_factory=dict)
    inventory: dict[str, float] = field(default_factory=dict)
    average_cost: dict[str, float] = field(default_factory=dict)
    realized_pnl: float = 0.0

    def position(self, venue: str) -> float:
        return self.inventory.get(venue, 0.0)


@dataclass
class SimulationResult:
    orders: list[OrderRecord]
    fills: list[Fill]
    equity: list[dict[str, float | int]]
    final_portfolio: PortfolioState

    def summary(self) -> dict[str, float | int]:
        fees = sum(fill.fee for fill in self.fills)
        slippage = sum(fill.slippage for fill in self.fills)
        final_equity = self.equity[-1]["equity"] if self.equity else 0.0
        initial_equity = sum(self.final_portfolio.initial_cash.values())
        unrealized = self.equity[-1].get("unrealized_pnl", 0.0) if self.equity else 0.0
        return {
            "orders": len(self.orders),
            "fills": len(self.fills),
            "fees": fees,
            "slippage": slippage,
            "realized_pnl": self.final_portfolio.realized_pnl,
            "unrealized_pnl": unrealized,
            "net_pnl": final_equity - initial_equity,
            "final_equity": final_equity,
        }

    def write(self, output_dir: str | Path) -> None:
        """Write research outputs without changing the source dataset."""
        import polars as pl

        destination = Path(output_dir)
        destination.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([
            {
                "order_id": order.order_id,
                "venue": order.intent.venue,
                "side": order.intent.side,
                "quantity": order.intent.quantity,
                "order_type": order.intent.order_type,
                "decision_time_ns": order.decision_time_ns,
                "activation_time_ns": order.activation_time_ns,
                "status": order.status,
                "rejection_reason": order.rejection_reason,
                "reason": order.intent.reason,
            }
            for order in self.orders
        ]).write_parquet(destination / "orders.parquet")
        pl.DataFrame([fill.__dict__ for fill in self.fills]).write_parquet(destination / "fills.parquet")
        pl.DataFrame(self.equity).write_parquet(destination / "equity.parquet")
        (destination / "inventory.json").write_text(json.dumps({
            "cash": self.final_portfolio.cash,
            "inventory": self.final_portfolio.inventory,
            "average_cost": self.final_portfolio.average_cost,
            "realized_pnl": self.final_portfolio.realized_pnl,
        }, indent=2))
        (destination / "summary.json").write_text(json.dumps(self.summary(), indent=2))
