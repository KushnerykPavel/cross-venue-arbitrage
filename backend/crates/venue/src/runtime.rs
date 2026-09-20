use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use domain::{MarketCoin, Venue};
use market_data::{LocalObservationTime, NormalizedMarketEvent};
use tokio::time::{Interval, interval_at, sleep};

use crate::clock::{MonotonicClock, ObservationClock};
use crate::transport::{
    IncomingFrame, OutgoingFrame, Transport, TransportConnection, TungsteniteTransport,
};

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub type ShutdownSignal = Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketKey(usize);

impl MarketKey {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum AdapterAction {
    SendText(String),
    SendPing(Vec<u8>),
    MarketSubscribed(MarketKey),
    TradeDeduplicated(MarketKey),
    Publish(NormalizedMarketEvent),
    Reconnect { reason: String },
    Stop { reason: String },
}

pub trait MarketDataAdapter {
    fn venue(&self) -> Venue;
    fn endpoint(&self) -> &str;
    fn heartbeat_interval(&self) -> Option<Duration> {
        None
    }
    fn on_connected(&mut self) -> Vec<AdapterAction>;
    fn on_heartbeat(&mut self) -> Vec<AdapterAction> {
        Vec::new()
    }
    fn on_text(
        &mut self,
        text: &str,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Vec<AdapterAction>;
    fn on_disconnected(
        &mut self,
        reason: &str,
        observed_at: LocalObservationTime,
    ) -> Vec<AdapterAction>;
    fn market_coin(&self, market: MarketKey) -> Option<&MarketCoin>;
}

pub enum LiveSessionEvent<'a> {
    Connecting {
        venue: Venue,
        endpoint: &'a str,
    },
    Connected {
        venue: Venue,
    },
    MarketSubscribed {
        venue: Venue,
        market_coin: &'a MarketCoin,
    },
    MarketData {
        event: NormalizedMarketEvent,
    },
    TradeDeduplicated {
        venue: Venue,
        market_coin: &'a MarketCoin,
    },
    Disconnected {
        venue: Venue,
        reason: String,
        retry_in: Duration,
    },
}

pub struct LiveMarketDataSession<A> {
    adapter: A,
    clock: MonotonicClock,
}

impl<A> LiveMarketDataSession<A>
where
    A: MarketDataAdapter,
{
    pub fn new(adapter: A, clock: MonotonicClock) -> Self {
        Self { adapter, clock }
    }

    pub async fn run_until<F>(
        &mut self,
        shutdown: ShutdownSignal,
        on_event: F,
    ) -> std::io::Result<()>
    where
        F: for<'event> FnMut(LiveSessionEvent<'event>) -> std::io::Result<()>,
    {
        let _ = rustls::crypto::ring::default_provider().install_default();
        run_with_transport(
            &mut self.adapter,
            &self.clock,
            &mut TungsteniteTransport,
            shutdown,
            on_event,
        )
        .await
    }
}

async fn run_with_transport<A, C, T, F>(
    adapter: &mut A,
    clock: &C,
    transport: &mut T,
    mut shutdown: ShutdownSignal,
    mut on_event: F,
) -> std::io::Result<()>
where
    A: MarketDataAdapter,
    C: ObservationClock,
    T: Transport,
    F: for<'event> FnMut(LiveSessionEvent<'event>) -> std::io::Result<()>,
{
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;

    loop {
        on_event(LiveSessionEvent::Connecting {
            venue: adapter.venue(),
            endpoint: adapter.endpoint(),
        })?;
        let connection = tokio::select! {
            result = transport.connect(adapter.endpoint()) => Some(result),
            result = &mut shutdown => {
                result?;
                None
            }
        };
        let Some(connection) = connection else {
            return Ok(());
        };

        let disconnect_reason = match connection {
            Ok(mut connection) => {
                on_event(LiveSessionEvent::Connected {
                    venue: adapter.venue(),
                })?;
                let initial_actions = adapter.on_connected();
                match execute_actions(
                    adapter,
                    &mut connection,
                    initial_actions,
                    &mut reconnect_delay,
                    &mut on_event,
                )
                .await
                {
                    ActionOutcome::Reconnect(reason) | ActionOutcome::TransportFailure(reason) => {
                        reason
                    }
                    ActionOutcome::Stop(reason) => return Err(std::io::Error::other(reason)),
                    ActionOutcome::Continue => match run_connection(
                        adapter,
                        clock,
                        &mut connection,
                        &mut shutdown,
                        &mut reconnect_delay,
                        &mut on_event,
                    )
                    .await
                    {
                        ConnectionOutcome::Disconnected(reason) => reason,
                        ConnectionOutcome::Shutdown(result) => {
                            result?;
                            return Ok(());
                        }
                        ConnectionOutcome::Stop(reason) => {
                            return Err(std::io::Error::other(reason));
                        }
                    },
                }
            }
            Err(error) => format!("connection failed: {error}"),
        };

        let unavailable = adapter.on_disconnected(&disconnect_reason, clock.now());
        publish_disconnect_actions(unavailable, &mut on_event)?;
        on_event(LiveSessionEvent::Disconnected {
            venue: adapter.venue(),
            reason: disconnect_reason,
            retry_in: reconnect_delay,
        })?;

        let should_stop = tokio::select! {
            () = sleep(reconnect_delay) => false,
            result = &mut shutdown => {
                result?;
                true
            }
        };
        if should_stop {
            return Ok(());
        }
        reconnect_delay = next_reconnect_delay(reconnect_delay);
    }
}

async fn run_connection<A, C, S, F>(
    adapter: &mut A,
    clock: &C,
    connection: &mut S,
    shutdown: &mut ShutdownSignal,
    reconnect_delay: &mut Duration,
    on_event: &mut F,
) -> ConnectionOutcome
where
    A: MarketDataAdapter,
    C: ObservationClock,
    S: TransportConnection,
    F: for<'event> FnMut(LiveSessionEvent<'event>) -> std::io::Result<()>,
{
    let mut heartbeat = heartbeat_interval(adapter.heartbeat_interval());
    loop {
        tokio::select! {
            frame = connection.receive() => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    Ok(None) => return ConnectionOutcome::Disconnected(
                        "connection closed by remote peer".into()
                    ),
                    Err(error) => return ConnectionOutcome::Disconnected(
                        format!("transport receive failed: {error}")
                    ),
                };
                match frame {
                    IncomingFrame::Text(text) => {
                        let local_receive = clock.now();
                        let actions = adapter.on_text(&text, local_receive, clock);
                        match execute_actions(
                            adapter,
                            connection,
                            actions,
                            reconnect_delay,
                            on_event,
                        ).await {
                            ActionOutcome::Reconnect(reason)
                            | ActionOutcome::TransportFailure(reason) => {
                                return ConnectionOutcome::Disconnected(reason);
                            }
                            ActionOutcome::Stop(reason) => {
                                return ConnectionOutcome::Stop(reason);
                            }
                            ActionOutcome::Continue => {}
                        }
                    }
                    IncomingFrame::Ping(payload) => {
                        if let Err(error) = connection.send(OutgoingFrame::Pong(payload)).await {
                            return ConnectionOutcome::Disconnected(
                                format!("pong send failed: {error}")
                            );
                        }
                    }
                    IncomingFrame::Closed(frame) => {
                        return ConnectionOutcome::Disconnected(
                            format!("connection closed: {frame}")
                        );
                    }
                    IncomingFrame::Other => {}
                }
            }
            () = heartbeat_tick(&mut heartbeat) => {
                let actions = adapter.on_heartbeat();
                match execute_actions(
                    adapter,
                    connection,
                    actions,
                    reconnect_delay,
                    on_event,
                ).await {
                    ActionOutcome::Reconnect(reason)
                    | ActionOutcome::TransportFailure(reason) => {
                        return ConnectionOutcome::Disconnected(reason);
                    }
                    ActionOutcome::Stop(reason) => return ConnectionOutcome::Stop(reason),
                    ActionOutcome::Continue => {}
                }
            }
            result = shutdown.as_mut() => {
                connection.close().await;
                return ConnectionOutcome::Shutdown(result);
            }
        }
    }
}

async fn execute_actions<A, S, F>(
    adapter: &A,
    connection: &mut S,
    actions: Vec<AdapterAction>,
    reconnect_delay: &mut Duration,
    on_event: &mut F,
) -> ActionOutcome
where
    A: MarketDataAdapter,
    S: TransportConnection,
    F: for<'event> FnMut(LiveSessionEvent<'event>) -> std::io::Result<()>,
{
    for action in actions {
        match action {
            AdapterAction::SendText(text) => {
                if let Err(error) = connection.send(OutgoingFrame::Text(text)).await {
                    return ActionOutcome::TransportFailure(format!(
                        "transport send failed: {error}"
                    ));
                }
            }
            AdapterAction::SendPing(payload) => {
                if let Err(error) = connection.send(OutgoingFrame::Ping(payload)).await {
                    return ActionOutcome::TransportFailure(format!(
                        "transport send failed: {error}"
                    ));
                }
            }
            AdapterAction::MarketSubscribed(market) => {
                let market_coin = adapter
                    .market_coin(market)
                    .expect("adapter action references a configured market");
                if let Err(error) = on_event(LiveSessionEvent::MarketSubscribed {
                    venue: adapter.venue(),
                    market_coin,
                }) {
                    return ActionOutcome::Stop(error.to_string());
                }
            }
            AdapterAction::TradeDeduplicated(market) => {
                let market_coin = adapter
                    .market_coin(market)
                    .expect("adapter action references a configured market");
                if let Err(error) = on_event(LiveSessionEvent::TradeDeduplicated {
                    venue: adapter.venue(),
                    market_coin,
                }) {
                    return ActionOutcome::Stop(error.to_string());
                }
            }
            AdapterAction::Publish(event) => {
                *reconnect_delay = INITIAL_RECONNECT_DELAY;
                if let Err(error) = on_event(LiveSessionEvent::MarketData { event }) {
                    return ActionOutcome::Stop(error.to_string());
                }
            }
            AdapterAction::Reconnect { reason } => return ActionOutcome::Reconnect(reason),
            AdapterAction::Stop { reason } => return ActionOutcome::Stop(reason),
        }
    }
    ActionOutcome::Continue
}

enum ActionOutcome {
    Continue,
    Reconnect(String),
    TransportFailure(String),
    Stop(String),
}

fn publish_disconnect_actions<F>(
    actions: Vec<AdapterAction>,
    on_event: &mut F,
) -> std::io::Result<()>
where
    F: for<'event> FnMut(LiveSessionEvent<'event>) -> std::io::Result<()>,
{
    for action in actions {
        let AdapterAction::Publish(event) = action else {
            debug_assert!(
                false,
                "disconnect actions must only publish availability events"
            );
            continue;
        };
        on_event(LiveSessionEvent::MarketData { event })?;
    }
    Ok(())
}

enum ConnectionOutcome {
    Disconnected(String),
    Shutdown(std::io::Result<()>),
    Stop(String),
}

fn heartbeat_interval(period: Option<Duration>) -> Option<Interval> {
    period.map(|period| interval_at(tokio::time::Instant::now() + period, period))
}

async fn heartbeat_tick(interval: &mut Option<Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}

fn next_reconnect_delay(current: Duration) -> Duration {
    current.saturating_mul(2).min(MAX_RECONNECT_DELAY)
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::rc::Rc;

    use domain::{MarketCoin, Price, Quantity, Symbol};
    use market_data::{
        BookLevel, EventTimestamps, ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit,
        MarketDataUnavailable, NormalizedMarketEvent, OrderBook, OrderBookSnapshot,
        UnavailabilityCategory,
    };
    use tokio::sync::oneshot;

    use super::*;

    #[derive(Default)]
    struct FakeClock(Cell<u64>);

    impl ObservationClock for FakeClock {
        fn now(&self) -> LocalObservationTime {
            let next = self.0.get() + 1;
            self.0.set(next);
            LocalObservationTime::from_nanos_since_start(next)
        }
    }

    struct FakeAdapter {
        coin: MarketCoin,
        book: OrderBook,
    }

    impl FakeAdapter {
        fn new() -> Self {
            let coin = MarketCoin::try_new("BTC").unwrap();
            Self {
                book: OrderBook::new(Venue::Hyperliquid, Symbol::perpetual(coin.clone())),
                coin,
            }
        }
    }

    impl MarketDataAdapter for FakeAdapter {
        fn venue(&self) -> Venue {
            Venue::Hyperliquid
        }

        fn endpoint(&self) -> &str {
            "wss://example.test/ws"
        }

        fn on_connected(&mut self) -> Vec<AdapterAction> {
            vec![
                AdapterAction::SendText("subscribe".into()),
                AdapterAction::MarketSubscribed(MarketKey::new(0)),
            ]
        }

        fn on_text(
            &mut self,
            text: &str,
            local_receive: LocalObservationTime,
            clock: &dyn ObservationClock,
        ) -> Vec<AdapterAction> {
            if text == "stop" {
                return vec![AdapterAction::Stop {
                    reason: "dedup capacity exceeded".into(),
                }];
            }
            let level = BookLevel::new(
                "100".parse::<Price>().unwrap(),
                "1".parse::<Quantity>().unwrap(),
                None,
            );
            let ask = BookLevel::new(
                "101".parse::<Price>().unwrap(),
                "1".parse::<Quantity>().unwrap(),
                None,
            );
            let snapshot = OrderBookSnapshot::try_new(
                Venue::Hyperliquid,
                Symbol::perpetual(self.coin.clone()),
                None,
                EventTimestamps::new(
                    vec![ExchangeTimeObservation::new(
                        ExchangeTimeKind::EventTime,
                        1,
                        ExchangeTimeUnit::Unknown,
                    )],
                    local_receive,
                    clock.now(),
                ),
                vec![level],
                vec![ask],
            )
            .unwrap();
            self.book.replace(snapshot).unwrap();
            vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(
                    self.book
                        .current()
                        .expect("snapshot was just installed")
                        .clone(),
                ),
            )]
        }

        fn on_disconnected(
            &mut self,
            reason: &str,
            observed_at: LocalObservationTime,
        ) -> Vec<AdapterAction> {
            self.book.mark_unhealthy();
            vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookUnavailable(MarketDataUnavailable::new(
                    Venue::Hyperliquid,
                    self.coin.clone(),
                    observed_at,
                    UnavailabilityCategory::Disconnected,
                    reason,
                )),
            )]
        }

        fn market_coin(&self, market: MarketKey) -> Option<&MarketCoin> {
            (market.index() == 0).then_some(&self.coin)
        }
    }

    struct ScriptedTransport {
        connections: VecDeque<ScriptedConnection>,
        sent: Rc<RefCell<Vec<String>>>,
    }

    impl ScriptedTransport {
        fn new(scripts: Vec<Vec<IncomingFrame>>) -> Self {
            let sent = Rc::new(RefCell::new(Vec::new()));
            Self {
                connections: scripts
                    .into_iter()
                    .map(|frames| ScriptedConnection {
                        frames: frames.into(),
                        sent: Rc::clone(&sent),
                    })
                    .collect(),
                sent,
            }
        }
    }

    impl Transport for ScriptedTransport {
        type Connection = ScriptedConnection;

        async fn connect(&mut self, _endpoint: &str) -> Result<Self::Connection, String> {
            self.connections
                .pop_front()
                .ok_or_else(|| "script exhausted".into())
        }
    }

    struct ScriptedConnection {
        frames: VecDeque<IncomingFrame>,
        sent: Rc<RefCell<Vec<String>>>,
    }

    impl TransportConnection for ScriptedConnection {
        async fn receive(&mut self) -> Result<Option<IncomingFrame>, String> {
            match self.frames.pop_front() {
                Some(frame) => Ok(Some(frame)),
                None => std::future::pending().await,
            }
        }

        async fn send(&mut self, frame: OutgoingFrame) -> Result<(), String> {
            if let OutgoingFrame::Text(text) = frame {
                self.sent.borrow_mut().push(text);
            }
            Ok(())
        }

        async fn close(&mut self) {}
    }

    #[tokio::test(start_paused = true)]
    async fn reconnects_and_invalidates_before_disconnected_event() {
        let mut adapter = FakeAdapter::new();
        let clock = FakeClock::default();
        let mut transport = ScriptedTransport::new(vec![
            vec![IncomingFrame::Closed("first close".into())],
            vec![IncomingFrame::Text("snapshot".into())],
        ]);
        let sent = Rc::clone(&transport.sent);
        let (stop_tx, stop_rx) = oneshot::channel();
        let mut stop_tx = Some(stop_tx);
        let mut events = Vec::new();
        let shutdown: ShutdownSignal = Box::pin(async move {
            stop_rx.await.map_err(std::io::Error::other)?;
            Ok(())
        });

        run_with_transport(&mut adapter, &clock, &mut transport, shutdown, |event| {
            match event {
                LiveSessionEvent::Connecting { .. } => events.push("connecting"),
                LiveSessionEvent::Connected { .. } => events.push("connected"),
                LiveSessionEvent::MarketSubscribed { .. } => events.push("subscribed"),
                LiveSessionEvent::MarketData { event } => match event {
                    NormalizedMarketEvent::OrderBookSnapshot(snapshot) => {
                        events.push("updated");
                        assert!(
                            snapshot.timestamps().processing_completed()
                                > snapshot.timestamps().local_receive()
                        );
                        if let Some(sender) = stop_tx.take() {
                            let _ = sender.send(());
                        }
                    }
                    NormalizedMarketEvent::OrderBookUnavailable(_) => {
                        events.push("unavailable");
                    }
                    _ => panic!("unexpected normalized event"),
                },
                LiveSessionEvent::TradeDeduplicated { .. } => {
                    panic!("fake adapter does not deduplicate trades")
                }
                LiveSessionEvent::Disconnected { .. } => events.push("disconnected"),
            }
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(
            events,
            [
                "connecting",
                "connected",
                "subscribed",
                "unavailable",
                "disconnected",
                "connecting",
                "connected",
                "subscribed",
                "updated",
            ]
        );
        assert_eq!(sent.borrow().as_slice(), ["subscribe", "subscribe"]);
    }

    #[tokio::test(start_paused = true)]
    async fn stop_action_fails_closed_without_reconnecting() {
        let mut adapter = FakeAdapter::new();
        let clock = FakeClock::default();
        let mut transport = ScriptedTransport::new(vec![vec![IncomingFrame::Text("stop".into())]]);
        let shutdown: ShutdownSignal = Box::pin(std::future::pending());
        let error = run_with_transport(&mut adapter, &clock, &mut transport, shutdown, |_| Ok(()))
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "dedup capacity exceeded");
        assert!(transport.connections.is_empty());
    }
}
