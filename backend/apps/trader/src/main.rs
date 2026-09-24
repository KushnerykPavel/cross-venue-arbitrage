mod config;
mod shutdown;

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use domain::MarketCoin;
use engine::MarketDataEngine;
use recorder::{
    CaptureCoordinator, CaptureMetadata, CaptureSettings, CaptureStatus, ResolvedVenueMarket,
    SegmentLimits,
};
use tokio::sync::watch;
use tokio::task::JoinSet;
use venue::{LiveMarketDataSession, LiveSessionEvent, MonotonicClock, ShutdownSignal};
use venue_aster::AsterAdapter;
use venue_binance::BinanceAdapter;
use venue_hyperliquid::HyperliquidAdapter;
use venue_lighter::LighterAdapter;

use crate::config::TraderConfig;
use crate::shutdown::{ShutdownReason, ShutdownSignals};

type SharedCapture = Arc<Mutex<Option<CaptureCoordinator>>>;
type SharedEngine = Arc<Mutex<MarketDataEngine>>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = TraderConfig::load()?;
    let mut shutdown_signals = ShutdownSignals::install()?;
    println!(
        "Configured markets for all venues: {}",
        configured_markets(&config.market_coins)
    );
    println!("resolving Binance, Aster, and Lighter market metadata");

    let binance =
        BinanceAdapter::bootstrap(config.market_coins.clone(), config.trade_dedup_capacity).await?;
    let aster =
        AsterAdapter::bootstrap(config.market_coins.clone(), config.trade_dedup_capacity).await?;
    let hyperliquid = config
        .enable_hyperliquid
        .then(|| HyperliquidAdapter::new(config.market_coins.clone(), config.trade_dedup_capacity));
    let lighter =
        LighterAdapter::bootstrap(config.market_coins.clone(), config.trade_dedup_capacity).await?;

    let metadata = capture_metadata(&config, &binance, &aster, hyperliquid.as_ref(), &lighter);
    let capture = CaptureCoordinator::start(
        CaptureSettings {
            data_dir: config.data_dir.clone(),
            queue_capacity: config.recorder_queue_capacity,
            segment_limits: SegmentLimits::default(),
            metadata,
        },
        SystemTime::now(),
    )?;
    println!(
        "capture {} writing to {}",
        capture.capture_id(),
        capture.capture_directory().display()
    );
    let capture = Arc::new(Mutex::new(Some(capture)));
    let engine = Arc::new(Mutex::new(MarketDataEngine::new()));

    let clock = MonotonicClock::start();
    let binance = LiveMarketDataSession::new(binance, clock.clone());
    let aster = LiveMarketDataSession::new(aster, clock.clone());
    let lighter = LiveMarketDataSession::new(lighter, clock.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut sessions = JoinSet::new();
    spawn_session(
        &mut sessions,
        "Binance",
        binance,
        shutdown_rx.clone(),
        Arc::clone(&capture),
        Arc::clone(&engine),
    );
    spawn_session(
        &mut sessions,
        "Aster",
        aster,
        shutdown_rx.clone(),
        Arc::clone(&capture),
        Arc::clone(&engine),
    );
    if let Some(hyperliquid) = hyperliquid {
        let hyperliquid = LiveMarketDataSession::new(hyperliquid, clock.clone());
        spawn_session(
            &mut sessions,
            "Hyperliquid",
            hyperliquid,
            shutdown_rx.clone(),
            Arc::clone(&capture),
            Arc::clone(&engine),
        );
    } else {
        println!("Hyperliquid connector disabled by ENABLE_HYPERLIQUID=false");
    }
    spawn_session(
        &mut sessions,
        "Lighter",
        lighter,
        shutdown_rx,
        Arc::clone(&capture),
        Arc::clone(&engine),
    );

    println!(
        "capture will stop after {} seconds; press Ctrl-C or send SIGTERM to stop earlier",
        config.capture_duration.as_secs()
    );
    let mut failure: Option<Box<dyn Error>> = None;
    tokio::select! {
        signal = shutdown_signals.wait() => {
            match signal {
                Ok(ShutdownReason::Interrupt) => println!("interrupt received; shutting down"),
                Ok(ShutdownReason::Terminate) => println!("termination requested; shutting down"),
                Err(error) => {
                    failure = Some(Box::new(error));
                }
            }
        },
        () = tokio::time::sleep(config.capture_duration) => {
            println!("configured capture duration reached");
        },
        completed = sessions.join_next() => {
            failure = match completed {
                Some(Ok((name, Ok(())))) => Some(Box::new(io::Error::other(format!(
                    "{name} session ended unexpectedly"
                )))),
                other => completed_session_failure(other),
            };
        }
    }
    let _ = shutdown_tx.send(true);
    while let Some(completed) = sessions.join_next().await {
        if failure.is_none() {
            failure = completed_session_failure(Some(completed));
        }
    }

    let coordinator = capture
        .lock()
        .map_err(|_| io::Error::other("capture coordinator lock was poisoned"))?
        .take()
        .ok_or_else(|| io::Error::other("capture coordinator was already finalized"))?;
    let manifest = coordinator.finish(CaptureStatus::Complete, SystemTime::now())?;
    println!(
        "capture {} finalized as {:?}: {} events across {} segments",
        manifest.capture_id,
        manifest.capture_status,
        manifest
            .segments
            .last()
            .map_or(0, |segment| segment.last_capture_sequence),
        manifest.segments.len()
    );
    let engine_report = engine
        .lock()
        .map_err(|_| io::Error::other("market-data engine lock was poisoned"))?
        .report();
    println!(
        "engine processed {} events; digest={}",
        engine_report.events_processed, engine_report.event_digest_sha256
    );

    if let Some(error) = failure {
        return Err(error);
    }
    println!("shutdown complete");
    Ok(())
}

fn spawn_session<A>(
    sessions: &mut JoinSet<(&'static str, io::Result<()>)>,
    name: &'static str,
    mut session: LiveMarketDataSession<A>,
    shutdown: watch::Receiver<bool>,
    capture: SharedCapture,
    engine: SharedEngine,
) where
    A: venue::MarketDataAdapter + Send + Sync + 'static,
{
    sessions.spawn(async move {
        let result = session
            .run_until(shutdown_signal(shutdown), move |event| {
                handle_live_event(&capture, &engine, event)
            })
            .await;
        (name, result)
    });
}

fn handle_live_event(
    capture: &SharedCapture,
    engine: &SharedEngine,
    event: LiveSessionEvent<'_>,
) -> io::Result<()> {
    match event {
        LiveSessionEvent::MarketData { event } => {
            let mut capture = capture
                .lock()
                .map_err(|_| io::Error::other("capture coordinator lock was poisoned"))?;
            let accepted = capture
                .as_mut()
                .ok_or_else(|| io::Error::other("capture coordinator is shutting down"))?
                .accept(event)
                .map_err(io::Error::other)?;
            engine
                .lock()
                .map_err(|_| io::Error::other("market-data engine lock was poisoned"))?
                .process(accepted.capture_sequence, &accepted.event)
                .map_err(io::Error::other)?;
        }
        LiveSessionEvent::TradeDeduplicated { .. } => {
            capture
                .lock()
                .map_err(|_| io::Error::other("capture coordinator lock was poisoned"))?
                .as_mut()
                .ok_or_else(|| io::Error::other("capture coordinator is shutting down"))?
                .observe_deduplicated_trade()
                .map_err(io::Error::other)?;
        }
        lifecycle => present_lifecycle(lifecycle),
    }
    Ok(())
}

fn shutdown_signal(mut shutdown: watch::Receiver<bool>) -> ShutdownSignal {
    Box::pin(async move {
        if *shutdown.borrow() {
            return Ok(());
        }
        shutdown.changed().await.map_err(io::Error::other)?;
        Ok(())
    })
}

fn completed_session_failure(
    completed: Option<Result<(&'static str, io::Result<()>), tokio::task::JoinError>>,
) -> Option<Box<dyn Error>> {
    match completed {
        Some(Ok((_, Ok(())))) | None => None,
        Some(Ok((name, Err(error)))) => Some(Box::new(io::Error::other(format!(
            "{name} session stopped: {error}"
        )))),
        Some(Err(error)) => Some(Box::new(io::Error::other(format!(
            "market-data session task failed: {error}"
        )))),
    }
}

fn configured_markets(coins: &[MarketCoin]) -> String {
    coins
        .iter()
        .map(MarketCoin::as_str)
        .collect::<Vec<_>>()
        .join(", ")
}

fn capture_metadata(
    config: &TraderConfig,
    binance: &BinanceAdapter,
    aster: &AsterAdapter,
    hyperliquid: Option<&HyperliquidAdapter>,
    lighter: &LighterAdapter,
) -> CaptureMetadata {
    let mut sanitized_configuration = BTreeMap::new();
    sanitized_configuration.insert(
        "MARKET_COINS".into(),
        config
            .market_coins
            .iter()
            .map(MarketCoin::as_str)
            .collect::<Vec<_>>()
            .join(","),
    );
    sanitized_configuration.insert(
        "ENABLE_HYPERLIQUID".into(),
        config.enable_hyperliquid.to_string(),
    );
    sanitized_configuration.insert(
        "TRADE_DEDUP_CAPACITY".into(),
        config.trade_dedup_capacity.to_string(),
    );
    sanitized_configuration.insert("DATA_DIR".into(), config.data_dir.display().to_string());
    sanitized_configuration.insert(
        "RECORDER_QUEUE_CAPACITY".into(),
        config.recorder_queue_capacity.to_string(),
    );
    sanitized_configuration.insert(
        "CAPTURE_DURATION_SECONDS".into(),
        config.capture_duration.as_secs().to_string(),
    );

    let mut resolved_venue_markets = Vec::new();
    resolved_venue_markets.extend(
        binance
            .resolved_markets()
            .into_iter()
            .map(|(coin, symbol)| ResolvedVenueMarket {
                venue: "binance".into(),
                market_coin: coin.to_string(),
                venue_symbol: Some(symbol),
                venue_market_id: None,
            }),
    );
    resolved_venue_markets.extend(aster.resolved_markets().into_iter().map(|(coin, symbol)| {
        ResolvedVenueMarket {
            venue: "aster".into(),
            market_coin: coin.to_string(),
            venue_symbol: Some(symbol),
            venue_market_id: None,
        }
    }));
    if let Some(hyperliquid) = hyperliquid {
        resolved_venue_markets.extend(hyperliquid.resolved_markets().into_iter().map(|coin| {
            ResolvedVenueMarket {
                venue: "hyperliquid".into(),
                market_coin: coin.to_string(),
                venue_symbol: Some(coin.to_string()),
                venue_market_id: None,
            }
        }));
    }
    resolved_venue_markets.extend(lighter.resolved_markets().into_iter().map(|(coin, id)| {
        ResolvedVenueMarket {
            venue: "lighter".into(),
            market_coin: coin.to_string(),
            venue_symbol: None,
            venue_market_id: Some(id.to_string()),
        }
    }));

    CaptureMetadata {
        sanitized_configuration,
        git_commit: option_env!("GIT_COMMIT").unwrap_or("unknown").into(),
        dirty_build: option_env!("GIT_DIRTY").is_some_and(|value| value == "true"),
        package_version: env!("CARGO_PKG_VERSION").into(),
        rust_target: env!("BUILD_TARGET").into(),
        collector_label: env::var("COLLECTOR_LABEL")
            .or_else(|_| env::var("HOSTNAME"))
            .unwrap_or_else(|_| "unknown".into()),
        configured_markets: config
            .market_coins
            .iter()
            .map(ToString::to_string)
            .collect(),
        resolved_venue_markets,
    }
}

fn present_lifecycle(event: LiveSessionEvent<'_>) {
    match event {
        LiveSessionEvent::Connecting { venue, endpoint } => {
            println!("{venue} connecting to {endpoint}")
        }
        LiveSessionEvent::Connected { venue } => println!("{venue} connected"),
        LiveSessionEvent::MarketSubscribed { venue, market_coin } => {
            println!("{venue} subscribed to {market_coin}")
        }
        LiveSessionEvent::Disconnected {
            venue,
            reason,
            retry_in,
        } => eprintln!(
            "{venue} disconnected ({reason}); reconnecting in {} seconds",
            retry_in.as_secs()
        ),
        LiveSessionEvent::MarketData { .. } => {
            unreachable!("market data is handled before lifecycle presentation")
        }
        LiveSessionEvent::TradeDeduplicated { .. } => {
            unreachable!("deduplication is handled before lifecycle presentation")
        }
    }
}
