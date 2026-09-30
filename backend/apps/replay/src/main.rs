use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::io;
use std::path::PathBuf;

use engine::{EngineReport, MarketDataEngine};
use recorder::{ValidatedCapture, ValidationOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = ReplayArguments::parse(env::args().skip(1))?;
    let capture = ValidatedCapture::open(
        &arguments.capture_directory,
        ValidationOptions {
            allow_incomplete: arguments.allow_incomplete,
        },
    )?;
    let manifest = capture.manifest().clone();
    let resolved_configuration =
        resolved_configuration(&manifest.configuration, &arguments.configuration_overrides);

    println!(
        "validated capture {} ({:?}), {} segments and {} events",
        manifest.capture_id,
        manifest.capture_status,
        manifest.segments.len(),
        capture.event_count()
    );
    if !arguments.configuration_overrides.is_empty() {
        println!(
            "explicit configuration overrides: {}",
            arguments
                .configuration_overrides
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    let report = replay(&capture)?;
    println!(
        "replay complete: events={} last_sequence={:?} digest={}",
        report.events_processed, report.last_capture_sequence, report.event_digest_sha256
    );
    println!(
        "resolved configuration entries={}",
        resolved_configuration.len()
    );
    for book in report.order_books {
        println!(
            "{} {} status={:?} source_sequence={:?} best_bid={:?} best_ask={:?}",
            book.venue,
            book.market_coin,
            book.status,
            book.source_sequence,
            book.best_bid,
            book.best_ask
        );
    }
    Ok(())
}

fn replay(capture: &ValidatedCapture) -> Result<EngineReport, Box<dyn Error>> {
    let mut engine = MarketDataEngine::new();
    let mut engine_error = None;
    capture.for_each_event(|event| {
        if engine_error.is_none() {
            engine_error = engine.process(event.capture_sequence, &event.event).err();
        }
    })?;
    if let Some(error) = engine_error {
        return Err(Box::new(error));
    }
    Ok(engine.report())
}

#[derive(Debug, Eq, PartialEq)]
struct ReplayArguments {
    capture_directory: PathBuf,
    allow_incomplete: bool,
    configuration_overrides: BTreeMap<String, String>,
}

impl ReplayArguments {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Self, io::Error> {
        let mut capture_directory = None;
        let mut allow_incomplete = false;
        let mut configuration_overrides = BTreeMap::new();
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--allow-incomplete" => allow_incomplete = true,
                "--config-override" => {
                    let assignment = arguments.next().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "--config-override requires KEY=VALUE",
                        )
                    })?;
                    let (key, value) = assignment.split_once('=').ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "--config-override requires KEY=VALUE",
                        )
                    })?;
                    if key.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "configuration override key cannot be empty",
                        ));
                    }
                    if configuration_overrides
                        .insert(key.to_owned(), value.to_owned())
                        .is_some()
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("duplicate configuration override: {key}"),
                        ));
                    }
                }
                value if value.starts_with('-') => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown option: {value}"),
                    ));
                }
                value if capture_directory.is_none() => {
                    capture_directory = Some(PathBuf::from(value));
                }
                value => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unexpected argument: {value}"),
                    ));
                }
            }
        }
        Ok(Self {
            capture_directory: capture_directory.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "usage: replay <capture-directory> [--allow-incomplete] [--config-override KEY=VALUE]",
                )
            })?,
            allow_incomplete,
            configuration_overrides,
        })
    }
}

fn resolved_configuration(
    captured: &BTreeMap<String, String>,
    overrides: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut resolved = captured.clone();
    resolved.extend(overrides.clone());
    resolved
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::str::FromStr;
    use std::time::{Duration, UNIX_EPOCH};

    use domain::{MarketCoin, Price, Quantity, Symbol, Venue};
    use market_data::{
        AggressorSide, AggressorSideClassification, BookLevel, EventTimestamps, ExchangeTimeKind,
        ExchangeTimeObservation, ExchangeTimeUnit, LocalObservationTime, MarketDataUnavailable,
        MarketTrade, MarketTradeIdentity, MarketTradeKind, MarketTradeReportingKind,
        NormalizedMarketEvent, OrderBookSnapshot, UnavailabilityCategory,
    };
    use recorder::{
        CaptureCoordinator, CaptureMetadata, CaptureSettings, CaptureStatus, SegmentLimits,
    };

    use super::*;

    fn timestamps(nanos: u64) -> EventTimestamps {
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::EventTime,
                nanos,
                ExchangeTimeUnit::Milliseconds,
            )],
            LocalObservationTime::from_nanos_since_start(nanos),
            LocalObservationTime::from_nanos_since_start(nanos + 1),
        )
    }

    fn snapshot(venue: Venue, sequence: u64, best_bid: i64, depth: i64) -> NormalizedMarketEvent {
        let level = |price: i64| {
            BookLevel::new(
                Price::from_str(&price.to_string()).unwrap(),
                Quantity::from_str("1.5").unwrap(),
                None,
            )
        };
        NormalizedMarketEvent::OrderBookSnapshot(
            OrderBookSnapshot::try_new(
                venue,
                Symbol::perpetual(MarketCoin::try_new("BTC").unwrap()),
                Some(sequence),
                timestamps(sequence),
                (0..depth).map(|offset| level(best_bid - offset)).collect(),
                (0..depth)
                    .map(|offset| level(best_bid + 1 + offset))
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn trade(venue: Venue, id: u64, identity: MarketTradeIdentity) -> NormalizedMarketEvent {
        NormalizedMarketEvent::MarketTrade(MarketTrade::new(
            venue,
            Symbol::perpetual(MarketCoin::try_new("BTC").unwrap()),
            timestamps(id),
            Price::from_str("100.5").unwrap(),
            Quantity::from_str("0.01").unwrap(),
            MarketTradeReportingKind::TakerOrderAggregate,
            MarketTradeKind::Regular,
            AggressorSide::Buy,
            AggressorSideClassification::DerivedFromMakerSide,
            identity,
        ))
    }

    #[test]
    fn replay_is_deterministic_and_ends_on_the_last_recorded_books() {
        let data_dir = tempfile::TempDir::new().unwrap();
        let started_at = UNIX_EPOCH + Duration::from_secs(1);
        let mut capture = CaptureCoordinator::start(
            CaptureSettings {
                data_dir: data_dir.path().to_path_buf(),
                queue_capacity: NonZeroUsize::new(64).unwrap(),
                segment_limits: SegmentLimits::default(),
                metadata: CaptureMetadata {
                    sanitized_configuration: BTreeMap::new(),
                    git_commit: "test".into(),
                    dirty_build: false,
                    package_version: "test".into(),
                    rust_target: "test".into(),
                    collector_label: "test".into(),
                    configured_markets: vec!["BTC".into()],
                    resolved_venue_markets: Vec::new(),
                },
            },
            started_at,
        )
        .unwrap();
        let directory = capture.capture_directory().to_path_buf();
        let events = [
            snapshot(Venue::Binance, 10, 100, 10),
            snapshot(Venue::Aster, 11, 101, 10),
            snapshot(Venue::Lighter, 12, 102, 12),
            trade(
                Venue::Binance,
                13,
                MarketTradeIdentity::Binance {
                    aggregate_trade_id: 1,
                    first_trade_id: 2,
                    last_trade_id: 3,
                },
            ),
            trade(
                Venue::Aster,
                14,
                MarketTradeIdentity::Aster {
                    aggregate_trade_id: 4,
                    first_trade_id: 5,
                    last_trade_id: 6,
                },
            ),
            trade(
                Venue::Lighter,
                15,
                MarketTradeIdentity::Lighter {
                    market_id: 1,
                    trade_id: "7".into(),
                    message_nonce: Some(8),
                },
            ),
            NormalizedMarketEvent::OrderBookUnavailable(MarketDataUnavailable::new(
                Venue::Aster,
                MarketCoin::try_new("BTC").unwrap(),
                LocalObservationTime::from_nanos_since_start(16),
                UnavailabilityCategory::Disconnected,
                "fixture",
            )),
            snapshot(Venue::Binance, 17, 103, 10),
            snapshot(Venue::Lighter, 18, 99, 12),
        ];
        for event in events {
            capture.accept(event).unwrap();
        }
        capture
            .finish(CaptureStatus::Complete, started_at + Duration::from_secs(1))
            .unwrap();

        let validated = ValidatedCapture::open(&directory, ValidationOptions::default()).unwrap();
        let first = replay(&validated).unwrap();
        let second =
            replay(&ValidatedCapture::open(&directory, ValidationOptions::default()).unwrap())
                .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.events_processed, 9);

        let mut last_recorded = HashMap::new();
        validated
            .for_each_event(|event| {
                if let NormalizedMarketEvent::OrderBookSnapshot(snapshot) = event.event {
                    last_recorded.insert(snapshot.venue(), snapshot);
                }
            })
            .unwrap();
        assert_eq!(last_recorded[&Venue::Lighter].bids().len(), 10);
        assert_eq!(first.order_books.len(), 3);
        for book in &first.order_books {
            let recorded = &last_recorded[&book.venue];
            assert_eq!(book.source_sequence, recorded.source_sequence());
            assert_eq!(book.best_bid, Some(recorded.bids()[0].price().to_string()));
            assert_eq!(book.best_ask, Some(recorded.asks()[0].price().to_string()));
        }
    }

    #[test]
    fn arguments_require_explicit_incomplete_and_configuration_overrides() {
        let parsed = ReplayArguments::parse([
            "/data/capture".into(),
            "--allow-incomplete".into(),
            "--config-override".into(),
            "MODE=research".into(),
        ])
        .unwrap();

        assert!(parsed.allow_incomplete);
        assert_eq!(parsed.capture_directory, PathBuf::from("/data/capture"));
        assert_eq!(
            parsed
                .configuration_overrides
                .get("MODE")
                .map(String::as_str),
            Some("research")
        );
    }

    #[test]
    fn rejects_implicit_or_duplicate_overrides() {
        assert!(ReplayArguments::parse(["/data/capture".into(), "MODE=research".into()]).is_err());
        assert!(
            ReplayArguments::parse([
                "/data/capture".into(),
                "--config-override".into(),
                "MODE=a".into(),
                "--config-override".into(),
                "MODE=b".into(),
            ])
            .is_err()
        );
    }
}
