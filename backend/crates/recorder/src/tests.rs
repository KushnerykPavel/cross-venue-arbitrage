use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::time::{Duration, UNIX_EPOCH};

use domain::{MarketCoin, Price, Quantity, Symbol, Venue};
use market_data::{
    AggressorSide, AggressorSideClassification, BestBidOffer, BookLevel, EventTimestamps,
    ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit, LocalObservationTime, MarketTrade,
    MarketTradeIdentity, MarketTradeKind, MarketTradeReportingKind, NormalizedMarketEvent,
    OrderBookSnapshot, TradeStreamResumed,
};
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::format::{HEADER_LENGTH_U64, encode_record};

fn capture_id() -> Uuid {
    Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap()
}

fn stored_event(sequence: u64) -> StoredEventV1 {
    StoredEventV1::from_normalized(
        sequence,
        &NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
            Venue::Aster,
            MarketCoin::try_new("BTC").unwrap(),
            LocalObservationTime::from_nanos_since_start(10),
        )),
    )
    .unwrap()
}

fn capture_metadata() -> CaptureMetadata {
    CaptureMetadata {
        sanitized_configuration: BTreeMap::from([("MARKET_COINS".into(), "BTC".into())]),
        git_commit: "test".into(),
        dirty_build: false,
        package_version: "0.1.0".into(),
        rust_target: "test-target".into(),
        collector_label: "test".into(),
        configured_markets: vec!["BTC".into()],
        resolved_venue_markets: Vec::new(),
    }
}

fn limits(max_duration: Duration, max_bytes: u64) -> SegmentLimits {
    SegmentLimits::new(
        max_duration,
        NonZeroU64::new(max_bytes).unwrap(),
        Duration::from_secs(1),
    )
}

#[test]
fn stored_event_v1_has_stable_golden_postcard_bytes() {
    let bytes = postcard::to_allocvec(&stored_event(1)).unwrap();

    assert_eq!(bytes, [1, 0, 3, b'B', b'T', b'C', 10, 10, 0, 0, 4]);
}

#[test]
fn converts_snapshot_without_losing_exact_decimals_or_source_sequence() {
    let coin = MarketCoin::try_new("BTC").unwrap();
    let timestamps = EventTimestamps::new(
        vec![ExchangeTimeObservation::new(
            ExchangeTimeKind::EventTime,
            1_725_000_000_100,
            ExchangeTimeUnit::Milliseconds,
        )],
        LocalObservationTime::from_nanos_since_start(10),
        LocalObservationTime::from_nanos_since_start(11),
    );
    let snapshot = OrderBookSnapshot::try_new(
        Venue::Aster,
        Symbol::perpetual(coin),
        Some(99),
        timestamps,
        vec![BookLevel::new(
            Price::from_str("63250.125").unwrap(),
            Quantity::from_str("0.0040").unwrap(),
            Some(NonZeroU32::new(2).unwrap()),
        )],
        vec![BookLevel::new(
            Price::from_str("63251").unwrap(),
            Quantity::from_str("1").unwrap(),
            None,
        )],
    )
    .unwrap();
    let stored =
        StoredEventV1::from_normalized(1, &NormalizedMarketEvent::OrderBookSnapshot(snapshot))
            .unwrap();

    assert_eq!(
        stored.source_id(),
        Some(&StoredSourceIdV1::OrderBookSequence(99))
    );
    let StoredPayloadV1::OrderBookSnapshot { bids, .. } = stored.payload() else {
        panic!("expected stored Order Book")
    };
    assert_eq!(
        bids[0].price,
        StoredDecimalV1 {
            coefficient: 63_250_125,
            scale: 3,
        }
    );
    assert_eq!(
        bids[0].quantity,
        StoredDecimalV1 {
            coefficient: 4,
            scale: 3,
        }
    );
    assert_eq!(bids[0].order_count, Some(2));
}

#[test]
fn truncates_lighter_snapshots_to_top_ten_only_when_storing() {
    let coin = MarketCoin::try_new("BTC").unwrap();
    let bids = (0..12)
        .map(|index| {
            BookLevel::new(
                Price::from_str(&(100 - index).to_string()).unwrap(),
                Quantity::from_str("1").unwrap(),
                None,
            )
        })
        .collect::<Vec<_>>();
    let asks = (0..12)
        .map(|index| {
            BookLevel::new(
                Price::from_str(&(101 + index).to_string()).unwrap(),
                Quantity::from_str("1").unwrap(),
                None,
            )
        })
        .collect::<Vec<_>>();
    let snapshot = OrderBookSnapshot::try_new(
        Venue::Lighter,
        Symbol::perpetual(coin),
        Some(99),
        EventTimestamps::new(
            Vec::new(),
            LocalObservationTime::from_nanos_since_start(10),
            LocalObservationTime::from_nanos_since_start(11),
        ),
        bids,
        asks,
    )
    .unwrap();

    let stored =
        StoredEventV1::from_normalized(1, &NormalizedMarketEvent::OrderBookSnapshot(snapshot))
            .unwrap();

    let StoredPayloadV1::OrderBookSnapshot { bids, asks } = stored.payload() else {
        panic!("expected stored Order Book")
    };
    assert_eq!(bids.len(), 10);
    assert_eq!(asks.len(), 10);
    assert_eq!(bids[0].price.coefficient, 100);
    assert_eq!(asks[0].price.coefficient, 101);
}

#[test]
fn converts_typed_trade_identity_and_exchange_times() {
    let coin = MarketCoin::try_new("BTC").unwrap();
    let trade = MarketTrade::new(
        Venue::Hyperliquid,
        Symbol::perpetual(coin),
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::BlockTime,
                100,
                ExchangeTimeUnit::Unknown,
            )],
            LocalObservationTime::from_nanos_since_start(10),
            LocalObservationTime::from_nanos_since_start(12),
        ),
        Price::from_str("100.5").unwrap(),
        Quantity::from_str("2").unwrap(),
        MarketTradeReportingKind::Individual,
        MarketTradeKind::Liquidation,
        AggressorSide::Sell,
        AggressorSideClassification::VenueProvided,
        MarketTradeIdentity::Hyperliquid {
            block_time: 100,
            trade_id: 7,
            transaction_hash: "0xabc".into(),
        },
    );
    let normalized = NormalizedMarketEvent::MarketTrade(trade);
    let stored = StoredEventV1::from_normalized(1, &normalized).unwrap();

    assert_eq!(
        stored.source_id(),
        Some(&StoredSourceIdV1::HyperliquidTrade {
            block_time: 100,
            trade_id: 7,
            transaction_hash: "0xabc".into(),
        })
    );
    assert_eq!(
        stored.exchange_times()[0].declared_unit,
        StoredExchangeTimeUnitV1::Unknown
    );
    assert_eq!(stored.to_normalized().unwrap(), normalized);
}

#[test]
fn storage_dto_round_trips_to_the_same_normalized_event() {
    let original = NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
        Venue::Lighter,
        MarketCoin::try_new("BTC").unwrap(),
        LocalObservationTime::from_nanos_since_start(42),
    ));
    let stored = StoredEventV1::from_normalized(1, &original).unwrap();

    assert_eq!(stored.to_normalized().unwrap(), original);
}

#[test]
fn bbo_storage_round_trip_preserves_nullable_sides_and_timestamps() {
    let coin = MarketCoin::try_new("BTC").unwrap();
    let original = NormalizedMarketEvent::BestBidOfferUpdated(BestBidOffer::new(
        Venue::Hyperliquid,
        Symbol::perpetual(coin),
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::EventTime,
                123,
                ExchangeTimeUnit::Unknown,
            )],
            LocalObservationTime::from_nanos_since_start(10),
            LocalObservationTime::from_nanos_since_start(11),
        ),
        None,
        Some(BookLevel::new(
            Price::from_str("101").unwrap(),
            Quantity::from_str("2").unwrap(),
            Some(NonZeroU32::new(3).unwrap()),
        )),
    ));
    let stored = StoredEventV1::from_normalized(1, &original).unwrap();

    assert_eq!(stored.to_normalized().unwrap(), original);
}

#[test]
fn validates_entire_capture_before_exposing_replay_events() {
    let data_dir = TempDir::new().unwrap();
    let started_at = UNIX_EPOCH + Duration::from_secs(1);
    let mut coordinator = CaptureCoordinator::start(
        CaptureSettings {
            data_dir: data_dir.path().to_path_buf(),
            queue_capacity: NonZeroUsize::new(4).unwrap(),
            segment_limits: SegmentLimits::default(),
            metadata: capture_metadata(),
        },
        started_at,
    )
    .unwrap();
    let capture_directory = coordinator.capture_directory().to_path_buf();
    coordinator
        .accept(NormalizedMarketEvent::TradeStreamResumed(
            TradeStreamResumed::new(
                Venue::Aster,
                MarketCoin::try_new("BTC").unwrap(),
                LocalObservationTime::from_nanos_since_start(10),
            ),
        ))
        .unwrap();
    coordinator
        .finish(CaptureStatus::Complete, started_at + Duration::from_secs(1))
        .unwrap();

    let validated =
        ValidatedCapture::open(&capture_directory, ValidationOptions::default()).unwrap();
    assert_eq!(validated.event_count(), 1);
    let mut sequences = Vec::new();
    validated
        .for_each_event(|event| sequences.push(event.capture_sequence))
        .unwrap();
    assert_eq!(sequences, [1]);

    fs::write(capture_directory.join("segment-999999.log"), b"unlisted").unwrap();
    assert!(matches!(
        ValidatedCapture::open(&capture_directory, ValidationOptions::default()),
        Err(CaptureValidationError::UnlistedLogFile(_))
    ));
}

#[test]
fn incomplete_capture_requires_explicit_opt_in() {
    let data_dir = TempDir::new().unwrap();
    let started_at = UNIX_EPOCH + Duration::from_secs(1);
    let coordinator = CaptureCoordinator::start(
        CaptureSettings {
            data_dir: data_dir.path().to_path_buf(),
            queue_capacity: NonZeroUsize::new(1).unwrap(),
            segment_limits: SegmentLimits::default(),
            metadata: capture_metadata(),
        },
        started_at,
    )
    .unwrap();
    let capture_directory = coordinator.capture_directory().to_path_buf();
    coordinator
        .finish(
            CaptureStatus::IncompleteQueueFull,
            started_at + Duration::from_secs(1),
        )
        .unwrap();

    assert!(matches!(
        ValidatedCapture::open(&capture_directory, ValidationOptions::default()),
        Err(CaptureValidationError::IncompleteCapture(
            CaptureStatus::IncompleteQueueFull
        ))
    ));
    assert!(
        ValidatedCapture::open(
            capture_directory,
            ValidationOptions {
                allow_incomplete: true
            }
        )
        .is_ok()
    );
}

#[test]
fn writes_fixed_header_and_round_trips_records() {
    let directory = TempDir::new().unwrap();
    let start = UNIX_EPOCH + Duration::from_secs(1);
    let mut writer = SegmentWriter::create(
        directory.path(),
        capture_id(),
        limits(Duration::from_secs(300), 1024 * 1024),
    )
    .unwrap();
    writer.append(&stored_event(1), start).unwrap();
    writer
        .append(&stored_event(2), start + Duration::from_millis(1))
        .unwrap();
    let finalized = writer
        .finish(start + Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let path = directory.path().join(&finalized.filename);
    let mut bytes = Vec::new();
    fs::File::open(&path)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();

    assert_eq!(&bytes[..8], b"CVARLOG\0");
    assert_eq!(&bytes[8..10], &1_u16.to_le_bytes());
    assert_eq!(&bytes[10..12], &40_u16.to_le_bytes());
    assert_eq!(&bytes[12..28], capture_id().as_bytes());
    assert_eq!(&bytes[28..32], &0_u32.to_le_bytes());
    assert_eq!(&bytes[32..40], &1000_u64.to_le_bytes());

    let read = read_segment(&path, capture_id(), 0).unwrap();
    assert_eq!(read.events, [stored_event(1), stored_event(2)]);
    assert_eq!(read.sha256, finalized.sha256);
    assert_eq!(finalized.first_capture_sequence, 1);
    assert_eq!(finalized.last_capture_sequence, 2);
    assert_eq!(finalized.record_count, 2);
}

#[test]
fn rejects_crc_corruption_and_unknown_format_version() {
    let directory = TempDir::new().unwrap();
    let start = UNIX_EPOCH + Duration::from_secs(1);
    let mut writer =
        SegmentWriter::create(directory.path(), capture_id(), SegmentLimits::default()).unwrap();
    writer.append(&stored_event(1), start).unwrap();
    let finalized = writer.finish(start).unwrap().unwrap();
    let path = directory.path().join(finalized.filename);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(HEADER_LENGTH_U64 + 4)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_data().unwrap();
    assert!(matches!(
        read_segment(&path, capture_id(), 0),
        Err(SegmentReadError::CorruptRecord(_))
    ));

    file.seek(SeekFrom::Start(8)).unwrap();
    file.write_all(&2_u16.to_le_bytes()).unwrap();
    file.sync_data().unwrap();
    assert!(matches!(
        read_segment(&path, capture_id(), 0),
        Err(SegmentReadError::InvalidHeader(_))
    ));
}

#[test]
fn rotates_on_time_and_size_limits() {
    let start = UNIX_EPOCH + Duration::from_secs(1);
    let time_directory = TempDir::new().unwrap();
    let mut time_writer = SegmentWriter::create(
        time_directory.path(),
        capture_id(),
        limits(Duration::from_secs(5), 1024 * 1024),
    )
    .unwrap();
    assert!(
        time_writer
            .append(&stored_event(1), start)
            .unwrap()
            .is_none()
    );
    let first = time_writer
        .append(&stored_event(2), start + Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let second = time_writer
        .finish(start + Duration::from_secs(6))
        .unwrap()
        .unwrap();
    assert_eq!((first.index, second.index), (0, 1));

    let record_length = u64::try_from(encode_record(&stored_event(1)).unwrap().len()).unwrap();
    let size_directory = TempDir::new().unwrap();
    let mut size_writer = SegmentWriter::create(
        size_directory.path(),
        capture_id(),
        limits(Duration::from_secs(300), HEADER_LENGTH_U64 + record_length),
    )
    .unwrap();
    size_writer.append(&stored_event(1), start).unwrap();
    assert!(
        size_writer
            .append(&stored_event(2), start)
            .unwrap()
            .is_some()
    );
}

#[test]
fn rejects_non_contiguous_capture_sequence() {
    let directory = TempDir::new().unwrap();
    let mut writer =
        SegmentWriter::create(directory.path(), capture_id(), SegmentLimits::default()).unwrap();
    let error = writer
        .append(&stored_event(2), UNIX_EPOCH + Duration::from_secs(1))
        .unwrap_err();
    assert!(matches!(
        error,
        SegmentWriterError::SequenceMismatch {
            expected: 1,
            actual: 2
        }
    ));
}

#[test]
fn recovery_truncates_invalid_tail_and_publishes_valid_prefix() {
    let directory = TempDir::new().unwrap();
    let start = UNIX_EPOCH + Duration::from_secs(1);
    let mut writer =
        SegmentWriter::create(directory.path(), capture_id(), SegmentLimits::default()).unwrap();
    writer.append(&stored_event(1), start).unwrap();
    drop(writer);

    let open_path = directory.path().join("segment-000000.open");
    let valid_length = fs::metadata(&open_path).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&open_path).unwrap();
    file.write_all(&[4, 0, 0]).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let outcome =
        recover_open_segment(&open_path, capture_id(), 0, start + Duration::from_secs(2)).unwrap();
    let RecoveryOutcome::Finalized {
        segment,
        truncated_bytes,
    } = outcome
    else {
        panic!("expected recovered segment")
    };
    assert_eq!(truncated_bytes, 3);
    assert_eq!(segment.byte_size, valid_length);
    assert_eq!(segment.completion, SegmentCompletion::RecoveredAfterCrash);
    assert!(!open_path.exists());
    let read = read_segment(&directory.path().join(segment.filename), capture_id(), 0).unwrap();
    assert_eq!(read.events, [stored_event(1)]);
}

#[test]
fn recovery_removes_crc_invalid_record_and_everything_after_it() {
    let directory = TempDir::new().unwrap();
    let start = UNIX_EPOCH + Duration::from_secs(1);
    let first_record_length =
        u64::try_from(encode_record(&stored_event(1)).unwrap().len()).unwrap();
    let mut writer =
        SegmentWriter::create(directory.path(), capture_id(), SegmentLimits::default()).unwrap();
    writer.append(&stored_event(1), start).unwrap();
    writer.append(&stored_event(2), start).unwrap();
    drop(writer);

    let open_path = directory.path().join("segment-000000.open");
    let second_payload_offset = HEADER_LENGTH_U64 + first_record_length + 4;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&open_path)
        .unwrap();
    file.seek(SeekFrom::Start(second_payload_offset)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let outcome = recover_open_segment(&open_path, capture_id(), 0, start).unwrap();
    let RecoveryOutcome::Finalized { segment, .. } = outcome else {
        panic!("expected recovered segment")
    };
    let read = read_segment(&directory.path().join(segment.filename), capture_id(), 0).unwrap();
    assert_eq!(read.events, [stored_event(1)]);
}

#[test]
fn recovery_discards_header_only_open_segment() {
    let directory = TempDir::new().unwrap();
    let start = UNIX_EPOCH + Duration::from_secs(1);
    let mut writer =
        SegmentWriter::create(directory.path(), capture_id(), SegmentLimits::default()).unwrap();
    writer.append(&stored_event(1), start).unwrap();
    drop(writer);
    let open_path = directory.path().join("segment-000000.open");
    let file = OpenOptions::new().write(true).open(&open_path).unwrap();
    file.set_len(HEADER_LENGTH_U64).unwrap();
    file.sync_data().unwrap();
    drop(file);

    let outcome = recover_open_segment(&open_path, capture_id(), 0, start).unwrap();
    assert_eq!(
        outcome,
        RecoveryOutcome::DiscardedEmpty {
            segment_index: 0,
            truncated_bytes: 0,
        }
    );
    assert!(!open_path.exists());
}
