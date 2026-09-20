use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use domain::Venue;
use market_data::{
    BestBidOffer, EventTimestamps, ExchangeTimeKind, ExchangeTimeUnit, MarketTradeIdentity,
    NormalizedMarketEvent,
};
use recorder::{CaptureStatus, ValidatedCapture, ValidationOptions};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::parquet_tables::{
    AvailabilityRow, BestBidOfferRow, CaptureRow, ExchangeTimeRow, MarketTradeRow,
    OrderBookEventRow, OrderBookLevelRow, OrderBookLevelWriter, write_availability,
    write_best_bid_offers, write_captures, write_exchange_times, write_market_trades,
    write_order_book_events,
};

const PARQUET_SCHEMA_VERSION: u32 = 2;
const LEVEL_BATCH_ROWS: usize = 4_096;
type TableWrite<Row> = fn(&Path, &[Row]) -> Result<(), Box<dyn Error>>;

pub struct ExportRequest {
    pub capture_directory: PathBuf,
    pub output_directory: PathBuf,
    pub allow_incomplete: bool,
}

pub struct ExportResult {
    pub output_directory: PathBuf,
    pub capture_id: String,
    pub canonical_event_count: u64,
    pub table_rows: BTreeMap<String, u64>,
}

#[derive(Debug)]
pub enum ExportError {
    OutputMustBeAbsolute(PathBuf),
    OutputAlreadyExists(PathBuf),
    Io(io::Error),
    Validation(recorder::CaptureValidationError),
    InvalidCaptureStart,
    DecimalScale(u8),
    DecimalOverflow,
    RowCountOverflow,
    ReconciliationMismatch {
        canonical_events: u64,
        exported_events: u64,
    },
    Parquet(String),
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct Partition {
    venue: String,
    market: String,
}

struct LevelSink {
    writer: OrderBookLevelWriter,
    buffered: Vec<OrderBookLevelRow>,
}

struct LevelSinks {
    root: PathBuf,
    date: String,
    capture_id: String,
    sinks: BTreeMap<Partition, LevelSink>,
    row_count: u64,
}

#[derive(Serialize)]
struct DatasetMetadata {
    schema_version: u32,
    source_capture_ids: Vec<String>,
    source_manifest_sha256: Vec<String>,
    source_capture_statuses: Vec<String>,
    contains_incomplete_capture: bool,
    converter_git_commit: String,
    converter_package_version: String,
    utc_conversion_time_unix_millis: u64,
    table_rows: BTreeMap<String, u64>,
}

pub fn export_capture(request: ExportRequest) -> Result<ExportResult, ExportError> {
    if !request.output_directory.is_absolute() {
        return Err(ExportError::OutputMustBeAbsolute(request.output_directory));
    }
    if request.output_directory.exists() {
        return Err(ExportError::OutputAlreadyExists(request.output_directory));
    }
    let capture = ValidatedCapture::open(
        &request.capture_directory,
        ValidationOptions {
            allow_incomplete: request.allow_incomplete,
        },
    )
    .map_err(ExportError::Validation)?;
    let manifest = capture.manifest();
    let capture_id = manifest.capture_id.to_string();
    let date = capture_date(manifest.utc_start_unix_millis)?;
    let manifest_bytes =
        fs::read(request.capture_directory.join("manifest.json")).map_err(ExportError::Io)?;
    let manifest_sha256 = encode_hex(&Sha256::digest(&manifest_bytes));
    let temporary = temporary_directory(&request.output_directory, &capture_id)?;
    fs::create_dir_all(&temporary).map_err(ExportError::Io)?;

    let canonical_event_count = capture.event_count();
    let result = export_into(
        &temporary,
        &date,
        &capture_id,
        manifest,
        &manifest_sha256,
        &capture,
    );
    let table_rows = match result {
        Ok(rows) => rows,
        Err(error) => {
            let _ = fs::remove_dir_all(&temporary);
            return Err(error);
        }
    };
    fs::rename(&temporary, &request.output_directory).map_err(ExportError::Io)?;
    Ok(ExportResult {
        output_directory: request.output_directory,
        capture_id,
        canonical_event_count,
        table_rows,
    })
}

fn export_into(
    output: &Path,
    date: &str,
    capture_id: &str,
    manifest: &recorder::CaptureManifest,
    manifest_sha256: &str,
    capture: &ValidatedCapture,
) -> Result<BTreeMap<String, u64>, ExportError> {
    const EVENT_BATCH_ROWS: u64 = 4_096;
    let mut books = BTreeMap::<Partition, Vec<OrderBookEventRow>>::new();
    let mut best_bid_offers = BTreeMap::<Partition, Vec<BestBidOfferRow>>::new();
    let mut trades = BTreeMap::<Partition, Vec<MarketTradeRow>>::new();
    let mut availability = BTreeMap::<Partition, Vec<AvailabilityRow>>::new();
    let mut exchange_times = BTreeMap::<Partition, Vec<ExchangeTimeRow>>::new();
    let version_root = output.join(format!("version={PARQUET_SCHEMA_VERSION}"));
    let mut levels = LevelSinks::new(&version_root, date, capture_id);

    let mut table_rows = BTreeMap::from([
        ("captures".into(), 1_u64),
        ("order_book_events".into(), 0_u64),
        ("best_bid_offers".into(), 0_u64),
        ("order_book_levels".into(), 0_u64),
        ("market_trades".into(), 0_u64),
        ("availability_events".into(), 0_u64),
        ("exchange_times".into(), 0_u64),
    ]);
    let mut processed_events = 0_u64;
    let mut batch_index = 0_u64;
    let mut export_error = None;

    capture
        .for_each_event(|replay_event| {
            if export_error.is_some() {
                return;
            }
            let event = &replay_event.event;
            let venue = venue_name(event.venue()).to_owned();
            let market = event.market_coin().to_string();
            let partition = Partition {
                venue: venue.clone(),
                market: market.clone(),
            };
            let result = (|| -> Result<(), ExportError> {
                match event {
                    NormalizedMarketEvent::OrderBookSnapshot(snapshot) => {
                        books
                            .entry(partition.clone())
                            .or_default()
                            .push(OrderBookEventRow {
                                capture_id: capture_id.into(),
                                sequence: replay_event.capture_sequence,
                                venue: venue.clone(),
                                market: market.clone(),
                                local_receive: snapshot
                                    .timestamps()
                                    .local_receive()
                                    .nanos_since_start(),
                                processing_completion: snapshot
                                    .timestamps()
                                    .processing_completed()
                                    .nanos_since_start(),
                                source_sequence: snapshot.source_sequence(),
                                bid_count: u32::try_from(snapshot.bids().len())
                                    .map_err(|_| ExportError::RowCountOverflow)?,
                                ask_count: u32::try_from(snapshot.asks().len())
                                    .map_err(|_| ExportError::RowCountOverflow)?,
                            });
                        for (position, level) in snapshot.bids().iter().enumerate() {
                            levels.push(
                                partition.clone(),
                                OrderBookLevelRow {
                                    capture_id: capture_id.into(),
                                    sequence: replay_event.capture_sequence,
                                    venue: venue.clone(),
                                    market: market.clone(),
                                    side: "bid".into(),
                                    position: u32::try_from(position)
                                        .map_err(|_| ExportError::RowCountOverflow)?,
                                    price: decimal38(
                                        level.price().coefficient(),
                                        level.price().scale(),
                                    )?,
                                    quantity: decimal38(
                                        level.quantity().coefficient(),
                                        level.quantity().scale(),
                                    )?,
                                    order_count: level.order_count().map(std::num::NonZeroU32::get),
                                },
                            )?;
                        }
                        for (position, level) in snapshot.asks().iter().enumerate() {
                            levels.push(
                                partition.clone(),
                                OrderBookLevelRow {
                                    capture_id: capture_id.into(),
                                    sequence: replay_event.capture_sequence,
                                    venue: venue.clone(),
                                    market: market.clone(),
                                    side: "ask".into(),
                                    position: u32::try_from(position)
                                        .map_err(|_| ExportError::RowCountOverflow)?,
                                    price: decimal38(
                                        level.price().coefficient(),
                                        level.price().scale(),
                                    )?,
                                    quantity: decimal38(
                                        level.quantity().coefficient(),
                                        level.quantity().scale(),
                                    )?,
                                    order_count: level.order_count().map(std::num::NonZeroU32::get),
                                },
                            )?;
                        }
                        append_exchange_times(
                            &mut exchange_times,
                            partition,
                            capture_id,
                            replay_event.capture_sequence,
                            &venue,
                            &market,
                            snapshot.timestamps(),
                        )
                    }
                    NormalizedMarketEvent::MarketTrade(trade) => {
                        let mut row = MarketTradeRow {
                            capture_id: capture_id.into(),
                            sequence: replay_event.capture_sequence,
                            venue: venue.clone(),
                            market: market.clone(),
                            local_receive: trade.timestamps().local_receive().nanos_since_start(),
                            processing_completion: trade
                                .timestamps()
                                .processing_completed()
                                .nanos_since_start(),
                            price: decimal38(trade.price().coefficient(), trade.price().scale())?,
                            quantity: decimal38(
                                trade.quantity().coefficient(),
                                trade.quantity().scale(),
                            )?,
                            reporting_kind: format!("{:?}", trade.reporting_kind()),
                            trade_kind: format!("{:?}", trade.trade_kind()),
                            aggressor_side: format!("{:?}", trade.aggressor_side()),
                            classification: format!("{:?}", trade.aggressor_side_classification()),
                            aggregate_trade_id: None,
                            first_trade_id: None,
                            last_trade_id: None,
                            block_time: None,
                            trade_id_u64: None,
                            transaction_hash: None,
                            market_id: None,
                            trade_id_string: None,
                            message_nonce: None,
                        };
                        match trade.identity() {
                            MarketTradeIdentity::Aster {
                                aggregate_trade_id,
                                first_trade_id,
                                last_trade_id,
                            } => {
                                row.aggregate_trade_id = Some(*aggregate_trade_id);
                                row.first_trade_id = Some(*first_trade_id);
                                row.last_trade_id = Some(*last_trade_id);
                            }
                            MarketTradeIdentity::Hyperliquid {
                                block_time,
                                trade_id,
                                transaction_hash,
                            } => {
                                row.block_time = Some(*block_time);
                                row.trade_id_u64 = Some(*trade_id);
                                row.transaction_hash = Some(transaction_hash.clone());
                            }
                            MarketTradeIdentity::Lighter {
                                market_id,
                                trade_id,
                                message_nonce,
                            } => {
                                row.market_id = Some(*market_id);
                                row.trade_id_string = Some(trade_id.clone());
                                row.message_nonce = *message_nonce;
                            }
                        }
                        trades.entry(partition.clone()).or_default().push(row);
                        append_exchange_times(
                            &mut exchange_times,
                            partition,
                            capture_id,
                            replay_event.capture_sequence,
                            &venue,
                            &market,
                            trade.timestamps(),
                        )
                    }
                    NormalizedMarketEvent::BestBidOfferUpdated(bbo) => {
                        let bid = match bbo.bid() {
                            Some(level) => Some((
                                decimal38(level.price().coefficient(), level.price().scale())?,
                                decimal38(
                                    level.quantity().coefficient(),
                                    level.quantity().scale(),
                                )?,
                                level.order_count().map(std::num::NonZeroU32::get),
                            )),
                            None => None,
                        };
                        let ask = match bbo.ask() {
                            Some(level) => Some((
                                decimal38(level.price().coefficient(), level.price().scale())?,
                                decimal38(
                                    level.quantity().coefficient(),
                                    level.quantity().scale(),
                                )?,
                                level.order_count().map(std::num::NonZeroU32::get),
                            )),
                            None => None,
                        };
                        best_bid_offers.entry(partition.clone()).or_default().push(
                            BestBidOfferRow {
                                capture_id: capture_id.into(),
                                sequence: replay_event.capture_sequence,
                                venue: venue.clone(),
                                market: market.clone(),
                                local_receive: bbo.timestamps().local_receive().nanos_since_start(),
                                processing_completion: bbo
                                    .timestamps()
                                    .processing_completed()
                                    .nanos_since_start(),
                                bid_price: bid.as_ref().map(|value| value.0),
                                bid_quantity: bid.as_ref().map(|value| value.1),
                                bid_order_count: bid.and_then(|value| value.2),
                                ask_price: ask.as_ref().map(|value| value.0),
                                ask_quantity: ask.as_ref().map(|value| value.1),
                                ask_order_count: ask.and_then(|value| value.2),
                            },
                        );
                        append_exchange_times(
                            &mut exchange_times,
                            partition,
                            capture_id,
                            replay_event.capture_sequence,
                            &venue,
                            &market,
                            bbo.timestamps(),
                        )
                    }
                    NormalizedMarketEvent::OrderBookUnavailable(event) => {
                        availability
                            .entry(partition)
                            .or_default()
                            .push(AvailabilityRow {
                                capture_id: capture_id.into(),
                                sequence: replay_event.capture_sequence,
                                venue,
                                market,
                                stream: "order_book".into(),
                                transition: "unavailable".into(),
                                category: Some(format!("{:?}", event.category())),
                                diagnostic: Some(event.diagnostic().into()),
                                observed_at: event.observed_at().nanos_since_start(),
                            });
                        Ok(())
                    }
                    NormalizedMarketEvent::TradeStreamUnavailable(event) => {
                        availability
                            .entry(partition)
                            .or_default()
                            .push(AvailabilityRow {
                                capture_id: capture_id.into(),
                                sequence: replay_event.capture_sequence,
                                venue,
                                market,
                                stream: "trade_stream".into(),
                                transition: "unavailable".into(),
                                category: Some(format!("{:?}", event.category())),
                                diagnostic: Some(event.diagnostic().into()),
                                observed_at: event.observed_at().nanos_since_start(),
                            });
                        Ok(())
                    }
                    NormalizedMarketEvent::TradeStreamResumed(event) => {
                        availability
                            .entry(partition)
                            .or_default()
                            .push(AvailabilityRow {
                                capture_id: capture_id.into(),
                                sequence: replay_event.capture_sequence,
                                venue,
                                market,
                                stream: "trade_stream".into(),
                                transition: "resumed".into(),
                                category: None,
                                diagnostic: None,
                                observed_at: event.observed_at().nanos_since_start(),
                            });
                        Ok(())
                    }
                }
            })();
            if let Err(error) = result {
                export_error = Some(error);
                return;
            }
            processed_events += 1;
            if processed_events % EVENT_BATCH_ROWS == 0 {
                if let Err(error) = flush_event_batches(
                    &version_root,
                    date,
                    capture_id,
                    batch_index,
                    &mut books,
                    &mut best_bid_offers,
                    &mut trades,
                    &mut availability,
                    &mut exchange_times,
                    &mut table_rows,
                ) {
                    export_error = Some(error);
                } else {
                    batch_index += 1;
                }
            }
        })
        .map_err(ExportError::Validation)?;
    if let Some(error) = export_error {
        return Err(error);
    }
    flush_event_batches(
        &version_root,
        date,
        capture_id,
        batch_index,
        &mut books,
        &mut best_bid_offers,
        &mut trades,
        &mut availability,
        &mut exchange_times,
        &mut table_rows,
    )?;

    let configuration_json = serde_json::to_string(&manifest.configuration).map_err(|error| {
        ExportError::Io(io::Error::other(format!(
            "cannot serialize captured configuration: {error}"
        )))
    })?;
    let capture_rows = [CaptureRow {
        capture_id: capture_id.into(),
        capture_status: capture_status(manifest.capture_status).into(),
        utc_start: manifest.utc_start_unix_millis,
        utc_end: manifest.utc_end_unix_millis,
        manifest_sha256: manifest_sha256.into(),
        configuration_sha256: manifest.configuration_sha256.clone(),
        configuration_json,
        disconnects: manifest.data_quality.disconnects,
        order_book_unavailable: manifest.data_quality.order_book_unavailable,
        trade_stream_gaps: manifest.data_quality.trade_stream_gaps,
        invalid_messages: manifest.data_quality.invalid_messages,
        deduplicated_trades: manifest.data_quality.deduplicated_trades,
    }];

    let level_row_count = levels.finish()?;
    table_rows.insert("order_book_levels".into(), level_row_count);
    write_partition(
        &version_root,
        date,
        "captures",
        &Partition {
            venue: "all".into(),
            market: "all".into(),
        },
        capture_id,
        |path| write_captures(path, &capture_rows),
    )?;
    let canonical_events = capture.event_count();
    let exported_events = table_rows["order_book_events"]
        .checked_add(table_rows["best_bid_offers"])
        .and_then(|value| value.checked_add(table_rows["market_trades"]))
        .and_then(|value| value.checked_add(table_rows["availability_events"]))
        .ok_or(ExportError::RowCountOverflow)?;
    if canonical_events != exported_events {
        return Err(ExportError::ReconciliationMismatch {
            canonical_events,
            exported_events,
        });
    }
    let metadata = DatasetMetadata {
        schema_version: PARQUET_SCHEMA_VERSION,
        source_capture_ids: vec![capture_id.into()],
        source_manifest_sha256: vec![manifest_sha256.into()],
        source_capture_statuses: vec![capture_status(manifest.capture_status).into()],
        contains_incomplete_capture: manifest.capture_status != CaptureStatus::Complete,
        converter_git_commit: option_env!("GIT_COMMIT").unwrap_or("unknown").into(),
        converter_package_version: env!("CARGO_PKG_VERSION").into(),
        utc_conversion_time_unix_millis: unix_millis(SystemTime::now())?,
        table_rows: table_rows.clone(),
    };
    fs::write(
        output.join("dataset-metadata.json"),
        serde_json::to_vec_pretty(&metadata)
            .map_err(|error| ExportError::Io(io::Error::other(error)))?,
    )
    .map_err(ExportError::Io)?;
    Ok(table_rows)
}

fn flush_event_batches(
    root: &Path,
    date: &str,
    capture_id: &str,
    batch_index: u64,
    books: &mut BTreeMap<Partition, Vec<OrderBookEventRow>>,
    best_bid_offers: &mut BTreeMap<Partition, Vec<BestBidOfferRow>>,
    trades: &mut BTreeMap<Partition, Vec<MarketTradeRow>>,
    availability: &mut BTreeMap<Partition, Vec<AvailabilityRow>>,
    exchange_times: &mut BTreeMap<Partition, Vec<ExchangeTimeRow>>,
    table_rows: &mut BTreeMap<String, u64>,
) -> Result<(), ExportError> {
    let books_count = flush_groups(
        root,
        date,
        "order_book_events",
        capture_id,
        batch_index,
        books,
        write_order_book_events,
    )?;
    let trades_count = flush_groups(
        root,
        date,
        "market_trades",
        capture_id,
        batch_index,
        trades,
        write_market_trades,
    )?;
    let best_bid_offers_count = flush_groups(
        root,
        date,
        "best_bid_offers",
        capture_id,
        batch_index,
        best_bid_offers,
        write_best_bid_offers,
    )?;
    let availability_count = flush_groups(
        root,
        date,
        "availability_events",
        capture_id,
        batch_index,
        availability,
        write_availability,
    )?;
    let exchange_times_count = flush_groups(
        root,
        date,
        "exchange_times",
        capture_id,
        batch_index,
        exchange_times,
        write_exchange_times,
    )?;
    *table_rows
        .get_mut("order_book_events")
        .expect("table exists") += books_count;
    *table_rows.get_mut("best_bid_offers").expect("table exists") += best_bid_offers_count;
    *table_rows.get_mut("market_trades").expect("table exists") += trades_count;
    *table_rows
        .get_mut("availability_events")
        .expect("table exists") += availability_count;
    *table_rows.get_mut("exchange_times").expect("table exists") += exchange_times_count;
    Ok(())
}

fn flush_groups<Row>(
    root: &Path,
    date: &str,
    table: &str,
    capture_id: &str,
    batch_index: u64,
    groups: &mut BTreeMap<Partition, Vec<Row>>,
    write: TableWrite<Row>,
) -> Result<u64, ExportError> {
    let mut count = 0_u64;
    let filename = format!("part-{capture_id}-{batch_index:06}.parquet");
    for (partition, rows) in groups.iter_mut() {
        if rows.is_empty() {
            continue;
        }
        let path = partition_file_path_named(root, date, table, partition, &filename)?;
        write(&path, rows).map_err(|error| ExportError::Parquet(error.to_string()))?;
        count = count
            .checked_add(u64::try_from(rows.len()).map_err(|_| ExportError::RowCountOverflow)?)
            .ok_or(ExportError::RowCountOverflow)?;
        rows.clear();
    }
    Ok(count)
}

#[allow(clippy::too_many_arguments)]
fn append_exchange_times(
    groups: &mut BTreeMap<Partition, Vec<ExchangeTimeRow>>,
    partition: Partition,
    capture_id: &str,
    sequence: u64,
    venue: &str,
    market: &str,
    timestamps: &EventTimestamps,
) -> Result<(), ExportError> {
    let rows = groups.entry(partition).or_default();
    for (ordinal, observation) in timestamps.exchange_times().iter().enumerate() {
        rows.push(ExchangeTimeRow {
            capture_id: capture_id.into(),
            sequence,
            venue: venue.into(),
            market: market.into(),
            ordinal: u32::try_from(ordinal).map_err(|_| ExportError::RowCountOverflow)?,
            kind: exchange_time_kind(observation.kind()).into(),
            raw_value: observation.raw_value(),
            declared_unit: exchange_time_unit(observation.declared_unit()).into(),
        });
    }
    Ok(())
}

impl LevelSinks {
    fn new(root: &Path, date: &str, capture_id: &str) -> Self {
        Self {
            root: root.to_path_buf(),
            date: date.into(),
            capture_id: capture_id.into(),
            sinks: BTreeMap::new(),
            row_count: 0,
        }
    }

    fn push(&mut self, partition: Partition, row: OrderBookLevelRow) -> Result<(), ExportError> {
        if !self.sinks.contains_key(&partition) {
            let path = partition_file_path(
                &self.root,
                &self.date,
                "order_book_levels",
                &partition,
                &self.capture_id,
            )?;
            let writer = OrderBookLevelWriter::create(&path)
                .map_err(|error| ExportError::Parquet(error.to_string()))?;
            self.sinks.insert(
                partition.clone(),
                LevelSink {
                    writer,
                    buffered: Vec::with_capacity(LEVEL_BATCH_ROWS),
                },
            );
        }
        let sink = self
            .sinks
            .get_mut(&partition)
            .expect("level sink was inserted above");
        sink.buffered.push(row);
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or(ExportError::RowCountOverflow)?;
        if sink.buffered.len() >= LEVEL_BATCH_ROWS {
            sink.writer
                .write(&sink.buffered)
                .map_err(|error| ExportError::Parquet(error.to_string()))?;
            sink.buffered.clear();
        }
        Ok(())
    }

    fn finish(self) -> Result<u64, ExportError> {
        for (_, mut sink) in self.sinks {
            sink.writer
                .write(&sink.buffered)
                .map_err(|error| ExportError::Parquet(error.to_string()))?;
            sink.writer
                .close()
                .map_err(|error| ExportError::Parquet(error.to_string()))?;
        }
        Ok(self.row_count)
    }
}

fn write_partition(
    root: &Path,
    date: &str,
    table: &str,
    partition: &Partition,
    capture_id: &str,
    write: impl FnOnce(&Path) -> Result<(), Box<dyn Error>>,
) -> Result<(), ExportError> {
    let path = partition_file_path(root, date, table, partition, capture_id)?;
    write(&path).map_err(|error| ExportError::Parquet(error.to_string()))
}

fn partition_file_path(
    root: &Path,
    date: &str,
    table: &str,
    partition: &Partition,
    capture_id: &str,
) -> Result<PathBuf, ExportError> {
    partition_file_path_named(
        root,
        date,
        table,
        partition,
        &format!("part-{capture_id}.parquet"),
    )
}

fn partition_file_path_named(
    root: &Path,
    date: &str,
    table: &str,
    partition: &Partition,
    filename: &str,
) -> Result<PathBuf, ExportError> {
    let directory = root
        .join(format!("date={date}"))
        .join(format!("event_type={table}"))
        .join(format!("venue={}", safe_partition_value(&partition.venue)))
        .join(format!(
            "market={}",
            safe_partition_value(&partition.market)
        ));
    fs::create_dir_all(&directory).map_err(ExportError::Io)?;
    Ok(directory.join(filename))
}

fn decimal38(coefficient: i128, scale: u8) -> Result<i128, ExportError> {
    if scale > 18 {
        return Err(ExportError::DecimalScale(scale));
    }
    let value = coefficient
        .checked_mul(10_i128.pow(u32::from(18 - scale)))
        .ok_or(ExportError::DecimalOverflow)?;
    if value.unsigned_abs() >= 10_u128.pow(38) {
        return Err(ExportError::DecimalOverflow);
    }
    Ok(value)
}

fn capture_date(unix_millis: u64) -> Result<String, ExportError> {
    let unix_millis = i64::try_from(unix_millis).map_err(|_| ExportError::InvalidCaptureStart)?;
    DateTime::<Utc>::from_timestamp_millis(unix_millis)
        .map(|value| value.format("%Y-%m-%d").to_string())
        .ok_or(ExportError::InvalidCaptureStart)
}

fn temporary_directory(output: &Path, capture_id: &str) -> Result<PathBuf, ExportError> {
    let parent = output.parent().ok_or_else(|| {
        ExportError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dataset output has no parent directory",
        ))
    })?;
    fs::create_dir_all(parent).map_err(ExportError::Io)?;
    let name = output
        .file_name()
        .ok_or_else(|| {
            ExportError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "dataset output has no filename",
            ))
        })?
        .to_string_lossy();
    let temporary = parent.join(format!(".{name}.building-{capture_id}"));
    if temporary.exists() {
        return Err(ExportError::OutputAlreadyExists(temporary));
    }
    Ok(temporary)
}

fn safe_partition_value(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    encoded
}

fn venue_name(venue: Venue) -> &'static str {
    match venue {
        Venue::Aster => "aster",
        Venue::Hyperliquid => "hyperliquid",
        Venue::Lighter => "lighter",
    }
}

fn capture_status(status: CaptureStatus) -> &'static str {
    match status {
        CaptureStatus::Complete => "complete",
        CaptureStatus::IncompleteQueueFull => "incomplete_queue_full",
        CaptureStatus::IncompleteProcessCrash => "incomplete_process_crash",
        CaptureStatus::IncompleteIoError => "incomplete_io_error",
    }
}

fn exchange_time_kind(kind: ExchangeTimeKind) -> &'static str {
    match kind {
        ExchangeTimeKind::EventTime => "EventTime",
        ExchangeTimeKind::TradeTime => "TradeTime",
        ExchangeTimeKind::BlockTime => "BlockTime",
        ExchangeTimeKind::TransactionTime => "TransactionTime",
        ExchangeTimeKind::Other => "Other",
    }
}

fn exchange_time_unit(unit: ExchangeTimeUnit) -> &'static str {
    match unit {
        ExchangeTimeUnit::Milliseconds => "Milliseconds",
        ExchangeTimeUnit::Microseconds => "Microseconds",
        ExchangeTimeUnit::Nanoseconds => "Nanoseconds",
        ExchangeTimeUnit::Unknown => "Unknown",
    }
}

fn unix_millis(time: SystemTime) -> Result<u64, ExportError> {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ExportError::InvalidCaptureStart)?
        .as_millis();
    u64::try_from(millis).map_err(|_| ExportError::InvalidCaptureStart)
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

impl Display for ExportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputMustBeAbsolute(path) => {
                write!(
                    formatter,
                    "dataset output must be absolute: {}",
                    path.display()
                )
            }
            Self::OutputAlreadyExists(path) => write!(
                formatter,
                "immutable dataset output already exists: {}",
                path.display()
            ),
            Self::Io(error) => write!(formatter, "Parquet export I/O failed: {error}"),
            Self::Validation(error) => Display::fmt(error, formatter),
            Self::InvalidCaptureStart => {
                formatter.write_str("capture UTC start is outside the supported range")
            }
            Self::DecimalScale(scale) => {
                write!(formatter, "decimal scale {scale} exceeds DECIMAL(38,18)")
            }
            Self::DecimalOverflow => {
                formatter.write_str("exact decimal does not fit DECIMAL(38,18)")
            }
            Self::RowCountOverflow => {
                formatter.write_str("export row count exceeds supported range")
            }
            Self::ReconciliationMismatch {
                canonical_events,
                exported_events,
            } => write!(
                formatter,
                "Parquet event rows do not reconcile: canonical={canonical_events}, exported={exported_events}"
            ),
            Self::Parquet(error) => write!(formatter, "Parquet writer failed: {error}"),
        }
    }
}

impl Error for ExportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Validation(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::num::NonZeroUsize;
    use std::str::FromStr;
    use std::time::Duration;

    use arrow_schema::DataType;
    use domain::{MarketCoin, Price, Quantity, Symbol};
    use market_data::{
        BookLevel, EventTimestamps, ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit,
        LocalObservationTime, OrderBookSnapshot,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use recorder::{
        CaptureCoordinator, CaptureMetadata, CaptureSettings, CaptureStatus, SegmentLimits,
    };
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn exact_decimal_conversion_pads_without_rounding() {
        assert_eq!(decimal38(1234, 2).unwrap(), 12_340_000_000_000_000_000);
        assert!(matches!(
            decimal38(1, 19),
            Err(ExportError::DecimalScale(19))
        ));
    }

    #[test]
    fn partition_values_cannot_escape_the_dataset() {
        assert_eq!(safe_partition_value("dex/Coin 1"), "dex%2FCoin%201");
    }

    #[test]
    fn exports_validated_capture_with_exact_decimal_schema_and_reconciled_counts() {
        let data = TempDir::new().unwrap();
        let datasets = TempDir::new().unwrap();
        let started_at = UNIX_EPOCH + Duration::from_secs(1);
        let mut capture = CaptureCoordinator::start(
            CaptureSettings {
                data_dir: data.path().to_path_buf(),
                queue_capacity: NonZeroUsize::new(4).unwrap(),
                segment_limits: SegmentLimits::default(),
                metadata: CaptureMetadata {
                    sanitized_configuration: BTreeMap::from([(
                        "MARKET_COINS".into(),
                        "BTC".into(),
                    )]),
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
        let capture_directory = capture.capture_directory().to_path_buf();
        let coin = MarketCoin::try_new("BTC").unwrap();
        let snapshot = OrderBookSnapshot::try_new(
            Venue::Aster,
            Symbol::perpetual(coin),
            Some(7),
            EventTimestamps::new(
                vec![ExchangeTimeObservation::new(
                    ExchangeTimeKind::EventTime,
                    1,
                    ExchangeTimeUnit::Milliseconds,
                )],
                LocalObservationTime::from_nanos_since_start(2),
                LocalObservationTime::from_nanos_since_start(3),
            ),
            vec![BookLevel::new(
                Price::from_str("100.25").unwrap(),
                Quantity::from_str("2").unwrap(),
                None,
            )],
            vec![BookLevel::new(
                Price::from_str("100.50").unwrap(),
                Quantity::from_str("3").unwrap(),
                None,
            )],
        )
        .unwrap();
        let bbo = BestBidOffer::new(
            Venue::Aster,
            snapshot.symbol().clone(),
            snapshot.timestamps().clone(),
            snapshot.bids().first().cloned(),
            snapshot.asks().first().cloned(),
        );
        capture
            .accept(NormalizedMarketEvent::OrderBookSnapshot(snapshot))
            .unwrap();
        capture
            .accept(NormalizedMarketEvent::BestBidOfferUpdated(bbo))
            .unwrap();
        capture
            .finish(CaptureStatus::Complete, started_at + Duration::from_secs(1))
            .unwrap();

        let output = datasets.path().join("dataset");
        let result = export_capture(ExportRequest {
            capture_directory,
            output_directory: output.clone(),
            allow_incomplete: false,
        })
        .unwrap();
        assert_eq!(result.table_rows["order_book_events"], 1);
        assert_eq!(result.table_rows["best_bid_offers"], 1);
        assert_eq!(result.table_rows["order_book_levels"], 2);
        assert_eq!(result.table_rows["exchange_times"], 2);

        let levels_path = output
            .join("version=2/date=1970-01-01/event_type=order_book_levels")
            .join("venue=aster/market=BTC")
            .join(format!("part-{}.parquet", result.capture_id));
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(levels_path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch.schema().field_with_name("price").unwrap().data_type(),
            &DataType::Decimal128(38, 18)
        );
        assert!(output.join("dataset-metadata.json").is_file());
        assert!(
            output
                .join("version=2/date=1970-01-01/event_type=best_bid_offers")
                .join("venue=aster/market=BTC")
                .join(format!("part-{}-000000.parquet", result.capture_id))
                .is_file()
        );
    }
}
