use std::fs::File;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal128Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub struct CaptureRow {
    pub capture_id: String,
    pub capture_status: String,
    pub utc_start: u64,
    pub utc_end: Option<u64>,
    pub manifest_sha256: String,
    pub configuration_sha256: String,
    pub configuration_json: String,
    pub disconnects: u64,
    pub order_book_unavailable: u64,
    pub trade_stream_gaps: u64,
    pub invalid_messages: u64,
    pub deduplicated_trades: u64,
}

pub struct OrderBookEventRow {
    pub capture_id: String,
    pub sequence: u64,
    pub venue: String,
    pub market: String,
    pub local_receive: u64,
    pub processing_completion: u64,
    pub source_sequence: Option<u64>,
    pub bid_count: u32,
    pub ask_count: u32,
}

pub struct BestBidOfferRow {
    pub capture_id: String,
    pub sequence: u64,
    pub venue: String,
    pub market: String,
    pub local_receive: u64,
    pub processing_completion: u64,
    pub bid_price: Option<i128>,
    pub bid_quantity: Option<i128>,
    pub bid_order_count: Option<u32>,
    pub ask_price: Option<i128>,
    pub ask_quantity: Option<i128>,
    pub ask_order_count: Option<u32>,
}

pub struct OrderBookLevelRow {
    pub capture_id: String,
    pub sequence: u64,
    pub venue: String,
    pub market: String,
    pub side: String,
    pub position: u32,
    pub price: i128,
    pub quantity: i128,
    pub order_count: Option<u32>,
}

pub struct MarketTradeRow {
    pub capture_id: String,
    pub sequence: u64,
    pub venue: String,
    pub market: String,
    pub local_receive: u64,
    pub processing_completion: u64,
    pub price: i128,
    pub quantity: i128,
    pub reporting_kind: String,
    pub trade_kind: String,
    pub aggressor_side: String,
    pub classification: String,
    pub aggregate_trade_id: Option<u64>,
    pub first_trade_id: Option<u64>,
    pub last_trade_id: Option<u64>,
    pub block_time: Option<u64>,
    pub trade_id_u64: Option<u64>,
    pub transaction_hash: Option<String>,
    pub market_id: Option<u32>,
    pub trade_id_string: Option<String>,
    pub message_nonce: Option<u64>,
}

pub struct AvailabilityRow {
    pub capture_id: String,
    pub sequence: u64,
    pub venue: String,
    pub market: String,
    pub stream: String,
    pub transition: String,
    pub category: Option<String>,
    pub diagnostic: Option<String>,
    pub observed_at: u64,
}

pub struct ExchangeTimeRow {
    pub capture_id: String,
    pub sequence: u64,
    pub venue: String,
    pub market: String,
    pub ordinal: u32,
    pub kind: String,
    pub raw_value: u64,
    pub declared_unit: String,
}

pub struct OrderBookLevelWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    _row: PhantomData<OrderBookLevelRow>,
}

impl OrderBookLevelWriter {
    pub fn create(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let schema = order_book_level_schema();
        let properties = writer_properties();
        let writer =
            ArrowWriter::try_new(File::create(path)?, Arc::clone(&schema), Some(properties))?;
        Ok(Self {
            writer,
            schema,
            _row: PhantomData,
        })
    }

    pub fn write(&mut self, rows: &[OrderBookLevelRow]) -> Result<(), Box<dyn std::error::Error>> {
        if rows.is_empty() {
            return Ok(());
        }
        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), order_book_level_columns(rows)?)?;
        self.writer.write(&batch)?;
        Ok(())
    }

    pub fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.close()?;
        Ok(())
    }
}

pub fn write_captures(path: &Path, rows: &[CaptureRow]) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        string_field("capture_id", false),
        string_field("capture_status", false),
        u64_field("utc_start_unix_millis", false),
        u64_field("utc_end_unix_millis", true),
        string_field("manifest_sha256", false),
        string_field("configuration_sha256", false),
        string_field("configuration_json", false),
        u64_field("disconnects", false),
        u64_field("order_book_unavailable", false),
        u64_field("trade_stream_gaps", false),
        u64_field("invalid_messages", false),
        u64_field("deduplicated_trades", false),
    ]));
    write_batch(
        path,
        schema,
        vec![
            strings(rows.iter().map(|row| row.capture_id.as_str())),
            strings(rows.iter().map(|row| row.capture_status.as_str())),
            u64s(rows.iter().map(|row| row.utc_start)),
            optional_u64s(rows.iter().map(|row| row.utc_end)),
            strings(rows.iter().map(|row| row.manifest_sha256.as_str())),
            strings(rows.iter().map(|row| row.configuration_sha256.as_str())),
            strings(rows.iter().map(|row| row.configuration_json.as_str())),
            u64s(rows.iter().map(|row| row.disconnects)),
            u64s(rows.iter().map(|row| row.order_book_unavailable)),
            u64s(rows.iter().map(|row| row.trade_stream_gaps)),
            u64s(rows.iter().map(|row| row.invalid_messages)),
            u64s(rows.iter().map(|row| row.deduplicated_trades)),
        ],
    )
}

pub fn write_order_book_events(
    path: &Path,
    rows: &[OrderBookEventRow],
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        string_field("capture_id", false),
        u64_field("capture_sequence", false),
        string_field("venue", false),
        string_field("market_coin", false),
        u64_field("local_receive_time", false),
        u64_field("processing_completion_time", false),
        u64_field("source_sequence", true),
        u32_field("bid_count", false),
        u32_field("ask_count", false),
    ]));
    write_batch(
        path,
        schema,
        vec![
            strings(rows.iter().map(|row| row.capture_id.as_str())),
            u64s(rows.iter().map(|row| row.sequence)),
            strings(rows.iter().map(|row| row.venue.as_str())),
            strings(rows.iter().map(|row| row.market.as_str())),
            u64s(rows.iter().map(|row| row.local_receive)),
            u64s(rows.iter().map(|row| row.processing_completion)),
            optional_u64s(rows.iter().map(|row| row.source_sequence)),
            u32s(rows.iter().map(|row| row.bid_count)),
            u32s(rows.iter().map(|row| row.ask_count)),
        ],
    )
}

pub fn write_best_bid_offers(
    path: &Path,
    rows: &[BestBidOfferRow],
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        string_field("capture_id", false),
        u64_field("capture_sequence", false),
        string_field("venue", false),
        string_field("market_coin", false),
        u64_field("local_receive_time", false),
        u64_field("processing_completion_time", false),
        decimal_field_nullable("best_bid_price"),
        decimal_field_nullable("best_bid_quantity"),
        u32_field("best_bid_order_count", true),
        decimal_field_nullable("best_ask_price"),
        decimal_field_nullable("best_ask_quantity"),
        u32_field("best_ask_order_count", true),
    ]));
    write_batch(
        path,
        schema,
        vec![
            strings(rows.iter().map(|row| row.capture_id.as_str())),
            u64s(rows.iter().map(|row| row.sequence)),
            strings(rows.iter().map(|row| row.venue.as_str())),
            strings(rows.iter().map(|row| row.market.as_str())),
            u64s(rows.iter().map(|row| row.local_receive)),
            u64s(rows.iter().map(|row| row.processing_completion)),
            optional_decimals(rows.iter().map(|row| row.bid_price))?,
            optional_decimals(rows.iter().map(|row| row.bid_quantity))?,
            optional_u32s(rows.iter().map(|row| row.bid_order_count)),
            optional_decimals(rows.iter().map(|row| row.ask_price))?,
            optional_decimals(rows.iter().map(|row| row.ask_quantity))?,
            optional_u32s(rows.iter().map(|row| row.ask_order_count)),
        ],
    )
}

fn order_book_level_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        string_field("capture_id", false),
        u64_field("capture_sequence", false),
        string_field("venue", false),
        string_field("market_coin", false),
        string_field("side", false),
        u32_field("position", false),
        decimal_field("price"),
        decimal_field("quantity"),
        u32_field("order_count", true),
    ]))
}

fn order_book_level_columns(
    rows: &[OrderBookLevelRow],
) -> Result<Vec<ArrayRef>, arrow_schema::ArrowError> {
    Ok(vec![
        strings(rows.iter().map(|row| row.capture_id.as_str())),
        u64s(rows.iter().map(|row| row.sequence)),
        strings(rows.iter().map(|row| row.venue.as_str())),
        strings(rows.iter().map(|row| row.market.as_str())),
        strings(rows.iter().map(|row| row.side.as_str())),
        u32s(rows.iter().map(|row| row.position)),
        decimals(rows.iter().map(|row| row.price))?,
        decimals(rows.iter().map(|row| row.quantity))?,
        optional_u32s(rows.iter().map(|row| row.order_count)),
    ])
}

pub fn write_market_trades(
    path: &Path,
    rows: &[MarketTradeRow],
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        string_field("capture_id", false),
        u64_field("capture_sequence", false),
        string_field("venue", false),
        string_field("market_coin", false),
        u64_field("local_receive_time", false),
        u64_field("processing_completion_time", false),
        decimal_field("price"),
        decimal_field("quantity"),
        string_field("reporting_kind", false),
        string_field("trade_kind", false),
        string_field("aggressor_side", false),
        string_field("aggressor_side_classification", false),
        u64_field("aster_aggregate_trade_id", true),
        u64_field("aster_first_trade_id", true),
        u64_field("aster_last_trade_id", true),
        u64_field("hyperliquid_block_time", true),
        u64_field("hyperliquid_trade_id", true),
        string_field("hyperliquid_transaction_hash", true),
        u32_field("lighter_market_id", true),
        string_field("lighter_trade_id_string", true),
        u64_field("lighter_message_nonce", true),
    ]));
    write_batch(
        path,
        schema,
        vec![
            strings(rows.iter().map(|row| row.capture_id.as_str())),
            u64s(rows.iter().map(|row| row.sequence)),
            strings(rows.iter().map(|row| row.venue.as_str())),
            strings(rows.iter().map(|row| row.market.as_str())),
            u64s(rows.iter().map(|row| row.local_receive)),
            u64s(rows.iter().map(|row| row.processing_completion)),
            decimals(rows.iter().map(|row| row.price))?,
            decimals(rows.iter().map(|row| row.quantity))?,
            strings(rows.iter().map(|row| row.reporting_kind.as_str())),
            strings(rows.iter().map(|row| row.trade_kind.as_str())),
            strings(rows.iter().map(|row| row.aggressor_side.as_str())),
            strings(rows.iter().map(|row| row.classification.as_str())),
            optional_u64s(rows.iter().map(|row| row.aggregate_trade_id)),
            optional_u64s(rows.iter().map(|row| row.first_trade_id)),
            optional_u64s(rows.iter().map(|row| row.last_trade_id)),
            optional_u64s(rows.iter().map(|row| row.block_time)),
            optional_u64s(rows.iter().map(|row| row.trade_id_u64)),
            optional_strings(rows.iter().map(|row| row.transaction_hash.as_deref())),
            optional_u32s(rows.iter().map(|row| row.market_id)),
            optional_strings(rows.iter().map(|row| row.trade_id_string.as_deref())),
            optional_u64s(rows.iter().map(|row| row.message_nonce)),
        ],
    )
}

pub fn write_availability(
    path: &Path,
    rows: &[AvailabilityRow],
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        string_field("capture_id", false),
        u64_field("capture_sequence", false),
        string_field("venue", false),
        string_field("market_coin", false),
        string_field("stream", false),
        string_field("transition", false),
        string_field("category", true),
        string_field("diagnostic", true),
        u64_field("observed_at", false),
    ]));
    write_batch(
        path,
        schema,
        vec![
            strings(rows.iter().map(|row| row.capture_id.as_str())),
            u64s(rows.iter().map(|row| row.sequence)),
            strings(rows.iter().map(|row| row.venue.as_str())),
            strings(rows.iter().map(|row| row.market.as_str())),
            strings(rows.iter().map(|row| row.stream.as_str())),
            strings(rows.iter().map(|row| row.transition.as_str())),
            optional_strings(rows.iter().map(|row| row.category.as_deref())),
            optional_strings(rows.iter().map(|row| row.diagnostic.as_deref())),
            u64s(rows.iter().map(|row| row.observed_at)),
        ],
    )
}

pub fn write_exchange_times(
    path: &Path,
    rows: &[ExchangeTimeRow],
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        string_field("capture_id", false),
        u64_field("capture_sequence", false),
        string_field("venue", false),
        string_field("market_coin", false),
        u32_field("ordinal", false),
        string_field("kind", false),
        u64_field("raw_value", false),
        string_field("declared_unit", false),
    ]));
    write_batch(
        path,
        schema,
        vec![
            strings(rows.iter().map(|row| row.capture_id.as_str())),
            u64s(rows.iter().map(|row| row.sequence)),
            strings(rows.iter().map(|row| row.venue.as_str())),
            strings(rows.iter().map(|row| row.market.as_str())),
            u32s(rows.iter().map(|row| row.ordinal)),
            strings(rows.iter().map(|row| row.kind.as_str())),
            u64s(rows.iter().map(|row| row.raw_value)),
            strings(rows.iter().map(|row| row.declared_unit.as_str())),
        ],
    )
}

fn write_batch(
    path: &Path,
    schema: Arc<Schema>,
    columns: Vec<ArrayRef>,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let properties = writer_properties();
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build()
}

fn string_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn u64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt64, nullable)
}

fn u32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt32, nullable)
}

fn decimal_field(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(38, 18), false)
}

fn decimal_field_nullable(name: &str) -> Field {
    Field::new(name, DataType::Decimal128(38, 18), true)
}

fn strings<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn optional_strings<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn u64s(values: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(values))
}

fn optional_u64s(values: impl Iterator<Item = Option<u64>>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter(values))
}

fn u32s(values: impl Iterator<Item = u32>) -> ArrayRef {
    Arc::new(UInt32Array::from_iter_values(values))
}

fn optional_u32s(values: impl Iterator<Item = Option<u32>>) -> ArrayRef {
    Arc::new(UInt32Array::from_iter(values))
}

fn decimals(values: impl Iterator<Item = i128>) -> Result<ArrayRef, arrow_schema::ArrowError> {
    Ok(Arc::new(
        Decimal128Array::from_iter_values(values).with_precision_and_scale(38, 18)?,
    ))
}

fn optional_decimals(
    values: impl Iterator<Item = Option<i128>>,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    Ok(Arc::new(
        Decimal128Array::from_iter(values).with_precision_and_scale(38, 18)?,
    ))
}
