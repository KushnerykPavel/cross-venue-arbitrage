use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{FinalizedSegment, SegmentCompletion, SegmentLimits};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureStatus {
    Complete,
    IncompleteQueueFull,
    IncompleteProcessCrash,
    IncompleteIoError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolvedVenueMarket {
    pub venue: String,
    pub market_coin: String,
    pub venue_symbol: Option<String>,
    pub venue_market_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CaptureMetadata {
    pub sanitized_configuration: BTreeMap<String, String>,
    pub git_commit: String,
    pub dirty_build: bool,
    pub package_version: String,
    pub rust_target: String,
    pub collector_label: String,
    pub configured_markets: Vec<String>,
    pub resolved_venue_markets: Vec<ResolvedVenueMarket>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DataQualityCounters {
    pub disconnects: u64,
    pub order_book_unavailable: u64,
    pub trade_stream_gaps: u64,
    pub invalid_messages: u64,
    pub deduplicated_trades: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentManifestEntry {
    pub index: u32,
    pub filename: String,
    pub first_capture_sequence: u64,
    pub last_capture_sequence: u64,
    pub record_count: u64,
    pub byte_size: u64,
    pub sha256: String,
    pub utc_start_unix_millis: u64,
    pub utc_end_unix_millis: u64,
    pub completion_status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CaptureManifest {
    pub capture_id: Uuid,
    pub capture_status: CaptureStatus,
    pub utc_start_unix_millis: u64,
    pub utc_end_unix_millis: Option<u64>,
    pub monotonic_origin_nanos: u64,
    pub schema_version: u32,
    pub configuration: BTreeMap<String, String>,
    pub configuration_sha256: String,
    pub git_commit: String,
    pub dirty_build: bool,
    pub package_version: String,
    pub rust_target: String,
    pub collector_label: String,
    pub queue_capacity: usize,
    pub queue_high_water_mark: usize,
    pub approximate_owned_event_bytes: u64,
    pub rotation_max_duration_millis: u64,
    pub rotation_max_bytes: u64,
    pub configured_markets: Vec<String>,
    pub resolved_venue_markets: Vec<ResolvedVenueMarket>,
    pub data_quality: DataQualityCounters,
    pub segments: Vec<SegmentManifestEntry>,
}

impl CaptureManifest {
    pub(crate) fn read(directory: &Path) -> Result<Self, io::Error> {
        let bytes = fs::read(directory.join("manifest.json"))?;
        serde_json::from_slice(&bytes).map_err(io::Error::other)
    }

    pub(crate) fn new(
        capture_id: Uuid,
        started_at_unix_millis: u64,
        queue_capacity: usize,
        limits: SegmentLimits,
        metadata: CaptureMetadata,
    ) -> Result<Self, io::Error> {
        let configuration_bytes =
            serde_json::to_vec(&metadata.sanitized_configuration).map_err(io::Error::other)?;
        let configuration_sha256 = hex_sha256(&configuration_bytes);
        let rotation_max_duration_millis = u64::try_from(limits.max_duration().as_millis())
            .map_err(|_| io::Error::other("rotation duration exceeds manifest range"))?;
        Ok(Self {
            capture_id,
            capture_status: CaptureStatus::IncompleteProcessCrash,
            utc_start_unix_millis: started_at_unix_millis,
            utc_end_unix_millis: None,
            monotonic_origin_nanos: 0,
            schema_version: 1,
            configuration: metadata.sanitized_configuration,
            configuration_sha256,
            git_commit: metadata.git_commit,
            dirty_build: metadata.dirty_build,
            package_version: metadata.package_version,
            rust_target: metadata.rust_target,
            collector_label: metadata.collector_label,
            queue_capacity,
            queue_high_water_mark: 0,
            approximate_owned_event_bytes: 0,
            rotation_max_duration_millis,
            rotation_max_bytes: limits.max_bytes().get(),
            configured_markets: metadata.configured_markets,
            resolved_venue_markets: metadata.resolved_venue_markets,
            data_quality: DataQualityCounters::default(),
            segments: Vec::new(),
        })
    }

    pub(crate) fn add_segment(&mut self, segment: FinalizedSegment) {
        self.segments.push(segment.into());
    }

    pub(crate) fn write_atomic(&self, directory: &Path) -> Result<(), io::Error> {
        let temporary_path = directory.join("manifest.json.tmp");
        let manifest_path = directory.join("manifest.json");
        let bytes = serde_json::to_vec_pretty(self).map_err(io::Error::other)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary_path, &manifest_path)?;
        File::open(directory)?.sync_all()?;
        Ok(())
    }
}

impl From<FinalizedSegment> for SegmentManifestEntry {
    fn from(segment: FinalizedSegment) -> Self {
        Self {
            index: segment.index,
            filename: segment.filename,
            first_capture_sequence: segment.first_capture_sequence,
            last_capture_sequence: segment.last_capture_sequence,
            record_count: segment.record_count,
            byte_size: segment.byte_size,
            sha256: encode_hex(&segment.sha256),
            utc_start_unix_millis: segment.started_at_unix_millis,
            utc_end_unix_millis: segment.ended_at_unix_millis,
            completion_status: match segment.completion {
                SegmentCompletion::Complete => "complete",
                SegmentCompletion::RecoveredAfterCrash => "recovered_after_crash",
            }
            .into(),
        }
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    encode_hex(&Sha256::digest(bytes))
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
