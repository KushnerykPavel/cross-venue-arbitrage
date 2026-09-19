use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError};
use market_data::{NormalizedMarketEvent, UnavailabilityCategory};
use uuid::Uuid;

use crate::manifest::{CaptureManifest, CaptureMetadata, CaptureStatus, DataQualityCounters};
use crate::writer::{
    FinalizedSegment, SegmentCompletion, SegmentLimits, SegmentWriter, SegmentWriterError,
    unix_millis,
};
use crate::{
    RecoveryOutcome, StorageConversionError, StoredEventV1, read_segment, recover_open_segment,
};

pub struct CaptureSettings {
    pub data_dir: PathBuf,
    pub queue_capacity: NonZeroUsize,
    pub segment_limits: SegmentLimits,
    pub metadata: CaptureMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedMarketEvent {
    pub capture_sequence: u64,
    pub event: NormalizedMarketEvent,
}

pub struct CaptureCoordinator {
    capture_id: Uuid,
    capture_directory: PathBuf,
    sender: Sender<WriterCommand>,
    writer_status: Receiver<String>,
    writer_thread: Option<JoinHandle<Result<CaptureManifest, CaptureCoordinatorError>>>,
    next_capture_sequence: u64,
    queue_high_water_mark: usize,
    approximate_owned_event_bytes: u64,
    data_quality: DataQualityCounters,
    queue_full: bool,
}

enum WriterCommand {
    Event(StoredEventV1),
    Finish {
        status: CaptureStatus,
        ended_at: SystemTime,
        queue_high_water_mark: usize,
        approximate_owned_event_bytes: u64,
        data_quality: DataQualityCounters,
    },
}

#[derive(Debug)]
pub enum CaptureCoordinatorError {
    DataDirectoryMustBeAbsolute(PathBuf),
    Io(io::Error),
    StorageConversion(StorageConversionError),
    QueueFull,
    WriterFailed(String),
    WriterThreadPanicked,
    SequenceOverflow,
    AlreadyFinished,
}

impl CaptureCoordinator {
    pub fn start(
        settings: CaptureSettings,
        started_at: SystemTime,
    ) -> Result<Self, CaptureCoordinatorError> {
        if !settings.data_dir.is_absolute() {
            return Err(CaptureCoordinatorError::DataDirectoryMustBeAbsolute(
                settings.data_dir,
            ));
        }
        recover_interrupted_captures(&settings.data_dir, started_at)?;
        let capture_id = Uuid::new_v4();
        let capture_directory = settings
            .data_dir
            .join("captures")
            .join(capture_id.to_string());
        fs::create_dir_all(&capture_directory).map_err(CaptureCoordinatorError::Io)?;
        let started_at_unix_millis = unix_millis(started_at).map_err(map_writer_error)?;
        let manifest = CaptureManifest::new(
            capture_id,
            started_at_unix_millis,
            settings.queue_capacity.get(),
            settings.segment_limits,
            settings.metadata,
        )
        .map_err(CaptureCoordinatorError::Io)?;
        manifest
            .write_atomic(&capture_directory)
            .map_err(CaptureCoordinatorError::Io)?;
        let writer = SegmentWriter::create(&capture_directory, capture_id, settings.segment_limits)
            .map_err(map_writer_error)?;
        let (sender, receiver) = crossbeam_channel::bounded(settings.queue_capacity.get());
        let (status_sender, writer_status) = crossbeam_channel::bounded(1);
        let writer_directory = capture_directory.clone();
        let writer_thread = thread::Builder::new()
            .name("market-data-recorder".into())
            .spawn(move || writer_loop(receiver, status_sender, writer, manifest, writer_directory))
            .map_err(CaptureCoordinatorError::Io)?;
        Ok(Self {
            capture_id,
            capture_directory,
            sender,
            writer_status,
            writer_thread: Some(writer_thread),
            next_capture_sequence: 1,
            queue_high_water_mark: 0,
            approximate_owned_event_bytes: 0,
            data_quality: DataQualityCounters::default(),
            queue_full: false,
        })
    }

    pub fn accept(
        &mut self,
        event: NormalizedMarketEvent,
    ) -> Result<AcceptedMarketEvent, CaptureCoordinatorError> {
        self.check_writer_status()?;
        if self.queue_full {
            return Err(CaptureCoordinatorError::QueueFull);
        }
        let sequence = self.next_capture_sequence;
        let stored = StoredEventV1::from_normalized(sequence, &event)
            .map_err(CaptureCoordinatorError::StorageConversion)?;
        let approximate_bytes = u64::try_from(stored.approximate_owned_bytes()).unwrap_or(u64::MAX);
        match self.sender.try_send(WriterCommand::Event(stored)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.queue_full = true;
                return Err(CaptureCoordinatorError::QueueFull);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.check_writer_status()?;
                return Err(CaptureCoordinatorError::WriterFailed(
                    "recorder thread disconnected".into(),
                ));
            }
        }
        self.queue_high_water_mark = self.queue_high_water_mark.max(self.sender.len().max(1));
        self.approximate_owned_event_bytes = self
            .approximate_owned_event_bytes
            .saturating_add(approximate_bytes);
        observe_quality(&mut self.data_quality, &event);
        self.next_capture_sequence = sequence
            .checked_add(1)
            .ok_or(CaptureCoordinatorError::SequenceOverflow)?;
        Ok(AcceptedMarketEvent {
            capture_sequence: sequence,
            event,
        })
    }

    pub fn finish(
        mut self,
        requested_status: CaptureStatus,
        ended_at: SystemTime,
    ) -> Result<CaptureManifest, CaptureCoordinatorError> {
        let status = if self.queue_full {
            CaptureStatus::IncompleteQueueFull
        } else {
            requested_status
        };
        self.sender
            .send(WriterCommand::Finish {
                status,
                ended_at,
                queue_high_water_mark: self.queue_high_water_mark,
                approximate_owned_event_bytes: self.approximate_owned_event_bytes,
                data_quality: self.data_quality,
            })
            .map_err(|_| {
                self.writer_status
                    .try_recv()
                    .map(CaptureCoordinatorError::WriterFailed)
                    .unwrap_or_else(|_| {
                        CaptureCoordinatorError::WriterFailed(
                            "recorder thread disconnected before shutdown".into(),
                        )
                    })
            })?;
        let handle = self
            .writer_thread
            .take()
            .ok_or(CaptureCoordinatorError::AlreadyFinished)?;
        handle
            .join()
            .map_err(|_| CaptureCoordinatorError::WriterThreadPanicked)?
    }

    pub fn observe_deduplicated_trade(&mut self) -> Result<(), CaptureCoordinatorError> {
        self.check_writer_status()?;
        self.data_quality.deduplicated_trades =
            self.data_quality.deduplicated_trades.saturating_add(1);
        Ok(())
    }

    pub const fn capture_id(&self) -> Uuid {
        self.capture_id
    }

    pub fn capture_directory(&self) -> &Path {
        &self.capture_directory
    }

    fn check_writer_status(&self) -> Result<(), CaptureCoordinatorError> {
        match self.writer_status.try_recv() {
            Ok(error) => Err(CaptureCoordinatorError::WriterFailed(error)),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => {
                if self
                    .writer_thread
                    .as_ref()
                    .is_some_and(|thread| thread.is_finished())
                {
                    Err(CaptureCoordinatorError::WriterFailed(
                        "recorder thread terminated".into(),
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }
}

fn recover_interrupted_captures(
    data_dir: &Path,
    recovered_at: SystemTime,
) -> Result<(), CaptureCoordinatorError> {
    let captures_directory = data_dir.join("captures");
    if !captures_directory.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(&captures_directory).map_err(CaptureCoordinatorError::Io)? {
        let directory = entry.map_err(CaptureCoordinatorError::Io)?.path();
        if !directory.is_dir() || !directory.join("manifest.json").exists() {
            continue;
        }
        let mut manifest =
            CaptureManifest::read(&directory).map_err(CaptureCoordinatorError::Io)?;
        if manifest.capture_status != CaptureStatus::IncompleteProcessCrash {
            continue;
        }
        let index = u32::try_from(manifest.segments.len()).map_err(|_| {
            CaptureCoordinatorError::WriterFailed("recovery segment index overflow".into())
        })?;
        let open_path = directory.join(format!("segment-{index:06}.open"));
        let log_path = directory.join(format!("segment-{index:06}.log"));
        if log_path.exists() {
            let read = read_segment(&log_path, manifest.capture_id, index)
                .map_err(|error| CaptureCoordinatorError::WriterFailed(error.to_string()))?;
            let ended_at = log_path
                .metadata()
                .and_then(|metadata| metadata.modified())
                .unwrap_or(recovered_at);
            manifest.add_segment(FinalizedSegment {
                index,
                filename: format!("segment-{index:06}.log"),
                first_capture_sequence: read.events[0].capture_sequence(),
                last_capture_sequence: read
                    .events
                    .last()
                    .expect("read_segment rejects empty segments")
                    .capture_sequence(),
                record_count: u64::try_from(read.events.len()).map_err(|_| {
                    CaptureCoordinatorError::WriterFailed("recovery record count overflow".into())
                })?,
                byte_size: read.byte_size,
                sha256: read.sha256,
                started_at_unix_millis: read.created_at_unix_millis,
                ended_at_unix_millis: unix_millis(ended_at).map_err(map_writer_error)?,
                completion: SegmentCompletion::RecoveredAfterCrash,
            });
        } else if open_path.exists() {
            match recover_open_segment(&open_path, manifest.capture_id, index, recovered_at)
                .map_err(|error| CaptureCoordinatorError::WriterFailed(error.to_string()))?
            {
                RecoveryOutcome::Finalized { segment, .. } => manifest.add_segment(segment),
                RecoveryOutcome::DiscardedEmpty { .. } => {}
            }
        } else {
            continue;
        }
        manifest.utc_end_unix_millis = Some(unix_millis(recovered_at).map_err(map_writer_error)?);
        manifest
            .write_atomic(&directory)
            .map_err(CaptureCoordinatorError::Io)?;
    }
    Ok(())
}

fn writer_loop(
    receiver: Receiver<WriterCommand>,
    status_sender: Sender<String>,
    mut writer: SegmentWriter,
    mut manifest: CaptureManifest,
    directory: PathBuf,
) -> Result<CaptureManifest, CaptureCoordinatorError> {
    while let Ok(command) = receiver.recv() {
        match command {
            WriterCommand::Event(event) => {
                let now = SystemTime::now();
                match writer.append(&event, now) {
                    Ok(Some(segment)) => {
                        manifest.add_segment(segment);
                        if let Err(error) = manifest.write_atomic(&directory) {
                            return writer_failure(
                                error,
                                &status_sender,
                                &mut manifest,
                                &directory,
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return writer_failure(error, &status_sender, &mut manifest, &directory);
                    }
                }
            }
            WriterCommand::Finish {
                status,
                ended_at,
                queue_high_water_mark,
                approximate_owned_event_bytes,
                data_quality,
            } => {
                match writer.finish(ended_at) {
                    Ok(Some(segment)) => manifest.add_segment(segment),
                    Ok(None) => {}
                    Err(error) => {
                        return writer_failure(error, &status_sender, &mut manifest, &directory);
                    }
                }
                manifest.capture_status = status;
                manifest.utc_end_unix_millis = match unix_millis(ended_at) {
                    Ok(ended_at) => Some(ended_at),
                    Err(error) => {
                        return writer_failure(error, &status_sender, &mut manifest, &directory);
                    }
                };
                manifest.queue_high_water_mark = queue_high_water_mark;
                manifest.approximate_owned_event_bytes = approximate_owned_event_bytes;
                manifest.data_quality = data_quality;
                if let Err(error) = manifest.write_atomic(&directory) {
                    return writer_failure(error, &status_sender, &mut manifest, &directory);
                }
                return Ok(manifest);
            }
        }
    }
    Err(CaptureCoordinatorError::WriterFailed(
        "capture coordinator dropped without finalizing".into(),
    ))
}

fn writer_failure(
    error: impl Display,
    status_sender: &Sender<String>,
    manifest: &mut CaptureManifest,
    directory: &Path,
) -> Result<CaptureManifest, CaptureCoordinatorError> {
    let message = error.to_string();
    manifest.capture_status = CaptureStatus::IncompleteIoError;
    manifest.utc_end_unix_millis = unix_millis(SystemTime::now()).ok();
    let _ = manifest.write_atomic(directory);
    let _ = status_sender.try_send(message.clone());
    Err(CaptureCoordinatorError::WriterFailed(message))
}

fn observe_quality(counters: &mut DataQualityCounters, event: &NormalizedMarketEvent) {
    match event {
        NormalizedMarketEvent::OrderBookUnavailable(unavailable) => {
            counters.order_book_unavailable = counters.order_book_unavailable.saturating_add(1);
            if unavailable.category() == UnavailabilityCategory::Disconnected {
                counters.disconnects = counters.disconnects.saturating_add(1);
            }
            if unavailable.category() == UnavailabilityCategory::InvalidMarketData {
                counters.invalid_messages = counters.invalid_messages.saturating_add(1);
            }
        }
        NormalizedMarketEvent::TradeStreamUnavailable(unavailable) => {
            counters.trade_stream_gaps = counters.trade_stream_gaps.saturating_add(1);
            if unavailable.category() == UnavailabilityCategory::InvalidMarketData {
                counters.invalid_messages = counters.invalid_messages.saturating_add(1);
            }
        }
        _ => {}
    }
}

fn map_writer_error(error: SegmentWriterError) -> CaptureCoordinatorError {
    match error {
        SegmentWriterError::Io(error) => CaptureCoordinatorError::Io(error),
        other => CaptureCoordinatorError::WriterFailed(other.to_string()),
    }
}

impl Display for CaptureCoordinatorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataDirectoryMustBeAbsolute(path) => {
                write!(formatter, "DATA_DIR must be absolute: {}", path.display())
            }
            Self::Io(error) => write!(formatter, "capture I/O failed: {error}"),
            Self::StorageConversion(error) => Display::fmt(error, formatter),
            Self::QueueFull => formatter.write_str("recorder queue is full"),
            Self::WriterFailed(error) => write!(formatter, "recorder writer failed: {error}"),
            Self::WriterThreadPanicked => formatter.write_str("recorder thread panicked"),
            Self::SequenceOverflow => formatter.write_str("Capture Sequence overflow"),
            Self::AlreadyFinished => formatter.write_str("Capture Run is already finished"),
        }
    }
}

impl Error for CaptureCoordinatorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::StorageConversion(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    use std::time::{Duration, UNIX_EPOCH};

    use domain::{MarketCoin, Venue};
    use market_data::{LocalObservationTime, NormalizedMarketEvent, TradeStreamResumed};
    use tempfile::TempDir;

    use super::*;
    use crate::read_segment;

    fn metadata() -> CaptureMetadata {
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

    fn event(nanos: u64) -> NormalizedMarketEvent {
        NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
            Venue::Aster,
            MarketCoin::try_new("BTC").unwrap(),
            LocalObservationTime::from_nanos_since_start(nanos),
        ))
    }

    #[test]
    fn records_contiguous_events_and_finalizes_manifest() {
        let data_dir = TempDir::new().unwrap();
        let started_at = UNIX_EPOCH + Duration::from_secs(1);
        let mut coordinator = CaptureCoordinator::start(
            CaptureSettings {
                data_dir: data_dir.path().to_path_buf(),
                queue_capacity: NonZeroUsize::new(4).unwrap(),
                segment_limits: SegmentLimits::default(),
                metadata: metadata(),
            },
            started_at,
        )
        .unwrap();
        let capture_id = coordinator.capture_id();
        let capture_directory = coordinator.capture_directory().to_path_buf();

        assert_eq!(coordinator.accept(event(1)).unwrap().capture_sequence, 1);
        assert_eq!(coordinator.accept(event(2)).unwrap().capture_sequence, 2);
        let manifest = coordinator
            .finish(CaptureStatus::Complete, started_at + Duration::from_secs(1))
            .unwrap();

        assert_eq!(manifest.capture_status, CaptureStatus::Complete);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].first_capture_sequence, 1);
        assert_eq!(manifest.segments[0].last_capture_sequence, 2);
        assert!(manifest.queue_high_water_mark > 0);
        assert!(manifest.approximate_owned_event_bytes > 0);
        assert!(!capture_directory.join("manifest.json.tmp").exists());
        let persisted: CaptureManifest =
            serde_json::from_slice(&fs::read(capture_directory.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(persisted, manifest);
        let segment = read_segment(
            &capture_directory.join(&manifest.segments[0].filename),
            capture_id,
            0,
        )
        .unwrap();
        assert_eq!(segment.events.len(), 2);
    }

    #[test]
    fn queue_full_rejects_event_without_advancing_sequence() {
        let data_dir = TempDir::new().unwrap();
        let (sender, _receiver) = crossbeam_channel::bounded(1);
        let (_status_sender, writer_status) = crossbeam_channel::bounded(1);
        let mut coordinator = CaptureCoordinator {
            capture_id: Uuid::new_v4(),
            capture_directory: data_dir.path().to_path_buf(),
            sender,
            writer_status,
            writer_thread: None,
            next_capture_sequence: 1,
            queue_high_water_mark: 0,
            approximate_owned_event_bytes: 0,
            data_quality: DataQualityCounters::default(),
            queue_full: false,
        };

        assert_eq!(coordinator.accept(event(1)).unwrap().capture_sequence, 1);
        assert!(matches!(
            coordinator.accept(event(2)),
            Err(CaptureCoordinatorError::QueueFull)
        ));
        assert_eq!(coordinator.next_capture_sequence, 2);
        assert!(coordinator.queue_full);
    }

    #[test]
    fn rejects_relative_data_directory() {
        let result = CaptureCoordinator::start(
            CaptureSettings {
                data_dir: PathBuf::from("relative"),
                queue_capacity: NonZeroUsize::new(1).unwrap(),
                segment_limits: SegmentLimits::default(),
                metadata: metadata(),
            },
            UNIX_EPOCH,
        );

        assert!(matches!(
            result,
            Err(CaptureCoordinatorError::DataDirectoryMustBeAbsolute(_))
        ));
    }

    #[test]
    fn starting_new_capture_recovers_previous_open_segment_without_continuing_it() {
        let data_dir = TempDir::new().unwrap();
        let old_capture_id = Uuid::new_v4();
        let old_directory = data_dir
            .path()
            .join("captures")
            .join(old_capture_id.to_string());
        fs::create_dir_all(&old_directory).unwrap();
        let started_at = UNIX_EPOCH + Duration::from_secs(1);
        let old_manifest = CaptureManifest::new(
            old_capture_id,
            unix_millis(started_at).unwrap(),
            4,
            SegmentLimits::default(),
            metadata(),
        )
        .unwrap();
        old_manifest.write_atomic(&old_directory).unwrap();
        let mut writer =
            SegmentWriter::create(&old_directory, old_capture_id, SegmentLimits::default())
                .unwrap();
        let stored = StoredEventV1::from_normalized(1, &event(1)).unwrap();
        writer.append(&stored, started_at).unwrap();
        drop(writer);

        let new_capture = CaptureCoordinator::start(
            CaptureSettings {
                data_dir: data_dir.path().to_path_buf(),
                queue_capacity: NonZeroUsize::new(4).unwrap(),
                segment_limits: SegmentLimits::default(),
                metadata: metadata(),
            },
            started_at + Duration::from_secs(2),
        )
        .unwrap();
        assert_ne!(new_capture.capture_id(), old_capture_id);

        let recovered = CaptureManifest::read(&old_directory).unwrap();
        assert_eq!(
            recovered.capture_status,
            CaptureStatus::IncompleteProcessCrash
        );
        assert_eq!(recovered.segments.len(), 1);
        assert_eq!(
            recovered.segments[0].completion_status,
            "recovered_after_crash"
        );
        assert_eq!(recovered.segments[0].first_capture_sequence, 1);
        new_capture
            .finish(CaptureStatus::Complete, started_at + Duration::from_secs(3))
            .unwrap();
    }
}
