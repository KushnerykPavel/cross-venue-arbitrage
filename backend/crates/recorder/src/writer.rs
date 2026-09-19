use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::StoredEventV1;
use crate::format::{
    HEADER_LENGTH_U64, RecordEncodeError, SegmentHeader, encode_record, sha256_file,
    sync_directory, write_header,
};

const DEFAULT_MAX_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentLimits {
    max_duration: Duration,
    max_bytes: NonZeroU64,
    sync_interval: Duration,
}

impl SegmentLimits {
    pub const fn new(
        max_duration: Duration,
        max_bytes: NonZeroU64,
        sync_interval: Duration,
    ) -> Self {
        Self {
            max_duration,
            max_bytes,
            sync_interval,
        }
    }

    pub const fn max_duration(self) -> Duration {
        self.max_duration
    }

    pub const fn max_bytes(self) -> NonZeroU64 {
        self.max_bytes
    }

    pub const fn sync_interval(self) -> Duration {
        self.sync_interval
    }
}

impl Default for SegmentLimits {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(5 * 60),
            NonZeroU64::new(DEFAULT_MAX_BYTES).expect("default segment size is positive"),
            Duration::from_secs(1),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentCompletion {
    Complete,
    RecoveredAfterCrash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizedSegment {
    pub index: u32,
    pub filename: String,
    pub first_capture_sequence: u64,
    pub last_capture_sequence: u64,
    pub record_count: u64,
    pub byte_size: u64,
    pub sha256: [u8; 32],
    pub started_at_unix_millis: u64,
    pub ended_at_unix_millis: u64,
    pub completion: SegmentCompletion,
}

#[derive(Debug)]
pub struct SegmentWriter {
    directory: PathBuf,
    capture_id: Uuid,
    next_segment_index: u32,
    next_capture_sequence: u64,
    limits: SegmentLimits,
    active: Option<ActiveSegment>,
}

#[derive(Debug)]
struct ActiveSegment {
    index: u32,
    open_path: PathBuf,
    writer: BufWriter<File>,
    started_at: SystemTime,
    started_at_unix_millis: u64,
    last_sync_at: SystemTime,
    first_capture_sequence: u64,
    last_capture_sequence: u64,
    record_count: u64,
    byte_size: u64,
}

#[derive(Debug)]
pub enum SegmentWriterError {
    Io(io::Error),
    Serialization(postcard::Error),
    PayloadTooLarge(usize),
    SequenceMismatch { expected: u64, actual: u64 },
    SequenceOverflow,
    ByteSizeOverflow,
    RecordCountOverflow,
    SegmentIndexOverflow,
    TimeBeforeUnixEpoch,
}

impl SegmentWriter {
    pub fn create(
        directory: impl Into<PathBuf>,
        capture_id: Uuid,
        limits: SegmentLimits,
    ) -> Result<Self, SegmentWriterError> {
        let directory = directory.into();
        fs::create_dir_all(&directory).map_err(SegmentWriterError::Io)?;
        Ok(Self {
            directory,
            capture_id,
            next_segment_index: 0,
            next_capture_sequence: 1,
            limits,
            active: None,
        })
    }

    pub fn append(
        &mut self,
        event: &StoredEventV1,
        now: SystemTime,
    ) -> Result<Option<FinalizedSegment>, SegmentWriterError> {
        if event.capture_sequence() != self.next_capture_sequence {
            return Err(SegmentWriterError::SequenceMismatch {
                expected: self.next_capture_sequence,
                actual: event.capture_sequence(),
            });
        }
        let record = encode_record(event).map_err(|error| match error {
            RecordEncodeError::Postcard(error) => SegmentWriterError::Serialization(error),
            RecordEncodeError::PayloadTooLarge(length) => {
                SegmentWriterError::PayloadTooLarge(length)
            }
        })?;
        let record_length = u64::try_from(record.len())
            .map_err(|_| SegmentWriterError::PayloadTooLarge(record.len()))?;
        let next_capture_sequence = event
            .capture_sequence()
            .checked_add(1)
            .ok_or(SegmentWriterError::SequenceOverflow)?;

        let finalized = if self.should_rotate(now, record_length) {
            self.finalize_active(now, SegmentCompletion::Complete)?
        } else {
            None
        };
        if self.active.is_none() {
            self.active = Some(self.open_segment(now, event.capture_sequence())?);
        }

        let active = self.active.as_mut().expect("segment was opened");
        let byte_size = active
            .byte_size
            .checked_add(record_length)
            .ok_or(SegmentWriterError::ByteSizeOverflow)?;
        let record_count = active
            .record_count
            .checked_add(1)
            .ok_or(SegmentWriterError::RecordCountOverflow)?;
        active
            .writer
            .write_all(&record)
            .map_err(SegmentWriterError::Io)?;
        active.byte_size = byte_size;
        active.last_capture_sequence = event.capture_sequence();
        active.record_count = record_count;
        self.next_capture_sequence = next_capture_sequence;

        if elapsed_at_least(active.last_sync_at, now, self.limits.sync_interval) {
            active.writer.flush().map_err(SegmentWriterError::Io)?;
            active
                .writer
                .get_ref()
                .sync_data()
                .map_err(SegmentWriterError::Io)?;
            active.last_sync_at = now;
        }
        Ok(finalized)
    }

    pub fn finish(
        mut self,
        now: SystemTime,
    ) -> Result<Option<FinalizedSegment>, SegmentWriterError> {
        self.finalize_active(now, SegmentCompletion::Complete)
    }

    pub const fn capture_id(&self) -> Uuid {
        self.capture_id
    }

    pub const fn next_capture_sequence(&self) -> u64 {
        self.next_capture_sequence
    }

    fn should_rotate(&self, now: SystemTime, next_record_length: u64) -> bool {
        let Some(active) = &self.active else {
            return false;
        };
        if active.record_count == 0 {
            return false;
        }
        elapsed_at_least(active.started_at, now, self.limits.max_duration)
            || active.byte_size.saturating_add(next_record_length) > self.limits.max_bytes.get()
    }

    fn open_segment(
        &mut self,
        now: SystemTime,
        first_capture_sequence: u64,
    ) -> Result<ActiveSegment, SegmentWriterError> {
        let index = self.next_segment_index;
        self.next_segment_index = self
            .next_segment_index
            .checked_add(1)
            .ok_or(SegmentWriterError::SegmentIndexOverflow)?;
        let started_at_unix_millis = unix_millis(now)?;
        let open_path = self.directory.join(open_filename(index));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&open_path)
            .map_err(SegmentWriterError::Io)?;
        let mut writer = BufWriter::new(file);
        write_header(
            &mut writer,
            SegmentHeader {
                capture_id: self.capture_id,
                segment_index: index,
                created_at_unix_millis: started_at_unix_millis,
            },
        )
        .map_err(SegmentWriterError::Io)?;
        writer.flush().map_err(SegmentWriterError::Io)?;
        writer
            .get_ref()
            .sync_data()
            .map_err(SegmentWriterError::Io)?;
        sync_directory(&self.directory).map_err(SegmentWriterError::Io)?;
        Ok(ActiveSegment {
            index,
            open_path,
            writer,
            started_at: now,
            started_at_unix_millis,
            last_sync_at: now,
            first_capture_sequence,
            last_capture_sequence: first_capture_sequence,
            record_count: 0,
            byte_size: HEADER_LENGTH_U64,
        })
    }

    fn finalize_active(
        &mut self,
        now: SystemTime,
        completion: SegmentCompletion,
    ) -> Result<Option<FinalizedSegment>, SegmentWriterError> {
        let Some(mut active) = self.active.take() else {
            return Ok(None);
        };
        let ended_at_unix_millis = unix_millis(now)?;
        active.writer.flush().map_err(SegmentWriterError::Io)?;
        active
            .writer
            .get_ref()
            .sync_data()
            .map_err(SegmentWriterError::Io)?;
        drop(active.writer);

        let sha256 = sha256_file(&active.open_path).map_err(SegmentWriterError::Io)?;
        let filename = log_filename(active.index);
        let log_path = self.directory.join(&filename);
        fs::rename(&active.open_path, &log_path).map_err(SegmentWriterError::Io)?;
        sync_directory(&self.directory).map_err(SegmentWriterError::Io)?;
        Ok(Some(FinalizedSegment {
            index: active.index,
            filename,
            first_capture_sequence: active.first_capture_sequence,
            last_capture_sequence: active.last_capture_sequence,
            record_count: active.record_count,
            byte_size: active.byte_size,
            sha256,
            started_at_unix_millis: active.started_at_unix_millis,
            ended_at_unix_millis,
            completion,
        }))
    }
}

pub(crate) fn open_filename(index: u32) -> String {
    format!("segment-{index:06}.open")
}

pub(crate) fn log_filename(index: u32) -> String {
    format!("segment-{index:06}.log")
}

pub(crate) fn unix_millis(time: SystemTime) -> Result<u64, SegmentWriterError> {
    let millis = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SegmentWriterError::TimeBeforeUnixEpoch)?
        .as_millis();
    u64::try_from(millis).map_err(|_| SegmentWriterError::TimeBeforeUnixEpoch)
}

fn elapsed_at_least(start: SystemTime, end: SystemTime, duration: Duration) -> bool {
    end.duration_since(start)
        .is_ok_and(|elapsed| elapsed >= duration)
}

impl Display for SegmentWriterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "segment I/O failed: {error}"),
            Self::Serialization(error) => {
                write!(formatter, "cannot serialize StoredEventV1: {error}")
            }
            Self::PayloadTooLarge(length) => {
                write!(
                    formatter,
                    "record payload of {length} bytes exceeds framing"
                )
            }
            Self::SequenceMismatch { expected, actual } => write!(
                formatter,
                "non-contiguous Capture Sequence: expected {expected}, received {actual}"
            ),
            Self::SequenceOverflow => formatter.write_str("Capture Sequence overflow"),
            Self::ByteSizeOverflow => formatter.write_str("segment byte-size overflow"),
            Self::RecordCountOverflow => formatter.write_str("segment record-count overflow"),
            Self::SegmentIndexOverflow => formatter.write_str("segment index overflow"),
            Self::TimeBeforeUnixEpoch => {
                formatter.write_str("segment time is outside the supported UTC range")
            }
        }
    }
}

impl Error for SegmentWriterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Serialization(error) => Some(error),
            _ => None,
        }
    }
}

pub(crate) fn ensure_log_extension(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "log")
}
