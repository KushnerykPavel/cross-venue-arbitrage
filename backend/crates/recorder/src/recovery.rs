use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;
use std::time::SystemTime;

use uuid::Uuid;

use crate::format::{
    HEADER_LENGTH_U64, HeaderError, read_header, read_record, sha256_file, sync_directory,
};
use crate::writer::{FinalizedSegment, SegmentCompletion, log_filename, unix_millis};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryOutcome {
    Finalized {
        segment: FinalizedSegment,
        truncated_bytes: u64,
    },
    DiscardedEmpty {
        segment_index: u32,
        truncated_bytes: u64,
    },
}

#[derive(Debug)]
pub enum RecoveryError {
    NotOpenSegment,
    Io(io::Error),
    InvalidHeader(String),
    CaptureIdMismatch { expected: Uuid, actual: Uuid },
    SegmentIndexMismatch { expected: u32, actual: u32 },
    FinalizedTargetExists(String),
    TimeBeforeUnixEpoch,
}

pub fn recover_open_segment(
    path: &Path,
    expected_capture_id: Uuid,
    expected_segment_index: u32,
    recovered_at: SystemTime,
) -> Result<RecoveryOutcome, RecoveryError> {
    if path.extension().is_none_or(|extension| extension != "open") {
        return Err(RecoveryError::NotOpenSegment);
    }
    let directory = path.parent().ok_or_else(|| {
        RecoveryError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "open segment has no parent directory",
        ))
    })?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(RecoveryError::Io)?;
    let original_length = file.metadata().map_err(RecoveryError::Io)?.len();
    let header = read_header(&mut file).map_err(map_header_error)?;
    if header.capture_id != expected_capture_id {
        return Err(RecoveryError::CaptureIdMismatch {
            expected: expected_capture_id,
            actual: header.capture_id,
        });
    }
    if header.segment_index != expected_segment_index {
        return Err(RecoveryError::SegmentIndexMismatch {
            expected: expected_segment_index,
            actual: header.segment_index,
        });
    }
    let ended_at_unix_millis = unix_millis(recovered_at).map_err(|error| match error {
        crate::SegmentWriterError::TimeBeforeUnixEpoch => RecoveryError::TimeBeforeUnixEpoch,
        crate::SegmentWriterError::Io(error) => RecoveryError::Io(error),
        other => RecoveryError::Io(io::Error::other(other)),
    })?;

    let mut last_valid_offset = HEADER_LENGTH_U64;
    let mut first_sequence = None;
    let mut last_sequence = None;
    let mut record_count = 0_u64;
    let mut expected_sequence = None;
    while let Ok(Some(record)) = read_record(&mut file, original_length) {
        let (event, end_offset) = record;
        if event.capture_sequence() == 0
            || expected_sequence.is_some_and(|expected| expected != event.capture_sequence())
        {
            break;
        }
        first_sequence.get_or_insert(event.capture_sequence());
        last_sequence = Some(event.capture_sequence());
        record_count = record_count.saturating_add(1);
        last_valid_offset = end_offset;
        let Some(next) = event.capture_sequence().checked_add(1) else {
            break;
        };
        expected_sequence = Some(next);
    }

    let truncated_bytes = original_length.saturating_sub(last_valid_offset);
    file.set_len(last_valid_offset).map_err(RecoveryError::Io)?;
    file.sync_data().map_err(RecoveryError::Io)?;
    drop(file);

    let Some(first_capture_sequence) = first_sequence else {
        fs::remove_file(path).map_err(RecoveryError::Io)?;
        sync_directory(directory).map_err(RecoveryError::Io)?;
        return Ok(RecoveryOutcome::DiscardedEmpty {
            segment_index: header.segment_index,
            truncated_bytes: original_length.saturating_sub(HEADER_LENGTH_U64),
        });
    };
    let last_capture_sequence = last_sequence.expect("first record also establishes last record");
    let sha256 = sha256_file(path).map_err(RecoveryError::Io)?;
    let filename = log_filename(header.segment_index);
    let log_path = directory.join(&filename);
    if log_path.exists() {
        return Err(RecoveryError::FinalizedTargetExists(filename));
    }
    fs::rename(path, &log_path).map_err(RecoveryError::Io)?;
    sync_directory(directory).map_err(RecoveryError::Io)?;
    Ok(RecoveryOutcome::Finalized {
        segment: FinalizedSegment {
            index: header.segment_index,
            filename,
            first_capture_sequence,
            last_capture_sequence,
            record_count,
            byte_size: last_valid_offset,
            sha256,
            started_at_unix_millis: header.created_at_unix_millis,
            ended_at_unix_millis,
            completion: SegmentCompletion::RecoveredAfterCrash,
        },
        truncated_bytes,
    })
}

fn map_header_error(error: HeaderError) -> RecoveryError {
    match error {
        HeaderError::Io(error) => RecoveryError::Io(error),
        other => RecoveryError::InvalidHeader(other.to_string()),
    }
}

impl Display for RecoveryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotOpenSegment => formatter.write_str("recovery accepts only .open segments"),
            Self::Io(error) => write!(formatter, "segment recovery I/O failed: {error}"),
            Self::InvalidHeader(detail) => write!(formatter, "invalid segment header: {detail}"),
            Self::CaptureIdMismatch { expected, actual } => write!(
                formatter,
                "segment Capture Run mismatch: expected {expected}, received {actual}"
            ),
            Self::SegmentIndexMismatch { expected, actual } => write!(
                formatter,
                "segment index mismatch: expected {expected}, received {actual}"
            ),
            Self::FinalizedTargetExists(filename) => {
                write!(formatter, "recovery target already exists: {filename}")
            }
            Self::TimeBeforeUnixEpoch => {
                formatter.write_str("recovery time is outside the supported UTC range")
            }
        }
    }
}

impl Error for RecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
