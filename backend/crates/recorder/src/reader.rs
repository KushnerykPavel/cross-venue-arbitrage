use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io;
use std::path::Path;

use uuid::Uuid;

use crate::StoredEventV1;
use crate::format::{HeaderError, read_header, read_record, sha256_file};
use crate::writer::ensure_log_extension;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadSegment {
    pub capture_id: Uuid,
    pub segment_index: u32,
    pub created_at_unix_millis: u64,
    pub byte_size: u64,
    pub sha256: [u8; 32],
    pub events: Vec<StoredEventV1>,
}

#[derive(Debug)]
pub enum SegmentReadError {
    NotFinalizedLog,
    Io(io::Error),
    InvalidHeader(String),
    CaptureIdMismatch { expected: Uuid, actual: Uuid },
    SegmentIndexMismatch { expected: u32, actual: u32 },
    CorruptRecord(String),
    ZeroCaptureSequence,
    SequenceMismatch { expected: u64, actual: u64 },
    SequenceOverflow,
    EmptySegment,
}

pub fn read_segment(
    path: &Path,
    expected_capture_id: Uuid,
    expected_segment_index: u32,
) -> Result<ReadSegment, SegmentReadError> {
    if !ensure_log_extension(path) {
        return Err(SegmentReadError::NotFinalizedLog);
    }
    let mut file = File::open(path).map_err(SegmentReadError::Io)?;
    let byte_size = file.metadata().map_err(SegmentReadError::Io)?.len();
    let header = read_header(&mut file).map_err(map_header_error)?;
    if header.capture_id != expected_capture_id {
        return Err(SegmentReadError::CaptureIdMismatch {
            expected: expected_capture_id,
            actual: header.capture_id,
        });
    }
    if header.segment_index != expected_segment_index {
        return Err(SegmentReadError::SegmentIndexMismatch {
            expected: expected_segment_index,
            actual: header.segment_index,
        });
    }

    let mut events = Vec::new();
    let mut expected_sequence = None;
    while let Some((event, _end_offset)) = read_record(&mut file, byte_size)
        .map_err(|error| SegmentReadError::CorruptRecord(error.to_string()))?
    {
        if event.capture_sequence() == 0 {
            return Err(SegmentReadError::ZeroCaptureSequence);
        }
        if let Some(expected) = expected_sequence
            && event.capture_sequence() != expected
        {
            return Err(SegmentReadError::SequenceMismatch {
                expected,
                actual: event.capture_sequence(),
            });
        }
        expected_sequence = Some(
            event
                .capture_sequence()
                .checked_add(1)
                .ok_or(SegmentReadError::SequenceOverflow)?,
        );
        events.push(event);
    }
    let sha256 = sha256_file(path).map_err(SegmentReadError::Io)?;
    if events.is_empty() {
        return Err(SegmentReadError::EmptySegment);
    }
    Ok(ReadSegment {
        capture_id: header.capture_id,
        segment_index: header.segment_index,
        created_at_unix_millis: header.created_at_unix_millis,
        byte_size,
        sha256,
        events,
    })
}

fn map_header_error(error: HeaderError) -> SegmentReadError {
    match error {
        HeaderError::Io(error) => SegmentReadError::Io(error),
        other => SegmentReadError::InvalidHeader(other.to_string()),
    }
}

impl Display for SegmentReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFinalizedLog => {
                formatter.write_str("replay accepts only finalized .log files")
            }
            Self::Io(error) => write!(formatter, "segment read failed: {error}"),
            Self::InvalidHeader(detail) => write!(formatter, "invalid segment header: {detail}"),
            Self::CaptureIdMismatch { expected, actual } => write!(
                formatter,
                "segment Capture Run mismatch: expected {expected}, received {actual}"
            ),
            Self::SegmentIndexMismatch { expected, actual } => write!(
                formatter,
                "segment index mismatch: expected {expected}, received {actual}"
            ),
            Self::CorruptRecord(detail) => write!(formatter, "corrupt segment record: {detail}"),
            Self::ZeroCaptureSequence => formatter.write_str("Capture Sequence must start at one"),
            Self::SequenceMismatch { expected, actual } => write!(
                formatter,
                "non-contiguous Capture Sequence: expected {expected}, received {actual}"
            ),
            Self::SequenceOverflow => formatter.write_str("Capture Sequence overflow"),
            Self::EmptySegment => formatter.write_str("finalized segment contains no records"),
        }
    }
}

impl Error for SegmentReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}
