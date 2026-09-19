use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::{
    CaptureManifest, CaptureStatus, SegmentReadError, StorageConversionError, read_segment,
};

const SUPPORTED_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ValidationOptions {
    pub allow_incomplete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedReplayEvent {
    pub capture_sequence: u64,
    pub event: market_data::NormalizedMarketEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedCapture {
    directory: PathBuf,
    manifest: CaptureManifest,
    events: Vec<ValidatedReplayEvent>,
}

#[derive(Debug)]
pub enum CaptureValidationError {
    Io(io::Error),
    UnsupportedManifestSchema(u32),
    IncompleteCapture(CaptureStatus),
    ConfigurationHashMismatch,
    SegmentIndexMismatch {
        expected: u32,
        actual: u32,
    },
    SegmentFilenameMismatch {
        expected: String,
        actual: String,
    },
    UnlistedLogFile(String),
    MissingLogFile(String),
    SegmentRead(SegmentReadError),
    SegmentHashMismatch(u32),
    SegmentByteSizeMismatch(u32),
    SegmentRecordCountMismatch(u32),
    SegmentSequenceRangeMismatch(u32),
    InvalidSegmentCompletion {
        index: u32,
        value: String,
    },
    CaptureSequenceMismatch {
        expected: u64,
        actual: u64,
    },
    StorageConversion {
        capture_sequence: u64,
        source: StorageConversionError,
    },
}

impl ValidatedCapture {
    pub fn open(
        directory: impl Into<PathBuf>,
        options: ValidationOptions,
    ) -> Result<Self, CaptureValidationError> {
        let directory = directory.into();
        let manifest = CaptureManifest::read(&directory).map_err(CaptureValidationError::Io)?;
        validate_manifest(&manifest, options)?;
        validate_configuration_hash(&manifest)?;
        validate_log_inventory(&directory, &manifest)?;

        let mut events = Vec::new();
        let mut expected_capture_sequence = 1_u64;
        for (position, segment) in manifest.segments.iter().enumerate() {
            let expected_index = u32::try_from(position).map_err(|_| {
                CaptureValidationError::SegmentIndexMismatch {
                    expected: u32::MAX,
                    actual: segment.index,
                }
            })?;
            if segment.index != expected_index {
                return Err(CaptureValidationError::SegmentIndexMismatch {
                    expected: expected_index,
                    actual: segment.index,
                });
            }
            let expected_filename = format!("segment-{expected_index:06}.log");
            if segment.filename != expected_filename {
                return Err(CaptureValidationError::SegmentFilenameMismatch {
                    expected: expected_filename,
                    actual: segment.filename.clone(),
                });
            }
            if segment.completion_status != "complete"
                && segment.completion_status != "recovered_after_crash"
            {
                return Err(CaptureValidationError::InvalidSegmentCompletion {
                    index: expected_index,
                    value: segment.completion_status.clone(),
                });
            }
            if manifest.capture_status == CaptureStatus::Complete
                && segment.completion_status != "complete"
            {
                return Err(CaptureValidationError::InvalidSegmentCompletion {
                    index: expected_index,
                    value: segment.completion_status.clone(),
                });
            }
            let read = read_segment(
                &directory.join(&segment.filename),
                manifest.capture_id,
                expected_index,
            )
            .map_err(CaptureValidationError::SegmentRead)?;
            if encode_hex(&read.sha256) != segment.sha256 {
                return Err(CaptureValidationError::SegmentHashMismatch(expected_index));
            }
            if read.byte_size != segment.byte_size {
                return Err(CaptureValidationError::SegmentByteSizeMismatch(
                    expected_index,
                ));
            }
            if read.events.len() as u128 != u128::from(segment.record_count) {
                return Err(CaptureValidationError::SegmentRecordCountMismatch(
                    expected_index,
                ));
            }
            let first = read
                .events
                .first()
                .expect("read_segment rejects empty segments")
                .capture_sequence();
            let last = read
                .events
                .last()
                .expect("read_segment rejects empty segments")
                .capture_sequence();
            if first != segment.first_capture_sequence || last != segment.last_capture_sequence {
                return Err(CaptureValidationError::SegmentSequenceRangeMismatch(
                    expected_index,
                ));
            }
            for stored in read.events {
                let actual = stored.capture_sequence();
                if actual != expected_capture_sequence {
                    return Err(CaptureValidationError::CaptureSequenceMismatch {
                        expected: expected_capture_sequence,
                        actual,
                    });
                }
                let event = stored.to_normalized().map_err(|source| {
                    CaptureValidationError::StorageConversion {
                        capture_sequence: actual,
                        source,
                    }
                })?;
                events.push(ValidatedReplayEvent {
                    capture_sequence: actual,
                    event,
                });
                expected_capture_sequence = actual.checked_add(1).ok_or(
                    CaptureValidationError::CaptureSequenceMismatch {
                        expected: u64::MAX,
                        actual,
                    },
                )?;
            }
        }
        Ok(Self {
            directory,
            manifest,
            events,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub const fn manifest(&self) -> &CaptureManifest {
        &self.manifest
    }

    pub fn events(&self) -> &[ValidatedReplayEvent] {
        &self.events
    }

    pub fn into_events(self) -> Vec<ValidatedReplayEvent> {
        self.events
    }
}

fn validate_manifest(
    manifest: &CaptureManifest,
    options: ValidationOptions,
) -> Result<(), CaptureValidationError> {
    if manifest.schema_version != SUPPORTED_MANIFEST_SCHEMA_VERSION {
        return Err(CaptureValidationError::UnsupportedManifestSchema(
            manifest.schema_version,
        ));
    }
    if manifest.capture_status != CaptureStatus::Complete && !options.allow_incomplete {
        return Err(CaptureValidationError::IncompleteCapture(
            manifest.capture_status,
        ));
    }
    Ok(())
}

fn validate_configuration_hash(manifest: &CaptureManifest) -> Result<(), CaptureValidationError> {
    let bytes = serde_json::to_vec(&manifest.configuration)
        .map_err(io::Error::other)
        .map_err(CaptureValidationError::Io)?;
    if encode_hex(&Sha256::digest(bytes)) != manifest.configuration_sha256 {
        return Err(CaptureValidationError::ConfigurationHashMismatch);
    }
    Ok(())
}

fn validate_log_inventory(
    directory: &Path,
    manifest: &CaptureManifest,
) -> Result<(), CaptureValidationError> {
    let listed = manifest
        .segments
        .iter()
        .map(|segment| segment.filename.as_str())
        .collect::<BTreeSet<_>>();
    for filename in &listed {
        if !directory.join(filename).is_file() {
            return Err(CaptureValidationError::MissingLogFile((*filename).into()));
        }
    }
    for entry in fs::read_dir(directory).map_err(CaptureValidationError::Io)? {
        let entry = entry.map_err(CaptureValidationError::Io)?;
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "log") {
            let filename = entry.file_name().to_string_lossy().into_owned();
            if !listed.contains(filename.as_str()) {
                return Err(CaptureValidationError::UnlistedLogFile(filename));
            }
        }
    }
    Ok(())
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

impl Display for CaptureValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "capture validation I/O failed: {error}"),
            Self::UnsupportedManifestSchema(version) => {
                write!(formatter, "unsupported manifest schema version {version}")
            }
            Self::IncompleteCapture(status) => write!(
                formatter,
                "capture status is {status:?}; pass --allow-incomplete to replay it explicitly"
            ),
            Self::ConfigurationHashMismatch => {
                formatter.write_str("manifest configuration SHA-256 does not match")
            }
            Self::SegmentIndexMismatch { expected, actual } => write!(
                formatter,
                "segment index is not contiguous: expected {expected}, received {actual}"
            ),
            Self::SegmentFilenameMismatch { expected, actual } => write!(
                formatter,
                "segment filename mismatch: expected {expected}, received {actual}"
            ),
            Self::UnlistedLogFile(filename) => {
                write!(formatter, "capture contains unlisted log file {filename}")
            }
            Self::MissingLogFile(filename) => {
                write!(formatter, "manifest segment is missing: {filename}")
            }
            Self::SegmentRead(error) => Display::fmt(error, formatter),
            Self::SegmentHashMismatch(index) => {
                write!(formatter, "segment {index} SHA-256 does not match manifest")
            }
            Self::SegmentByteSizeMismatch(index) => {
                write!(
                    formatter,
                    "segment {index} byte size does not match manifest"
                )
            }
            Self::SegmentRecordCountMismatch(index) => {
                write!(
                    formatter,
                    "segment {index} record count does not match manifest"
                )
            }
            Self::SegmentSequenceRangeMismatch(index) => write!(
                formatter,
                "segment {index} Capture Sequence range does not match manifest"
            ),
            Self::InvalidSegmentCompletion { index, value } => write!(
                formatter,
                "segment {index} has invalid completion status {value:?}"
            ),
            Self::CaptureSequenceMismatch { expected, actual } => write!(
                formatter,
                "non-contiguous Capture Sequence: expected {expected}, received {actual}"
            ),
            Self::StorageConversion {
                capture_sequence,
                source,
            } => write!(
                formatter,
                "Capture Sequence {capture_sequence} cannot be restored: {source}"
            ),
        }
    }
}

impl Error for CaptureValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::SegmentRead(error) => Some(error),
            Self::StorageConversion { source, .. } => Some(source),
            _ => None,
        }
    }
}
