use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::Path;

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::StoredEventV1;

pub(crate) const MAGIC: [u8; 8] = *b"CVARLOG\0";
pub(crate) const FORMAT_VERSION: u16 = 1;
pub(crate) const HEADER_LENGTH: u16 = 40;
pub(crate) const HEADER_LENGTH_U64: u64 = HEADER_LENGTH as u64;
pub(crate) const RECORD_OVERHEAD: u64 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentHeader {
    pub capture_id: Uuid,
    pub segment_index: u32,
    pub created_at_unix_millis: u64,
}

pub(crate) fn write_header(
    writer: &mut impl Write,
    header: SegmentHeader,
) -> Result<(), io::Error> {
    writer.write_all(&MAGIC)?;
    writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&HEADER_LENGTH.to_le_bytes())?;
    writer.write_all(header.capture_id.as_bytes())?;
    writer.write_all(&header.segment_index.to_le_bytes())?;
    writer.write_all(&header.created_at_unix_millis.to_le_bytes())?;
    Ok(())
}

pub(crate) fn read_header(reader: &mut impl Read) -> Result<SegmentHeader, HeaderError> {
    let mut bytes = [0_u8; HEADER_LENGTH as usize];
    reader.read_exact(&mut bytes).map_err(HeaderError::Io)?;
    if bytes[..8] != MAGIC {
        return Err(HeaderError::InvalidMagic);
    }
    let version = u16::from_le_bytes([bytes[8], bytes[9]]);
    if version != FORMAT_VERSION {
        return Err(HeaderError::UnsupportedVersion(version));
    }
    let header_length = u16::from_le_bytes([bytes[10], bytes[11]]);
    if header_length != HEADER_LENGTH {
        return Err(HeaderError::InvalidHeaderLength(header_length));
    }
    let capture_id = Uuid::from_slice(&bytes[12..28]).map_err(HeaderError::InvalidCaptureId)?;
    let segment_index = u32::from_le_bytes(bytes[28..32].try_into().expect("fixed slice"));
    let created_at_unix_millis = u64::from_le_bytes(bytes[32..40].try_into().expect("fixed slice"));
    Ok(SegmentHeader {
        capture_id,
        segment_index,
        created_at_unix_millis,
    })
}

pub(crate) fn encode_record(event: &StoredEventV1) -> Result<Vec<u8>, RecordEncodeError> {
    let payload = postcard::to_allocvec(event).map_err(RecordEncodeError::Postcard)?;
    let payload_length = u32::try_from(payload.len())
        .map_err(|_| RecordEncodeError::PayloadTooLarge(payload.len()))?;
    let mut record = Vec::with_capacity(payload.len() + RECORD_OVERHEAD as usize);
    record.extend_from_slice(&payload_length.to_le_bytes());
    record.extend_from_slice(&payload);
    record.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    Ok(record)
}

pub(crate) fn read_record(
    reader: &mut (impl Read + Seek),
    file_length: u64,
) -> Result<Option<(StoredEventV1, u64)>, RecordReadError> {
    let offset = reader.stream_position().map_err(RecordReadError::Io)?;
    if offset == file_length {
        return Ok(None);
    }
    let remaining = file_length.saturating_sub(offset);
    if remaining < RECORD_OVERHEAD {
        return Err(RecordReadError::Truncated { offset });
    }

    let mut length_bytes = [0_u8; 4];
    reader
        .read_exact(&mut length_bytes)
        .map_err(RecordReadError::Io)?;
    let payload_length = u64::from(u32::from_le_bytes(length_bytes));
    let record_length = payload_length
        .checked_add(RECORD_OVERHEAD)
        .ok_or(RecordReadError::InvalidLength { offset })?;
    if record_length > remaining {
        return Err(RecordReadError::Truncated { offset });
    }
    let payload_length_usize =
        usize::try_from(payload_length).map_err(|_| RecordReadError::InvalidLength { offset })?;
    let mut payload = vec![0_u8; payload_length_usize];
    reader
        .read_exact(&mut payload)
        .map_err(RecordReadError::Io)?;
    let mut checksum_bytes = [0_u8; 4];
    reader
        .read_exact(&mut checksum_bytes)
        .map_err(RecordReadError::Io)?;
    let expected = u32::from_le_bytes(checksum_bytes);
    let actual = crc32c::crc32c(&payload);
    if actual != expected {
        return Err(RecordReadError::ChecksumMismatch {
            offset,
            expected,
            actual,
        });
    }
    let event = postcard::from_bytes(&payload)
        .map_err(|source| RecordReadError::InvalidPayload { offset, source })?;
    let end_offset = offset + record_length;
    Ok(Some((event, end_offset)))
}

pub(crate) fn sha256_file(path: &Path) -> Result<[u8; 32], io::Error> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), io::Error> {
    File::open(path)?.sync_all()
}

#[derive(Debug)]
pub(crate) enum HeaderError {
    Io(io::Error),
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidHeaderLength(u16),
    InvalidCaptureId(uuid::Error),
}

#[derive(Debug)]
pub(crate) enum RecordEncodeError {
    Postcard(postcard::Error),
    PayloadTooLarge(usize),
}

#[derive(Debug)]
pub(crate) enum RecordReadError {
    Io(io::Error),
    Truncated {
        offset: u64,
    },
    InvalidLength {
        offset: u64,
    },
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },
    InvalidPayload {
        offset: u64,
        source: postcard::Error,
    },
}

impl Display for HeaderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "cannot read segment header: {error}"),
            Self::InvalidMagic => formatter.write_str("invalid segment magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported segment format version {version}")
            }
            Self::InvalidHeaderLength(length) => {
                write!(formatter, "invalid segment header length {length}")
            }
            Self::InvalidCaptureId(error) => write!(formatter, "invalid Capture Run UUID: {error}"),
        }
    }
}

impl Display for RecordEncodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Postcard(error) => write!(formatter, "cannot serialize StoredEventV1: {error}"),
            Self::PayloadTooLarge(length) => {
                write!(
                    formatter,
                    "record payload of {length} bytes exceeds u32 framing"
                )
            }
        }
    }
}

impl Display for RecordReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "cannot read record: {error}"),
            Self::Truncated { offset } => write!(formatter, "truncated record at offset {offset}"),
            Self::InvalidLength { offset } => {
                write!(formatter, "invalid record length at offset {offset}")
            }
            Self::ChecksumMismatch {
                offset,
                expected,
                actual,
            } => write!(
                formatter,
                "CRC32C mismatch at offset {offset}: stored {expected:#010x}, computed {actual:#010x}"
            ),
            Self::InvalidPayload { offset, source } => {
                write!(
                    formatter,
                    "invalid Postcard payload at offset {offset}: {source}"
                )
            }
        }
    }
}

impl Error for HeaderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidCaptureId(error) => Some(error),
            _ => None,
        }
    }
}

impl Error for RecordEncodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Postcard(error) => Some(error),
            Self::PayloadTooLarge(_) => None,
        }
    }
}

impl Error for RecordReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidPayload { source, .. } => Some(source),
            _ => None,
        }
    }
}
