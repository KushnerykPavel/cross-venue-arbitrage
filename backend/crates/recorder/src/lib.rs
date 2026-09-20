mod capture;
mod dto;
mod format;
mod reader;
mod recovery;
mod validated;
mod writer;

pub use dto::{
    StorageConversionError, StoredAggressorSideClassificationV1, StoredAggressorSideV1,
    StoredBookLevelV1, StoredDecimalV1, StoredEventV1, StoredExchangeTimeKindV1,
    StoredExchangeTimeObservationV1, StoredExchangeTimeUnitV1, StoredMarketTradeKindV1,
    StoredMarketTradeReportingKindV1, StoredPayloadV1, StoredSourceIdV1,
    StoredUnavailabilityCategoryV1, StoredVenueV1,
};
pub use manifest::{
    CaptureManifest, CaptureMetadata, CaptureStatus, DataQualityCounters, ResolvedVenueMarket,
    SegmentManifestEntry,
};
pub use reader::{
    ReadSegment, ReadSegmentSummary, SegmentReadError, read_segment, read_segment_streaming,
};
pub use recovery::{RecoveryError, RecoveryOutcome, recover_open_segment};
pub use validated::{
    CaptureValidationError, ValidatedCapture, ValidatedReplayEvent, ValidationOptions,
};
pub use writer::{
    FinalizedSegment, SegmentCompletion, SegmentLimits, SegmentWriter, SegmentWriterError,
};

mod manifest;
#[cfg(test)]
mod tests;
pub use capture::{
    AcceptedMarketEvent, CaptureCoordinator, CaptureCoordinatorError, CaptureSettings,
};
