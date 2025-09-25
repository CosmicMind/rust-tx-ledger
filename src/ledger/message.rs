//! Message types flowing through the ledger pipeline.
//!
//! # Design
//! - `DataMessage`: validated entry delivered to partitions via the broker.
//!   - Contains a single `Entry` which is already parsed and validated.
//!   - Provides convenience accessors for client ID and entry reference.
//!
//! - `DlqMessage`: invalid or rejected rows routed to the dead-letter queue.
//!   - Includes the original `StringRecord` for replay or analysis.
//!   - Attaches a `Cursor` for tracing file position (line/byte).
//!   - Encapsulates a `DlqReason` to describe why the row failed.
//!
//! - `DlqReason`: structured reasons for rejection.
//!   - Distinguishes between deserialization errors and validation errors.
//!   - Allows downstream systems to group or report errors consistently.
//!
//! # Flow
//! Provider → Broker → Partitions → PartitionWriter  
//! └──────────────→ DlqMessage → DlqWriter
//!
//! # Notes
//! - DataMessages are assumed valid and ready to apply.
//! - DlqMessages preserve the raw record and reason to enable replay,
//!   debugging, or external monitoring.
//! - Cursor is included to support tracing and potential replay features.

use csv::StringRecord;

use crate::ledger::account::ClientId;
use crate::ledger::cursor::Cursor;
use crate::ledger::entry::{Entry, EntryType, EntryValidateError};

#[derive(Debug)]
pub struct DataMessage {
    entry: Entry,
}

impl DataMessage {
    #[inline]
    pub fn new(entry: Entry) -> Self {
        Self { entry }
    }

    #[inline]
    pub fn entry(&self) -> &Entry {
        &self.entry
    }

    #[inline]
    pub fn client_id(&self) -> ClientId {
        self.entry.client_id()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DlqReason {
    #[error("csv deserialize: {0}")]
    CsvDeserialize(String),

    #[error("missing amount: {0}")]
    MissingAmount(EntryType),

    #[error("zero amount")]
    ZeroAmount,

    #[error("negative amount")]
    NegativeAmount,

    #[error("amount not allowed for {0:?}")]
    AmountNotAllowed(EntryType),
}

impl From<csv::Error> for DlqReason {
    fn from(e: csv::Error) -> Self {
        DlqReason::CsvDeserialize(e.to_string())
    }
}

impl From<EntryValidateError> for DlqReason {
    fn from(e: EntryValidateError) -> Self {
        use EntryValidateError::*;
        match e {
            MissingAmount(kind) => DlqReason::MissingAmount(kind),
            ZeroAmount => DlqReason::ZeroAmount,
            NegativeAmount => DlqReason::NegativeAmount,
            AmountNotAllowed(kind) => DlqReason::AmountNotAllowed(kind),
        }
    }
}

#[derive(Debug)]
pub struct DlqMessage {
    cursor: Cursor,
    record: StringRecord,
    reason: DlqReason,
}

impl DlqMessage {
    #[inline]
    pub fn new(cursor: Cursor, record: StringRecord, reason: DlqReason) -> Self {
        Self {
            cursor,
            record,
            reason,
        }
    }

    #[inline]
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    #[inline]
    pub fn record(&self) -> &StringRecord {
        &self.record
    }

    #[inline]
    pub fn reason(&self) -> &DlqReason {
        &self.reason
    }
}
