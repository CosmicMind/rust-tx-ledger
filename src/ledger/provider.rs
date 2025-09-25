//! Provider: reads CSV, validates rows, and publishes messages.
//!
//! # Role
//! Single source of truth for ingest. Turns raw CSV rows into either:
//! - `DataMessage` (valid, ready for the Broker), or
//! - `DlqMessage` (invalid row with reason + original fields).
//!
//! # Ordering
//! The Provider is the *only* producer. It uses a bounded channel and
//! `blocking_send` to preserve input order into the Broker.
//!
//! # Cursor semantics
//! We capture `csv::Position` **before** reading each record so the cursor
//! points at the start of the row (useful for rewinds and precise DLQ logs).
//!
//! > **Note:** Currently the `cursor` is only attached to `DlqMessage`. If
//! > replay, re-ingest, or fine-grained tracing become requirements, it’s
//! > straightforward to extend `DataMessage` to carry the cursor as well.
//!
//! # Backpressure
//! If downstream slows, `blocking_send` will block the blocking thread,
//! keeping memory bounded and preserving order.

use std::path::PathBuf;

use csv::{ReaderBuilder, StringRecord};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::ledger::cursor::Cursor;
use crate::ledger::entry::{Entry, WireEntry};
use crate::ledger::message::{DataMessage, DlqMessage};

/// Errors that can occur while producing messages from the CSV source.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// CSV parsing/IO error from the `csv` crate.
    #[error("csv error {0}")]
    Csv(#[from] csv::Error),

    /// Downstream broker channel was closed while sending a valid row.
    #[error("broker send error: {0}")]
    BrokerSend(#[from] SendError<DataMessage>),

    /// DLQ channel was closed while sending an invalid row.
    #[error("dlq send error: {0}")]
    DlqSend(#[from] SendError<DlqMessage>),
}

pub type ProviderResult<T> = Result<T, ProviderError>;
pub type ProviderHandle = JoinHandle<ProviderResult<()>>;

/// CSV-backed message producer.
///
/// Owns the senders for the data and DLQ channels. Exposes the corresponding
/// receivers via `with_consumer_receivers`, which avoids leaking/cloning the
/// senders outside.
pub struct Provider {
    path: PathBuf,
    data_tx: Sender<DataMessage>,
    dlq_tx: Sender<DlqMessage>,
}

impl Provider {
    /// Construct a `Provider` and return it **plus** the consumer receivers.
    ///
    /// The Provider retains ownership of the senders, so only it can publish.
    /// This prevents accidental multi-producer scenarios and helps preserve
    /// ordering guarantees.
    pub fn with_consumer_receivers(
        path: PathBuf,
        data_message_capacity: usize,
        dlq_message_capacity: usize,
    ) -> (Self, Receiver<DataMessage>, Receiver<DlqMessage>) {
        let (data_tx, data_rx) = mpsc::channel::<DataMessage>(data_message_capacity);
        let (dlq_tx, dlq_rx) = mpsc::channel::<DlqMessage>(dlq_message_capacity);

        let provider = Self {
            path,
            data_tx,
            dlq_tx,
        };

        (provider, data_rx, dlq_rx)
    }

    /// Spawn the blocking CSV reader on Tokio’s blocking pool.
    ///
    /// # Concurrency
    /// Runs on a dedicated blocking thread so it won’t starve async tasks.
    /// Uses `blocking_send` on bounded channels to apply backpressure.
    ///
    /// # Errors
    /// - Returns `ProviderError::Csv` for CSV IO/parse issues.
    /// - Returns `ProviderError::BrokerSend` / `DlqSend` if the respective
    ///   consumer has dropped.
    pub fn spawn_blocking(self) -> ProviderHandle {
        tokio::task::spawn_blocking(move || -> ProviderResult<()> {
            let mut reader = ReaderBuilder::new()
                .has_headers(true)
                .trim(csv::Trim::All)
                .flexible(true)
                .from_path(&self.path)?;

            let mut record = StringRecord::new();
            let headers = reader.headers()?.clone();
            let data_tx = self.data_tx;
            let dlq_tx = self.dlq_tx;

            loop {
                // capture start-of-record position *before* reading
                let pos = reader.position().clone();

                match reader.read_record(&mut record) {
                    Ok(true) => {
                        let cursor = Cursor::from(pos);
                        process_record(&cursor, &record, &headers, &data_tx, &dlq_tx)?
                    }
                    Ok(false) => break, // EOF
                    Err(err) => return Err(err.into()),
                }

                // reuse the same buffer; avoid per-row allocations
                record.clear();
            }

            Ok(())
        })
    }
}

//// Validate and route a single CSV row.
///
/// - If CSV → `WireEntry` succeeds and `WireEntry` → `Entry` validation passes,
///   publish a `DataMessage`.
/// - Otherwise, publish a `DlqMessage` with the original fields and reason.
///
/// > **Note:** Currently only DLQ messages carry the cursor. If you want
/// > partitions or writers to reconstruct/replay original rows, change the
/// > `DataMessage::new` call to also accept `cursor`.
fn process_record(
    cursor: &Cursor,
    record: &StringRecord,
    headers: &StringRecord,
    data_tx: &Sender<DataMessage>,
    dlq_tx: &Sender<DlqMessage>,
) -> ProviderResult<()> {
    match record.deserialize::<WireEntry>(Some(headers)) {
        Ok(wire_entry) => match Entry::try_from(wire_entry) {
            Ok(entry) => data_tx.blocking_send(DataMessage::new(entry))?,
            Err(err) => dlq_tx.blocking_send(DlqMessage::new(
                cursor.clone(),
                record.to_owned(),
                err.into(),
            ))?,
        },
        Err(err) => {
            dlq_tx.blocking_send(DlqMessage::new(
                cursor.clone(),
                record.to_owned(),
                err.into(),
            ))?;
        }
    }

    Ok(())
}
