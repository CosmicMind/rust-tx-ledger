//! DlqWriter: collects invalid or rejected rows and persists them to `dlq.csv`.
//!
//! # Design
//! - Consumes `DlqMessage`s from a bounded channel.
//! - Each message includes cursor position, failure reason, and the raw row.
//! - Rows are re-serialized back to CSV for faithful reproduction.
//!
//! # Behavior
//! - On spawn, creates a CSV file at the provided path (overwrites if exists).
//! - Always writes a header: `line,byte,reason,raw`.
//! - Each subsequent record captures where the bad row appeared in the input
//!   and why it was rejected.
//! - Flushes at the end to ensure durability.
//!
//! # Shutdown
//! - Writer exits cleanly when the channel closes (i.e. when the Provider has
//!   no more rows to send).
//!
//! # Notes
//! - DLQ output is *only* produced if invalid rows exist. If none are sent, the
//!   file will still be created with just the header row.
//! - Keeping output as CSV makes it easy to re-ingest or inspect downstream.
//!
//! # Example Output
//! ```csv
//! line,byte,reason,raw
//! 5,85,"csv deserialize: unknown variant `1`","1,3,0.5"
//! 7,117,"zero amount","withdrawal,1,5,0"
//! 9,165,"negative amount","withdrawal,2,7,-0.10"
//! ```

use std::fs::{File, create_dir_all};
use std::path::PathBuf;

use csv::{StringRecord, Writer, WriterBuilder};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ledger::message::DlqMessage;

#[derive(Debug, thiserror::Error)]
pub enum DlqWriterError {
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type DlqWriterResult<T> = Result<T, DlqWriterError>;
pub type DlqWriterHandle = JoinHandle<DlqWriterResult<()>>;

pub struct DlqWriter {
    dql_rx: mpsc::Receiver<DlqMessage>,
    path: PathBuf,
}

impl DlqWriter {
    /// Construct a new `DlqWriter`.
    ///
    /// # Parameters
    /// - `dlq_rx`: the channel from which dead-letter messages will be
    ///   consumed.
    /// - `path`: file path where the DLQ CSV will be written.
    ///
    /// # Notes
    /// This constructor is intentionally minimal — it only wires the inputs.
    /// Actual writing is started by calling [`spawn`].
    #[inline]
    pub fn new(dql_rx: mpsc::Receiver<DlqMessage>, path: PathBuf) -> Self {
        Self { dql_rx, path }
    }

    /// Spawn this writer as a Tokio task.
    ///
    /// Consumes `self` and runs the writer loop (`run`) in the background.
    ///
    /// # Behavior
    /// - Opens the output CSV at the configured path.
    /// - Writes a header row on startup.
    /// - Appends each DLQ message as a new row until the channel is closed.
    ///
    /// # Returns
    /// A `JoinHandle` that resolves to the writer result when awaited.
    pub fn spawn(self) -> DlqWriterHandle {
        tokio::spawn(Self::run(self))
    }

    /// Runs the DLQ writer loop.
    ///
    /// # Lifecycle
    /// - Creates or overwrites the configured CSV file.
    /// - Writes the header row: `line,byte,reason,raw`.
    /// - Consumes `DlqMessage` values from `dql_rx` until the channel closes.
    /// - Each message is serialized back into a CSV row with its reason and raw
    ///   data.
    /// - Flushes the writer once the channel is exhausted.
    ///
    /// # Shutdown
    /// - Clean exit when the sender (Provider) is dropped and the channel is
    ///   closed.
    /// - Returns `Ok(())` after final flush.
    ///
    /// # Errors
    /// - Returns `DlqWriterError::Csv` on CSV serialization errors.
    /// - Returns `DlqWriterError::Io` if file creation or writing fails.
    async fn run(self) -> DlqWriterResult<()> {
        // Ensure parent dirs exist *before* creating the file.
        if let Some(parent) = self.path.parent() {
            create_dir_all(parent)?;
        }

        let file = File::create(&self.path)?;
        let mut writer = Writer::from_writer(file);
        let mut dql_rx = self.dql_rx;

        writer.write_record(["line", "byte", "reason", "raw"])?;

        while let Some(message) = dql_rx.recv().await {
            let raw = record_as_csv(message.record())?;

            writer.write_record(&[
                message.cursor().line().to_string(),
                message.cursor().byte().to_string(),
                message.reason().to_string(),
                raw,
            ])?;
        }

        writer.flush()?;

        Ok(())
    }
}

/// Serialize a `StringRecord` back into a single CSV row string.
///
/// # Why not just `join(",")`?
/// Using `csv::Writer` ensures:
/// - Proper escaping of quotes and commas inside fields.
/// - Exact round-tripping of the raw row as CSV.
/// - Consistency with how rows are written elsewhere.
///
/// # Behavior
/// - Disables headers (`has_headers(false)`).
/// - Trims the trailing newline from the writer output.
/// - Returns a clean, one-line string suitable for embedding in DLQ logs.
///
/// # Errors
/// - Returns `csv::Error` if writing or flushing fails.
fn record_as_csv(record: &StringRecord) -> DlqWriterResult<String> {
    let mut buf = Vec::new();
    {
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut buf);

        writer.write_record(record.iter())?;
        writer.flush()?;
    }

    Ok(String::from_utf8_lossy(&buf)
        .trim_end_matches('\n')
        .to_string())
}
