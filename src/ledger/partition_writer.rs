//! PartitionWriter: collects account states from partitions and writes them
//! out.
//!
//! # Design
//! - Consumes `oneshot::Receiver<HashMap<ClientId, Account>>` from each
//!   partition.
//! - Each partition owns a disjoint set of clients, so the writer simply
//!   serializes what it receives. No merging logic or invariant checks here.
//! - Output is written once to stdout in CSV format.
//!
//! # Output
//! The CSV includes one row per client account with the following headers:
//!
//! ```text
//! client,available,held,total,locked
//! ```
//!
//! Amounts are serialized with 4 decimal places (floor). `locked` is a boolean.
//!
//! # Concurrency
//! - All partition receivers are awaited concurrently (`FuturesUnordered`).
//! - Writer yields rows as each partition completes, avoiding head-of-line
//!   blocking from a slow partition.
//!
//! # Shutdown
//! - Ends cleanly once all partitions send their account maps.
//! - Returns an error if any partition fails to send (`PartitionDropped`).

use std::collections::HashMap;
use std::io;

use csv::Writer;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::oneshot;

use crate::ledger::account::{Account, ClientId};

#[derive(Debug, thiserror::Error)]
pub enum PartitionWriterError {
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("partition dropped before sending accounts")]
    PartitionDropped,
}

pub type PartitionWriterResult<T> = Result<T, PartitionWriterError>;

pub struct PartitionWriter;

impl PartitionWriter {
    /// Write final account states from all partitions to stdout.
    /// Single responsibility: serialize what it receives. No merging, no
    /// checks.
    pub async fn run(
        done_rxs: Vec<oneshot::Receiver<HashMap<ClientId, Account>>>,
    ) -> PartitionWriterResult<()> {
        let stdout = io::stdout();
        let mut writer = Writer::from_writer(stdout.lock());

        // header first
        writer.write_record(["client", "available", "held", "total", "locked"])?;

        // collect concurrently, write rows as each partition completes
        let mut pending = FuturesUnordered::from_iter(done_rxs.into_iter().map(|done_rx| async {
            done_rx
                .await
                .map_err(|_| PartitionWriterError::PartitionDropped)
        }));

        while let Some(map_res) = pending.next().await {
            let map = map_res?;
            for (client_id, account) in map {
                writer.write_record(&[
                    client_id.to_string(),
                    account.available().to_string(),
                    account.held().to_string(),
                    account.total().to_string(),
                    account.is_locked().to_string(),
                ])?;
            }
        }

        writer.flush()?;
        Ok(())
    }
}
