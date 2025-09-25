//! Broker: routes validated entries to partitions.
//!
//! # Role in the pipeline
//! - **Input:** consumes `DataMessage`s from the Provider.
//! - **Work:** hashes each message by `client_id` to select a partition.
//! - **Output:** forwards to exactly one partition’s channel.
//!
//! # Invariants
//! - A client id always maps to the same partition (`client_id %
//!   partition_count`).
//! - Broker is the **sole sender** per partition, so FIFO order is preserved.
//! - If a partition closes unexpectedly, Broker exits with `PartitionClosed`
//!   (this should not occur in normal operation).
//!
//! # Lifecycle
//! - When the Provider closes its channel, the Broker drains remaining messages
//!   and then drops its partition senders.
//! - Dropping the senders signals partitions to finish and return their
//!   results.
//!
//! # Design notes
//! - `with_partition_receivers` wires everything at once: it builds each
//!   partition with its receiver and returns both the `Broker` and the
//!   partition list (with oneshot result receivers).
//! - Spawning is async via Tokio; shutdown is clean and coordinated by channel
//!   lifetimes.
//!
//! # Future work
//! - Could replace simple modulo hashing with a more sophisticated sharding
//!   strategy if client distribution is skewed.
//! - Metrics could be added here (queue depth, send latency) with `tracing`.

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ledger::message::DataMessage;
use crate::ledger::partition::{Partition, PartitionDoneReceiver};

/// Errors specific to the broker loop.
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("partition closed")]
    PartitionClosed,
}

pub type BrokerResult = Result<(), BrokerError>;
pub type BrokerHandle = JoinHandle<BrokerResult>;

/// The set of partitions paired with their result receivers.
///
/// Each partition is returned with a oneshot receiver so that the
/// `PartitionWriter` can collect the final account maps.
pub type BrokerPartions = Vec<(Partition, PartitionDoneReceiver)>;

/// Broker routes messages to partitions by client id.
///
/// Holds the inbound receiver from the Provider and a sender for each
/// partition.
pub struct Broker {
    data_rx: mpsc::Receiver<DataMessage>,
    entry_txs: Vec<mpsc::Sender<DataMessage>>,
}

impl Broker {
    /// Build a `Broker` together with its partitions (and their result
    /// receivers).
    ///
    /// # Why here?
    /// Centralizing construction guarantees:
    /// - The Broker becomes the **sole owner** of each partition's sender. That
    ///   preserves FIFO per partition and prevents accidental cloning
    ///   elsewhere.
    /// - Each `Partition` is created with its own mpsc receiver and a oneshot
    ///   for returning its final account map to the `PartitionWriter`.
    ///
    /// # Parameters
    /// - `data_rx`: the single input stream from `Provider` carrying validated
    ///   `DataMessage`s.
    /// - `partition_message_capacity`: per-partition channel capacity (bounded
    ///   => backpressure).
    /// - `partition_count`: number of partitions (must be ≥ 1).
    ///
    /// # Returns
    /// - `(Broker, Vec<(Partition, PartitionDoneReceiver)>)`:
    ///   - The `Broker` holding all partition senders.
    ///   - A vector of partitions paired with the oneshot receivers used by
    ///     `PartitionWriter`.
    ///
    /// # Panics
    /// Panics if `partition_count == 0` (a broker needs at least one
    /// partition).
    pub fn with_partition_receivers(
        data_rx: mpsc::Receiver<DataMessage>,
        partition_message_capacity: usize,
        partition_count: usize,
    ) -> (Self, BrokerPartions) {
        assert!(0 < partition_count, "Broker requires at least 1 partition");

        let mut entry_txs = Vec::with_capacity(partition_count);
        let mut partitions = Vec::with_capacity(partition_count);

        for _ in 0..partition_count {
            let (entry_tx, entry_rx) = mpsc::channel(partition_message_capacity);
            entry_txs.push(entry_tx);
            partitions.push(Partition::with_done_receiver(entry_rx));
        }

        let broker = Self { data_rx, entry_txs };
        (broker, partitions)
    }

    /// Spawn the broker on the Tokio runtime.
    ///
    /// Equivalent to `tokio::spawn(Self::run(self))`. Prefer this to keep the
    /// ownership story simple (the broker consumes itself and owns its
    /// senders).
    pub fn spawn(self) -> BrokerHandle {
        tokio::spawn(Self::run(self))
    }

    /// Main routing loop: read from `data_rx` and forward to the chosen
    /// partition.
    ///
    /// # Routing rule
    /// `partition_idx = client_id % entry_txs.len()`. All entries for the same
    /// client go to the same partition, preserving per-client order.
    ///
    /// # Backpressure
    /// Uses `send(message).await` on the partition channel. If a partition’s
    /// queue is full, the broker **awaits** until capacity frees up. This keeps
    /// memory bounded and naturally slows the upstream provider via its own
    /// bounded channel.
    ///
    /// # Shutdown
    /// When `data_rx` closes (provider finished), the loop ends and the
    /// function returns `Ok(())`. Dropping the broker’s partition senders
    /// (on return) signals partitions to finish and send their results.
    ///
    /// # Errors
    /// Returns `BrokerError::PartitionClosed` if a partition task shuts down
    /// early and its channel is closed while the broker still has messages.
    async fn run(self) -> BrokerResult {
        let mut data_rx = self.data_rx;
        let entry_txs = self.entry_txs;

        while let Some(message) = data_rx.recv().await {
            let partition_idx = (message.client_id() as usize) % entry_txs.len();

            if entry_txs[partition_idx].send(message).await.is_err() {
                return Err(BrokerError::PartitionClosed);
            }
        }

        Ok(())
    }
}
