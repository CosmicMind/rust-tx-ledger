//! Partition: manages a mutually exclusive list of clients and their accounts.
//!
//! # Role
//! Each Partition is responsible for applying transactions for a disjoint set
//! of clients, guaranteeing per-client order and isolation.
//!
//! # Design
//! - Accounts are stored in a HashMap keyed by client ID.
//! - The broker routes each client deterministically to one partition.
//! - Each Partition runs as a background task under Tokio.
//!
//! # Shutdown
//! - Exits when the Broker closes its sender.
//! - Sends its final AccountsMap through a one-shot channel to the
//!   PartitionWriter.

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::ledger::account::{AccountError, AccountsMap, ClientId};
use crate::ledger::entry::EntryType;
use crate::ledger::message::DataMessage;

#[derive(Debug, thiserror::Error)]
pub enum PartitionError {
    #[error("apply error client {client} tx {tx}: {source}")]
    Apply {
        client: ClientId,
        tx: u32,

        #[source]
        source: AccountError,
    },
}

pub type PartitionResult<T> = Result<T, PartitionError>;
pub type PartitionHandle = JoinHandle<PartitionResult<()>>;
pub type PartitionDoneReceiver = oneshot::Receiver<AccountsMap>;

pub struct Partition {
    entry_rx: mpsc::Receiver<DataMessage>,
    accounts: AccountsMap,
    done_tx: oneshot::Sender<AccountsMap>,
}

impl Partition {
    /// Constructs a Partition and returns both the instance and a
    /// `oneshot::Receiver` for its final AccountsMap.
    ///
    /// # Design
    /// - Owns its one-shot sender privately to avoid misuse or cloning.
    /// - External callers only see the receiver side.
    /// - Receives transactions via `entry_rx` and consumes that stream in
    ///   `run()`.
    pub fn with_done_receiver(
        entry_rx: mpsc::Receiver<DataMessage>,
    ) -> (Self, PartitionDoneReceiver) {
        let (done_tx, done_rx) = oneshot::channel();

        let partition = Self {
            entry_rx,
            accounts: AccountsMap::new(),
            done_tx,
        };

        (partition, done_rx)
    }

    /// Spawn this partition as a Tokio task.
    ///
    /// Consumes `self` and runs the partition loop (`run`) in the background.
    ///
    /// # Why
    /// Keeps the ownership model clean:
    /// - `self` owns the account map and entry receiver.
    /// - By consuming `self`, only the spawned task can drive this partition,
    ///   avoiding accidental use elsewhere.
    ///
    /// # Returns
    /// A `JoinHandle` that resolves to the partition’s result when awaited.
    pub fn spawn(self) -> PartitionHandle {
        tokio::spawn(Self::run(self))
    }

    /// Runs the main partition loop.
    ///
    /// # Concurrency
    /// - Processes transactions sequentially per client (HashMap keyed by
    ///   client_id).
    /// - Deduplication and validation should already be enforced upstream.
    ///
    /// # Shutdown
    /// - Terminates cleanly when the `entry_rx` channel is closed by the
    ///   Broker.
    /// - Dispatches its final AccountsMap via one-shot before returning.
    ///
    /// # Errors
    /// - Ignores common account errors (locked, insufficient funds, duplicate
    ///   disputes, etc).
    /// - Returns `PartitionError::Apply` only if an unexpected validation
    ///   breach occurs (should not happen in normal operation).
    async fn run(self) -> PartitionResult<()> {
        let mut entry_rx = self.entry_rx;
        let mut accounts = self.accounts;

        while let Some(message) = entry_rx.recv().await {
            let entry = message.entry();
            let client_id = entry.client_id();
            let account = accounts.entry(client_id).or_default();

            let result = match entry.kind() {
                EntryType::Deposit => account.deposit(entry),
                EntryType::Withdrawal => account.withdrawal(entry),
                EntryType::Dispute => account.dispute(entry),
                EntryType::Chargeback => account.chargeback(entry),
                EntryType::Resolve => account.resolve(entry),
            };

            match result {
                Ok(()) => { /* applied */ }
                Err(err) => {
                    let ignorable = matches!(
                        err,
                        AccountError::Locked
                            | AccountError::Insufficient
                            | AccountError::TxNotFound
                            | AccountError::AlreadyDisputed
                            | AccountError::NotDisputed
                            | AccountError::NotDisputable
                            | AccountError::MissingAmount
                    );

                    if ignorable {
                        continue;
                    }

                    // should never arrive here. Received data is already validated upon
                    // arrival.
                    return Err(PartitionError::Apply {
                        client: client_id,
                        tx: entry.tx(),
                        source: err,
                    });
                }
            }
        }

        // oneshot message sent when `extry_rx` is closed.
        let _ = self.done_tx.send(accounts);

        Ok(())
    }
}
