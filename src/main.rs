//! # rust-tx-ledger
//!
//! A streaming transaction ledger with ordered routing by client ID.
//!
//! ## Pipeline
//! Provider (CSV) → Broker (fan-out) → Partitions (per-client state)
//! → PartitionWriter (accounts.csv → stdout)
//! └───────────────────────────────→ DlqWriter (dlq.csv)
//!
//! ## Invariants
//! - Input order is preserved by the Provider using `blocking_send`.
//! - The Broker is the sole sender per partition, guaranteeing FIFO ordering.
//! - Amounts are floored to 4 decimal places *on output*; arithmetic keeps full
//!   precision.
//! - Duplicate `tx` IDs per client are ignored (idempotency).
//!
//! ## Concurrency
//! - Runs on the Tokio runtime.
//! - Uses bounded `mpsc` channels for backpressure.
//! - No `unsafe` code.
//!
//! ## CLI
//! Configurable options:
//! - `--data-message-capacity`: Broker queue capacity (default: 1024).
//! - `--partition-message-capacity`: Per-partition queue capacity (default:
//!   1024).
//! - `--partition-thread-count`: Number of partitions (default: num_cpus, min
//!   2).
//! - `--dlq-message-capacity`: DlqWriter queue capacity (default: 1024).
//! - `--dlq-output-path`: Output path for the DlqWriter CSV (default:
//!   `dlq.csv`).
//!
//! ## Example
//! ```bash
//! cargo run -- transactions.csv > accounts.csv
//! # produces dlq.csv with any invalid rows
//! ```

use std::{cmp, error};

use futures::future::join_all;

mod cli;
mod ledger;
mod prelude;

use crate::prelude::*;

/// Program entrypoint.
///
/// # Flow
/// 1. Parse CLI arguments (`Cli::parse()`).
/// 2. Build the Provider (CSV reader), Broker, Partitions, and DlqWriter.
/// 3. Spawn each component onto the Tokio runtime:
///    - Provider runs in a blocking task, preserving input order.
///    - Broker runs async, fanning out messages to partitions.
///    - Each Partition runs async, maintaining account state per client.
///    - DlqWriter runs async, writing bad rows to `dlq.csv`.
/// 4. Join all tasks with `tokio::join!`.
/// 5. Collect final account states from partitions via oneshot channels.
/// 6. Pass those states to the PartitionWriter, which outputs `accounts.csv`.
///
/// # Notes
/// - Bounded channels provide backpressure between each stage.
/// - Order is guaranteed per client, not globally.
/// - All errors propagate up and are reported cleanly.
///
/// # Example
/// ```bash
/// cargo run -- transactions.csv > accounts.csv
/// ```
#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error>> {
    let args = Cli::parse();
    let file_input_path = args.file_input_path;
    let dlq_output_path = args.dlq_output_path;
    let data_message_capacity = args.data_message_capacity;
    let dlq_message_capacity = args.dlq_message_capacity;
    let partition_message_capacity = args.partition_message_capacity;
    let partition_thread_count = args.partition_thread_count;

    let partition_count = cmp::max(2, partition_thread_count);

    let (provider, data_rx, dlq_rx) = Provider::with_consumer_receivers(
        file_input_path,
        data_message_capacity,
        dlq_message_capacity,
    );
    let dlq_write = DlqWriter::new(dlq_rx, dlq_output_path);
    let (broker, partitions) =
        Broker::with_partition_receivers(data_rx, partition_message_capacity, partition_count);

    let mut partition_handles = Vec::with_capacity(partition_count);
    let mut partition_done_rxs = Vec::with_capacity(partition_count);
    for (partition, done_rx) in partitions {
        partition_handles.push(partition.spawn());
        partition_done_rxs.push(done_rx);
    }

    let dlq_writer_handle = dlq_write.spawn();
    let broker_handle = broker.spawn();
    let provider_handle = provider.spawn_blocking();

    let (partition_res, dlq_writer_res, broker_res, provider_res) = tokio::join!(
        join_all(partition_handles),
        dlq_writer_handle,
        broker_handle,
        provider_handle
    );

    for res in partition_res {
        res??;
    }

    dlq_writer_res??;
    broker_res??;
    provider_res??;

    PartitionWriter::run(partition_done_rxs).await?;

    Ok(())
}
