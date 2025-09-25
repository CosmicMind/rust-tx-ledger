//! Cli: command-line interface for configuring the ledger runtime.
//!
//! # Role
//! Provides user-tunable parameters for queue sizes, partitioning,
//! and DLQ output location. Keeps defaults sane so that the program
//! runs out-of-the-box, while still exposing full knobs for tuning.
//!
//! # Design notes
//! - Uses `clap` derive API for ergonomic parsing and `--help` output.
//! - Enforces `arg_required_else_help` so running without arguments shows
//!   usage.
//! - Path arguments use `PathBuf` for direct file-system compatibility.
//!
//! # Example
//! ```bash
//! cargo run -- transactions.csv \
//!   --data-message-capacity 2048 \
//!   --partition-thread-count 8 \
//!   --dlq-output-path /tmp/dlq.csv > accounts.csv
//! ```

use std::path::PathBuf;

use clap::Parser;

/// Command-line arguments for configuring the ledger pipeline.
#[derive(Parser, Debug)]
#[command(
    name = "rust-tx-ledger",
    author = "Jonathan Dahan",
    version = "0.1.0",
    about = "Toy transaction ledger.",
    arg_required_else_help = true
)]
pub struct Cli {
    /// Input CSV file containing transactions.
    pub file_input_path: PathBuf,

    /// Max messages in the broker’s inbound queue.
    /// Default: 1024
    #[arg(long, default_value_t = 1024)]
    pub data_message_capacity: usize,

    /// Max messages in each partition’s inbound queue.
    /// Default: 1024
    #[arg(long, default_value_t = 1024)]
    pub partition_message_capacity: usize,

    /// Number of partitions (workers). Defaults to CPU count, but at least 2.
    #[arg(long, default_value_t = num_cpus::get())]
    pub partition_thread_count: usize,

    /// Max messages in the DLQ writer queue.
    /// Default: 1024
    #[arg(long, default_value_t = 1024)]
    pub dlq_message_capacity: usize,

    /// Output path for DLQ CSV.
    /// Default: dlq.csv
    #[arg(long, value_name = "PATH", default_value = "dlq.csv")]
    pub dlq_output_path: PathBuf,
}
