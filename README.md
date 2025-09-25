# rust-tx-ledger

A streaming transaction ledger with ordered routing by client id.

## Overview

This project implements a toy ledger engine inspired by real-world transaction
processing systems. It ingests transactions from a CSV file, validates and
routes them through a streaming pipeline, applies account state transitions, and
writes results to output files.

---

## Pipeline

```text
Provider (CSV)
    ↓
Broker (fan-out by client_id)
    ↓
Partitions (per-client state)
    ↓
PartitionWriter (accounts.csv, stdout)

└──────────────→ DlqWriter (dlq.csv)
```

- **Provider**: Reads the input CSV line-by-line and validates each transaction.
  - Good rows become `DataMessage`s sent to the Broker.
  - Bad rows become `DlqMessage`s sent to the DLQ Writer.

- **Broker**: Routes each message to a partition, keyed by client id
  (`client_id % partition_count`).

- **Partition**: Owns a set of accounts (per client). Applies transactions
  (deposits, withdrawals, disputes, resolves, chargebacks).

- **PartitionWriter**: Collects the final account maps from each partition at
  shutdown and writes them to `accounts.csv` (stdout by default).

- **DlqWriter**: Writes rejected or malformed transactions to `dlq.csv` with
  structured error reasons.

---

## Invariants

- **Input order preserved**: Provider uses blocking sends.
- **Per-client FIFO**: Broker is the sole sender for each partition.
- **Amounts**:
  - Displayed truncated to 4 decimal places (floor).
  - Arithmetic preserves full precision until display.
- **Idempotency**: Duplicate `tx` ids per client are ignored.

---

## Concurrency

- Tokio runtime.
- Bounded `mpsc` channels for backpressure.
- No `unsafe` code.

---

## CLI

The binary accepts the following options:

| Flag                            | Description                                      | Default       |
|---------------------------------|--------------------------------------------------|---------------|
| `--data-message-capacity`       | Broker inbound queue capacity                    | 1024          |
| `--partition-message-capacity`  | Partition inbound queue capacity                 | 1024          |
| `--partition-thread-count`      | Number of partitions (≥2, default = CPU count)   | num_cpus      |
| `--dlq-message-capacity`        | DLQ writer queue capacity                        | 1024          |
| `--dlq-output-path PATH`        | DLQ output file                                  | dlq.csv       |

---

## Example

```bash
cargo run -- transactions.csv > accounts.csv
# Produces accounts.csv (stdout) and dlq.csv (side file for bad rows).
```

---

## Sample Output

### accounts.csv

```csv
client,available,held,total,locked
1,1.0341,0.0000,1.0341,false
2,1.4658,0.0000,1.4658,false
```

### dlq.csv

```csv
line,byte,reason,raw
5,85,"csv deserialize: unknown variant `1`","1,3,0.5"
7,117,zero amount,"withdrawal,1,5,0"
9,165,negative amount,"withdrawal,2,7,-0.10"
```

---

## Notes

- `Cursor` positions are attached to each message, enabling replay and traceability.
- Partitioning strategy is pluggable; currently simple modulo hashing is used.
- This is a learning exercise: code emphasizes clarity and composable patterns
  (Tokio, channels, oneshot, `rust_decimal` for amounts).
