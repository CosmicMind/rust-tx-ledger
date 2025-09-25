//! Entry: typed representation of a ledger transaction.
//!
//! # Design
//! - Input rows are first parsed into a `WireEntry` via `serde::Deserialize`.
//!   - This keeps CSV parsing simple and close to the wire format.
//!   - Amount is optional here and parsed lazily through `de_amount_opt`.
//! - `Entry` is a validated domain type created from `WireEntry`.
//!   - `Funds` variant for Deposit/Withdrawal (requires amount).
//!   - `Control` variant for Dispute/Resolve/Chargeback (no amount allowed).
//! - Validation runs once at conversion time (`TryFrom<WireEntry>`).
//!   - Rejects negative and zero amounts for funds.
//!   - Rejects extra amounts for control types.
//!   - Ensures downstream code can trust invariants without re-checking.
//!
//! # Error semantics
//! - `EntryValidateError` captures validation failures in a structured way.
//! - These errors are sent to the DLQ, never downstream to accounts.
//!
//! # Notes
//! - `Entry` implements accessor methods for kind, client_id, tx, and amount.
//! - Amount is wrapped in a domain-specific `Amount` type for safe arithmetic.
//! - Future extensions (e.g. replay, audit) can enrich `WireEntry` without
//!   changing core `Entry` validation.
//!
//! # Example
//! ```ignore
//! let wire: WireEntry = record.deserialize()?;
//! let entry = Entry::try_from(wire)?;
//! assert_eq!(entry.kind(), EntryType::Deposit);
//! ```

use std::fmt::{self, Display};

use serde::Deserialize;
use serde::de::{self, Deserializer};

use crate::ledger::account::ClientId;
use crate::ledger::amount::Amount;

/// Transaction type, as declared in the CSV input.
/// Display matches CSV lowercase strings.
#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl Display for EntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Deposit => "deposit",
            Self::Withdrawal => "withdrawal",
            Self::Dispute => "dispute",
            Self::Resolve => "resolve",
            Self::Chargeback => "chargeback",
        };
        f.write_str(s)
    }
}

/// Raw deserialized row, straight from CSV.
/// Used only as input before validation.
#[derive(Debug, Deserialize)]
pub struct WireEntry {
    #[serde(rename = "type")]
    pub kind: EntryType,

    #[serde(rename = "client")]
    pub client_id: u16,

    pub tx: u32,

    #[serde(default, deserialize_with = "de_amount_opt")]
    pub amount: Option<Amount>,
}

/// Validated ledger entry.
/// Funds-entries must have an amount, control-entries must not.
#[derive(Debug)]
pub enum Entry {
    Funds {
        kind: EntryType,
        client_id: ClientId,
        tx: u32,
        amount: Amount,
    },
    Control {
        kind: EntryType,
        client_id: ClientId,
        tx: u32,
    },
}

impl Entry {
    pub fn kind(&self) -> EntryType {
        match self {
            Entry::Funds { kind, .. } => *kind,
            Entry::Control { kind, .. } => *kind,
        }
    }

    pub fn client_id(&self) -> ClientId {
        match self {
            Entry::Funds { client_id, .. } => *client_id,
            Entry::Control { client_id, .. } => *client_id,
        }
    }

    pub fn tx(&self) -> u32 {
        match self {
            Entry::Funds { tx, .. } => *tx,
            Entry::Control { tx, .. } => *tx,
        }
    }

    pub fn amount(&self) -> Option<Amount> {
        match self {
            Entry::Funds { amount, .. } => Some(*amount),
            _ => None,
        }
    }
}

/// Custom deserializer for optional amounts.
///
/// # Behavior
/// - Accepts a string like `"1.2345"` and parses it into an `Amount`.
/// - Empty field (`""`) or missing column → `None`.
/// - Any parse error → bubbles up to serde as a custom error.
///
/// # Notes
/// - This runs *before* validation. `Entry::try_from` enforces semantic rules
///   (non-negative, non-zero, amount required/forbidden).
/// - Keeps `WireEntry` close to the CSV shape while still giving us typed
///   arithmetic in downstream code.
///
/// # Example
/// ```ignore
/// #[serde(default, deserialize_with = "de_amount_opt")]
/// amount: Option<Amount>,
/// ```
fn de_amount_opt<'de, D>(de: D) -> Result<Option<Amount>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::<String>::deserialize(de)?;
    match opt.as_deref().map(str::trim) {
        None | Some("") => Ok(None),
        Some(s) => Amount::parse(s).map(Some).map_err(de::Error::custom),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EntryValidateError {
    #[error("missing amount for {0}")]
    MissingAmount(EntryType),

    #[error("amount not allowed for {0}")]
    AmountNotAllowed(EntryType),

    #[error("negative amount")]
    NegativeAmount,

    #[error("zero amount")]
    ZeroAmount,
}

/// Converts a raw `WireEntry` into a validated `Entry`.
/// This enforces all invariants so downstream consumers can trust the data.
impl TryFrom<WireEntry> for Entry {
    type Error = EntryValidateError;

    fn try_from(wire_entry: WireEntry) -> Result<Self, Self::Error> {
        let entry = match wire_entry.kind {
            EntryType::Deposit | EntryType::Withdrawal => {
                let amount = wire_entry
                    .amount
                    .ok_or(EntryValidateError::MissingAmount(wire_entry.kind))?;

                if amount.raw().is_sign_negative() {
                    return Err(EntryValidateError::NegativeAmount);
                }

                if amount.raw().is_zero() {
                    return Err(EntryValidateError::ZeroAmount);
                }

                Entry::Funds {
                    kind: wire_entry.kind,
                    client_id: wire_entry.client_id,
                    tx: wire_entry.tx,
                    amount,
                }
            }
            _ => {
                if wire_entry.amount.is_some() {
                    return Err(EntryValidateError::AmountNotAllowed(wire_entry.kind));
                }

                Entry::Control {
                    kind: wire_entry.kind,
                    client_id: wire_entry.client_id,
                    tx: wire_entry.tx,
                }
            }
        };

        Ok(entry)
    }
}
