//! Accounts and per-client state.
//!
//! # Model
//! - Each client has an `Account` that tracks:
//!   - `available`: spendable funds
//!   - `held`: funds under dispute
//!   - `is_locked`: account frozen after a chargeback
//!   - `txs`: per-transaction ledger for idempotency & dispute lifecycle
//!
//! # Rules / Invariants
//! - `deposit`/`withdrawal` are **idempotent** per `tx`. Replays are ignored.
//! - `withdrawal` requires sufficient `available` funds.
//! - `dispute` applies only to **deposit** transactions and moves funds from
//!   `available` → `held` (no amount attached to the dispute command).
//! - `resolve` releases held funds back to `available`.
//! - `chargeback` removes held funds permanently and **locks** the account.
//! - Once locked, no further operations are accepted.
//!
//! # Error surface
//! - `Locked`: account is frozen after a chargeback
//! - `Insufficient`: not enough available balance for withdrawal
//! - `TxNotFound`: dispute/resolve/chargeback references unknown tx
//! - `AlreadyDisputed` / `NotDisputed`: dispute state machine violations
//! - `NotDisputable`: non-deposit tx referenced by dispute
//! - `MissingAmount`: deposit/withdrawal without amount (should be
//!   pre-validated)

use std::collections::HashMap;

use crate::ledger::amount::Amount;
use crate::ledger::entry::{Entry, EntryType};

/// Client identifier type alias for clarity.
pub type ClientId = u16;

/// In-memory map of all accounts hosted by a partition.
pub type AccountsMap = HashMap<ClientId, Account>;

#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("account locked")]
    Locked,

    #[error("insufficient funds")]
    Insufficient,

    #[error("tx not found")]
    TxNotFound,

    #[error("tx already disputed")]
    AlreadyDisputed,

    #[error("tx not disputed")]
    NotDisputed,

    #[error("tx not disputable")]
    NotDisputable,

    #[error("missing amount")]
    MissingAmount,
}

/// Minimal per-transaction record retained to support idempotency and dispute
/// flow.
#[derive(Debug, Clone, Copy)]
pub struct TxInfo {
    pub kind: EntryType,
    pub amount: Amount,
    pub disputed: bool,
}

pub type AccountResult<T> = Result<T, AccountError>;

/// A single client's account state and rules.
#[derive(Debug, Default)]
pub struct Account {
    available: Amount,
    held: Amount,
    is_locked: bool,
    txs: HashMap<u32, TxInfo>,
}

impl Account {
    /// Returns `true` if the account has been locked due to chargeback.
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.is_locked
    }

    /// Spendable balance (excludes held funds).
    #[inline]
    pub fn available(&self) -> Amount {
        self.available
    }

    /// Funds currently under dispute.
    #[inline]
    pub fn held(&self) -> Amount {
        self.held
    }

    /// Total = available + held.
    #[inline]
    pub fn total(&self) -> Amount {
        self.available + self.held
    }

    /// Apply a deposit.
    ///
    /// - Idempotent per `tx` (replays ignored).
    /// - Fails if account is locked.
    pub fn deposit(&mut self, entry: &Entry) -> AccountResult<()> {
        if self.is_locked {
            return Err(AccountError::Locked);
        }

        let tx = entry.tx();
        if self.txs.contains_key(&tx) {
            return Ok(()); // dedupe
        }

        let amount = entry.amount().ok_or(AccountError::MissingAmount)?;

        self.available += amount;

        self.txs.insert(
            tx,
            TxInfo {
                kind: EntryType::Deposit,
                amount,
                disputed: false,
            },
        );

        Ok(())
    }

    /// Apply a withdrawal.
    ///
    /// - Idempotent per `tx` (replays ignored).
    /// - Requires sufficient `available` funds.
    /// - Fails if account is locked.
    pub fn withdrawal(&mut self, entry: &Entry) -> AccountResult<()> {
        if self.is_locked {
            return Err(AccountError::Locked);
        }

        let tx = entry.tx();
        if self.txs.contains_key(&tx) {
            return Ok(()); // dedupe
        }

        let amount = entry.amount().ok_or(AccountError::MissingAmount)?;

        if self.available < amount {
            return Err(AccountError::Insufficient);
        }

        self.available -= amount;

        self.txs.insert(
            tx,
            TxInfo {
                kind: EntryType::Withdrawal,
                amount,
                disputed: false,
            },
        );

        Ok(())
    }

    /// Enter dispute on a prior **deposit**.
    ///
    /// - Moves funds from `available` → `held`.
    /// - Only allowed for existing, non-disputed **deposit** txs.
    /// - Fails if account is locked.
    pub fn dispute(&mut self, entry: &Entry) -> AccountResult<()> {
        if self.is_locked {
            return Err(AccountError::Locked);
        }

        let tx = entry.tx();
        let tx_info = self.txs.get_mut(&tx).ok_or(AccountError::TxNotFound)?;

        if !matches!(tx_info.kind, EntryType::Deposit) {
            return Err(AccountError::NotDisputable);
        }

        if tx_info.disputed {
            return Err(AccountError::AlreadyDisputed);
        }

        // Typical payments-engine semantics: move funds to `held` even if that
        // sends `available` negative due to prior withdrawals.
        self.available -= tx_info.amount;
        self.held += tx_info.amount;

        tx_info.disputed = true;

        Ok(())
    }

    /// Resolve a dispute (release back to `available`).
    ///
    /// - Requires the tx be currently disputed.
    /// - Fails if account is locked.
    pub fn resolve(&mut self, entry: &Entry) -> AccountResult<()> {
        if self.is_locked {
            return Err(AccountError::Locked);
        }

        let tx = entry.tx();
        let tx_info = self.txs.get_mut(&tx).ok_or(AccountError::TxNotFound)?;

        if !tx_info.disputed {
            return Err(AccountError::NotDisputed);
        }

        self.held -= tx_info.amount;
        self.available += tx_info.amount;

        tx_info.disputed = false;

        Ok(())
    }

    /// Chargeback a disputed tx.
    ///
    /// - Removes the held funds permanently.
    /// - Locks the account.
    pub fn chargeback(&mut self, entry: &Entry) -> AccountResult<()> {
        if self.is_locked {
            return Err(AccountError::Locked);
        }

        let tx = entry.tx();
        let info = self.txs.get_mut(&tx).ok_or(AccountError::TxNotFound)?;

        if !info.disputed {
            return Err(AccountError::NotDisputed);
        }

        self.held -= info.amount;
        self.is_locked = true;

        Ok(())
    }
}
