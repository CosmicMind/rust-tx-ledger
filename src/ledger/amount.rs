//! Amount: fixed–scale decimal wrapper for ledger balances.
//!
//! # Design
//! - Wraps `rust_decimal::Decimal` to avoid floating–point errors.
//! - Displayed and reported at **exactly 4 decimal places** (floor truncated).
//! - Internally, all arithmetic keeps full precision until display.
//!
//! # Invariants
//! - Values are never rounded up; fractions beyond 4dp are truncated (company
//!   keeps the dust).
//! - Equality/ordering is delegated to the underlying Decimal.
//!
//! # Usage
//! ```rust
//! use rust_tx_ledger::ledger::amount::Amount;
//!
//! let a = Amount::parse("1.234567").unwrap();
//! let b = Amount::parse("2").unwrap();
//! assert_eq!(format!("{}", a), "1.2345"); // truncated
//! assert_eq!(format!("{}", b), "2.0000");
//! ```

use std::fmt::{self, Debug, Display};
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::str::FromStr;

use rust_decimal::RoundingStrategy;
use rust_decimal::prelude::*;

/// Fixed number of decimal places used across the ledger.
///
/// - All amounts are displayed with **exactly 4 decimal places**.
/// - Fractions beyond 4dp are truncated (floor toward zero).
/// - Chosen to match common financial systems that standardize on 4 decimal
///   precision for transactions.
const SCALE_DP: u32 = 4;

/// A fixed–scale decimal amount used for deposits, withdrawals, and balances.
///
/// Wraps `rust_decimal::Decimal` to ensure safe arithmetic without
/// floating–point drift. Internally maintains full precision, but
/// display/output always normalizes to 4dp.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct Amount(Decimal);

impl Amount {
    /// Return the raw underlying `Decimal` (full precision).
    #[inline]
    pub fn raw(&self) -> Decimal {
        self.0
    }

    /// Parse a string into an `Amount`.
    ///
    /// # Errors
    /// Returns a `rust_decimal::Error` if the string cannot be parsed.
    ///
    /// # Notes
    /// Does not normalize to 4dp immediately; truncation is applied
    /// only for `Display`.
    #[inline]
    pub fn parse(s: &str) -> Result<Self, rust_decimal::Error> {
        Ok(Amount(Decimal::from_str(s)?))
    }
}

impl From<Decimal> for Amount {
    fn from(decimal: Decimal) -> Self {
        Amount(decimal)
    }
}

impl Display for Amount {
    /// Display with exactly 4 decimal places, truncated toward zero.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let v = self
            .0
            .round_dp_with_strategy(SCALE_DP, RoundingStrategy::ToZero);
        let s = v.to_string();

        if !s.contains('.') {
            write!(f, "{s}.0000")
        } else {
            let (int, frac) = s.split_once('.').unwrap();

            write!(f, "{}.{:0<4}", int, &frac[..frac.len().min(4)])
        }
    }
}

impl Debug for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Add for Amount {
    type Output = Amount;

    /// Add two `Amount`s, preserving full precision internally.
    #[inline]
    fn add(self, rhs: Amount) -> Amount {
        Amount(self.0 + rhs.0)
    }
}
impl Sub for Amount {
    type Output = Amount;

    /// Subtract two `Amount`s, preserving full precision internally.
    #[inline]
    fn sub(self, rhs: Amount) -> Amount {
        Amount(self.0 - rhs.0)
    }
}

impl AddAssign for Amount {
    #[inline]
    fn add_assign(&mut self, rhs: Amount) {
        *self = *self + rhs;
    }
}

impl SubAssign for Amount {
    #[inline]
    fn sub_assign(&mut self, rhs: Amount) {
        *self = *self - rhs;
    }
}
