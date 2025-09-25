//! Cursor: a stable file position for tracing and replay.
//!
//! # What it is
//! A thin wrapper over `csv::Position` that captures **where** in the file a
//! row was *attempted* (line and byte). We attach this to messages so
//! downstream components (DLQ, logs, audits) can point back to the exact spot
//! in the input.
//!
//! # How to capture it correctly
//! - Clone the reader's position **before** calling `read_record`. That gives
//!   you the start-of-record cursor for the row you’re about to read.
//!
//! ```ignore
//! let pos = reader.position().clone();    // capture start-of-record
//! let got = reader.read_record(&mut rec)?;
//! if got { let cursor = Cursor::from(pos); /* use cursor */ }
//! ```
//!
//! # Notes
//! - `Cursor` is lightweight and `Clone`.
//! - We only expose `line()` and `byte()` since that’s what we log and write,
//!   but you can add a `position()` accessor later if you want to support
//!   `seek`.
//! - The semantics match “cursor” in streaming systems: a bookmark you can log,
//!   persist, and reason about during replay.

use csv::Position;

/// File-anchored cursor pointing to the start of a record.
///
/// Wraps `csv::Position` so we can keep CSV-reader-specific details tucked
/// away, while exposing simple `line()` / `byte()` accessors for logs and DLQ.
#[derive(Debug, Clone)]
pub struct Cursor {
    pos: Position,
}

impl Cursor {
    /// Construct from a `csv::Position` you captured *before* `read_record`.
    #[inline]
    pub const fn new(pos: Position) -> Self {
        Self { pos }
    }

    /// 1-based line number as reported by `csv::Reader`.
    #[inline]
    pub fn line(&self) -> u64 {
        self.pos.line()
    }

    /// Byte offset into the file at the start of the record.
    #[inline]
    pub fn byte(&self) -> u64 {
        self.pos.byte()
    }

    // If you later want to enable seeking/replay with the csv reader:
    // #[inline]
    // pub fn position(&self) -> &Position {
    //     &self.pos
    // }
}

impl From<Position> for Cursor {
    #[inline]
    fn from(pos: Position) -> Self {
        Self::new(pos)
    }
}
