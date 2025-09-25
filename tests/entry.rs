use rust_tx_ledger::ledger::amount::Amount;
use rust_tx_ledger::ledger::entry::{Entry, EntryType, EntryValidateError, WireEntry};

fn make_wire_entry(kind: EntryType, amount: Option<&str>) -> WireEntry {
    WireEntry {
        kind,
        client_id: 10,
        tx: 99,
        amount: amount.map(|s| Amount::parse(s).unwrap()),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn deposit_ok() {
    let entry = Entry::try_from(make_wire_entry(EntryType::Deposit, Some("1.2345"))).unwrap();
    assert!(matches!(entry.kind(), EntryType::Deposit));
    assert_eq!(entry.client_id(), 10);
    assert_eq!(entry.tx(), 99);
    assert!(entry.amount().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn deposit_rejects_zero_and_negative() {
    assert!(matches!(
        Entry::try_from(make_wire_entry(EntryType::Deposit, Some("0"))),
        Err(EntryValidateError::ZeroAmount)
    ));
    assert!(matches!(
        Entry::try_from(make_wire_entry(EntryType::Deposit, Some("-0.01"))),
        Err(EntryValidateError::NegativeAmount)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn control_ops_reject_amount() {
    for k in [
        EntryType::Dispute,
        EntryType::Resolve,
        EntryType::Chargeback,
    ] {
        assert!(matches!(
            Entry::try_from(make_wire_entry(k, Some("1.00"))),
            Err(EntryValidateError::AmountNotAllowed(_))
        ));
    }
}
