use rust_decimal::Decimal;
use rust_tx_ledger::ledger::account::{Account, AccountError};
use rust_tx_ledger::ledger::amount::Amount;
use rust_tx_ledger::ledger::entry::{Entry, EntryType};

fn make_funds_entry(kind: EntryType, client: u16, tx: u32, amt: &str) -> Entry {
    Entry::Funds {
        kind,
        client_id: client,
        tx,
        amount: Amount::from(Decimal::from_str_exact(amt).unwrap()),
    }
}
fn make_control_entry(kind: EntryType, client: u16, tx: u32) -> Entry {
    Entry::Control {
        kind,
        client_id: client,
        tx,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn deposit_withdraw_dispute_resolve_flow() {
    let mut account = Account::default();

    account
        .deposit(&make_funds_entry(EntryType::Deposit, 1, 1, "10"))
        .unwrap();
    account
        .withdrawal(&make_funds_entry(EntryType::Withdrawal, 1, 2, "3"))
        .unwrap();
    account
        .deposit(&make_funds_entry(EntryType::Deposit, 1, 3, "2.5"))
        .unwrap();

    account
        .dispute(&make_control_entry(EntryType::Dispute, 1, 3))
        .unwrap();
    account
        .resolve(&make_control_entry(EntryType::Resolve, 1, 3))
        .unwrap();

    assert_eq!(account.available().to_string(), "9.5000");
    assert_eq!(account.held().to_string(), "0.0000");
    assert_eq!(account.total().to_string(), "9.5000");
}

#[tokio::test(flavor = "current_thread")]
async fn chargeback_locks_account() {
    let mut account = Account::default();

    account
        .deposit(&make_funds_entry(EntryType::Deposit, 1, 1, "5"))
        .unwrap();
    account
        .dispute(&make_control_entry(EntryType::Dispute, 1, 1))
        .unwrap();
    account
        .chargeback(&make_control_entry(EntryType::Chargeback, 1, 1))
        .unwrap();

    assert!(account.is_locked());
    assert!(matches!(
        account.deposit(&make_funds_entry(EntryType::Deposit, 1, 2, "1")),
        Err(AccountError::Locked)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_tx_is_idempotent() {
    let mut account = Account::default();

    let entry = make_funds_entry(EntryType::Deposit, 1, 42, "1");
    account.deposit(&entry).unwrap();
    account.deposit(&entry).unwrap(); // same tx again

    assert_eq!(account.available().to_string(), "1.0000");
}
