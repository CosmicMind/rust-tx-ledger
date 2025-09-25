use rust_decimal::Decimal;
use rust_tx_ledger::ledger::amount::Amount;

#[tokio::test(flavor = "current_thread")]
async fn display_is_truncated_to_4dp() {
    assert_eq!(
        Amount::from(Decimal::from_str_exact("1").unwrap()).to_string(),
        "1.0000"
    );
    assert_eq!(
        Amount::from(Decimal::from_str_exact("1.234567").unwrap()).to_string(),
        "1.2345"
    );
    assert_eq!(
        Amount::from(Decimal::from_str_exact("-0.00991").unwrap()).to_string(),
        "-0.0099"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn arithmetic_is_exact_then_display_truncates() {
    let a = Amount::from(Decimal::from_str_exact("0.10005").unwrap());
    let b = Amount::from(Decimal::from_str_exact("0.10005").unwrap());

    assert_eq!((a + b).to_string(), "0.2001");
}
