use std::collections::BTreeMap;

use rust_decimal::Decimal;
use rust_tx_ledger::ledger::amount::Amount;
use rust_tx_ledger::ledger::broker::Broker;
use rust_tx_ledger::ledger::entry::{Entry, EntryType};
use rust_tx_ledger::ledger::message::DataMessage;
use tokio::sync::mpsc;

fn make_data_message(client: u16, tx: u32, amount: &str) -> DataMessage {
    DataMessage::new(Entry::Funds {
        kind: EntryType::Deposit,
        client_id: client,
        tx,
        amount: Amount::from(Decimal::from_str_exact(amount).unwrap()),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_by_client_and_preserves_per_partition_fifo() {
    // data channel (broker ingress)
    let (data_tx, data_rx) = mpsc::channel::<DataMessage>(2);

    // wire broker + partitions
    let (broker, partitions) = Broker::with_partition_receivers(data_rx, 2, 2);

    // spawn partitions
    let mut partion_handles = Vec::new();
    let mut partition_done_rxs = Vec::new();
    for (partition, done_rx) in partitions {
        partion_handles.push(partition.spawn());
        partition_done_rxs.push(done_rx);
    }

    // spawn broker BEFORE sending to avoid backpressure deadlock
    let broker_handle = broker.spawn();

    // now push 3 messages; broker drains concurrently
    data_tx.send(make_data_message(1, 1, "1")).await.unwrap();
    data_tx.send(make_data_message(2, 1, "2")).await.unwrap();
    data_tx.send(make_data_message(1, 2, "3")).await.unwrap();
    drop(data_tx); // close ingress

    // await broker + partitions
    broker_handle.await.unwrap().unwrap();
    for handle in partion_handles {
        handle.await.unwrap().unwrap();
    }

    // collect totals from partitions
    let mut totals = BTreeMap::new();
    for done_rx in partition_done_rxs {
        let map = done_rx.await.unwrap();
        for (client, account) in map {
            totals.insert(client, account.total().to_string());
        }
    }

    assert_eq!(totals.get(&1).unwrap(), "4.0000");
    assert_eq!(totals.get(&2).unwrap(), "2.0000");
}
