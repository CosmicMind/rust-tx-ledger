use std::path::PathBuf;

use futures::future::join_all;
use rust_tx_ledger::ledger::{Broker, DlqWriter, PartitionWriter, Provider};

#[tokio::test]
async fn pipeline_runs_and_writes_accounts() {
    // test input file
    let input_path = PathBuf::from("tests/fixtures/transactions.csv");
    let dlq_path = PathBuf::from("tests/output/dlq.csv");

    let data_message_capacity = 16;
    let dlq_message_capacity = 16;
    let partition_message_capacity = 16;
    let partition_thread_count = 2;

    let (provider, data_rx, dlq_rx) =
        Provider::with_consumer_receivers(input_path, data_message_capacity, dlq_message_capacity);

    let dlq_writer = DlqWriter::new(dlq_rx, dlq_path.clone());

    let (broker, partitions) = Broker::with_partition_receivers(
        data_rx,
        partition_message_capacity,
        partition_thread_count,
    );

    let mut partition_handles = Vec::new();
    let mut partition_done_rxs = Vec::new();

    for (partition, done_rx) in partitions {
        partition_handles.push(partition.spawn());
        partition_done_rxs.push(done_rx);
    }

    // spawn the pipeline
    let dlq_handle = dlq_writer.spawn();
    let broker_handle = broker.spawn();
    let provider_handle = provider.spawn_blocking();

    let partition_res = join_all(partition_handles).await;

    let dlq_res = dlq_handle.await;
    let brok_res = broker_handle.await;
    let prov_res = provider_handle.await;

    // unwraps
    for handle_res in partition_res {
        let res = handle_res.expect("partition task panicked");
        res.expect("partition returned error");
    }

    dlq_res
        .expect("dlq writer task panicked")
        .expect("dlq writer error");
    brok_res
        .expect("broker task panicked")
        .expect("broker error");
    prov_res
        .expect("provider task panicked")
        .expect("provider error");

    // collect accounts and write them
    PartitionWriter::run(partition_done_rxs)
        .await
        .expect("partition writer failed");
}
