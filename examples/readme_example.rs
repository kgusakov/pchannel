use pchannel::persist_channel::{Message, persistent_channel};
use tokio::runtime::Runtime;
use std::path::PathBuf;

fn main() {
    let mut runtime = Runtime::new().unwrap();
    // for decreasing locks contention -
    // storage is using separate files for storing message data and ack events
    let (data_path, ack_path) = (PathBuf::from("/tmp/messages.data"), PathBuf::from("/tmp/acks.data"));
    let (tx, mut rx) = 
        persistent_channel(data_path, ack_path, 1000)
            .expect("Error while trying to replay unacked messages from storage");

    let m = (1, 1);
    tx.send(m)
        .expect("Error while trying to persist and send message to channel");

    let f = async move {
        let m: Message<i32, i32> = rx.recv().await.unwrap();
        // process your message here
        println!("Message {:?} processed", m);
        m.ack().await
            .expect("Error while trying to execute persistent ack for message");
    };
    runtime.block_on(f);
}
