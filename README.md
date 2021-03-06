![Rust](https://github.com/kgusakov/pchannel/workflows/Rust/badge.svg?branch=master&event=push)

# pchannel

Simple extension of Tokio unbounded channels with:

- separate receive-ack semantic
- file-based persistence of messages (all unacked messages will be replayed after channel restart)

## Usage

```rust

use pchannel::persist_channel::{Message, persistent_channel};
use tokio::runtime::Runtime;
use std::path::PathBuf;

fn main() {
    let mut runtime = Runtime::new().unwrap();
    // for decreasing locks contention -
    // storage is using separate files for storing message data and ack events
    let (data_path, ack_path) = (PathBuf::from("/tmp/messages.data"), PathBuf::from("/tmp/acks.data"));

    // third parameter "compaction_threshold" is using for cleaning the storage from redundant data and ack events.
    // current compaction algorithm  freeze channel operations - so, compaction threshold should't be so small.
    // on the other hand - big compaction threshold will waste your disk space and increase the time of compaction process itself 
    let (tx, mut rx) = 
        persistent_channel(data_path, ack_path, 100)
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
```
