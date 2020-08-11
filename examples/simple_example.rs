use pchannel::persist_channel::*;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

fn main() {
    let mut runtime = Runtime::new().unwrap();
    let data_file = NamedTempFile::new().unwrap();
    let ack_file = NamedTempFile::new().unwrap();

    let (data_path, ack_path) = (
        data_file.path().to_path_buf(),
        ack_file.path().to_path_buf(),
    );
    let (tx, mut rx) = persistent_channel(data_path, ack_path, 1000).unwrap();

    let m = (1, 1);
    tx.send(m.clone(), true).unwrap();
    println!("Sent message {:?}", m);

    let f = async move {
        let m = rx.recv().await?;
        println!("Received message {:?}", (m.id, m.value));
        m.ack(true).await.unwrap();
        Some(())
    };
    runtime.block_on(f).unwrap();
}
