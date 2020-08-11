use serde::{de::DeserializeOwned, Serialize};

use std::fmt::Debug;

use std::hash::Hash;

use super::storage::*;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::*;

#[derive(Debug)]
pub enum SendError<T> {
    SendError(tokio::sync::mpsc::error::SendError<T>),
    PersistError(StorageError),
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendError(_) => write!(fmt, "channel closed"),
            Self::PersistError(_) => write!(fmt, "error during persist the message"),
        }
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SendError<T> {
    fn from(error: tokio::sync::mpsc::error::SendError<T>) -> Self {
        SendError::SendError(error)
    }
}

impl<T> From<StorageError> for SendError<T> {
    fn from(error: StorageError) -> Self {
        SendError::PersistError(error)
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

#[derive(Error, Debug)]
pub enum RecvError {
    #[error(transparent)]
    ReceivError(#[from] tokio::sync::mpsc::error::RecvError),
    #[error(transparent)]
    PersistError(#[from] StorageError),
}

pub fn persistent_channel<
    Id: Serialize + DeserializeOwned + Eq + Hash + Debug,
    Value: Serialize + DeserializeOwned + Debug,
>(
    data_file: PathBuf,
    ack_file: PathBuf,
    compaction_threshold: u64,
) -> Result<(PersistentSender<Id, Value>, PersistentReceiver<Id, Value>), SendError<(Id, Value)>> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (storage, alive_records) = Storage::load(
        data_file.to_path_buf(),
        ack_file.to_path_buf(),
        compaction_threshold,
    )?;

    let synced_storage = Arc::new(storage);

    for (id, value) in alive_records {
        tx.send((id, value))?;
    }
    let p_tx = PersistentSender {
        sender: tx,
        storage: synced_storage.clone(),
    };
    let p_rx = PersistentReceiver {
        receiver: rx,
        storage: synced_storage.clone(),
    };

    Ok((p_tx, p_rx))
}

pub struct PersistentSender<Id, Value> {
    sender: tokio::sync::mpsc::UnboundedSender<(Id, Value)>,
    storage: Arc<Storage<Id, Value>>,
}

impl<
        Id: Serialize + DeserializeOwned + Eq + Hash + Debug,
        Value: Serialize + DeserializeOwned + Debug,
    > PersistentSender<Id, Value>
{
    pub fn send(&self, t: (Id, Value), fsync: bool) -> Result<(), SendError<(Id, Value)>> {
        self.storage.persist(&t, fsync)?;
        Ok(self.sender.send(t)?)
    }

    pub fn send_all(&self, t: Vec<(Id, Value)>) -> Result<(), SendError<(Id, Value)>> {
        self.storage.persist_all(&t)?;
        for v in t {
            self.sender.send(v)?
        }
        Ok(())
    }
}

pub struct PersistentReceiver<Id, Value> {
    receiver: UnboundedReceiver<(Id, Value)>,
    storage: Arc<Storage<Id, Value>>,
}

impl<Id: DeserializeOwned, Value: DeserializeOwned> PersistentReceiver<Id, Value> {
    pub async fn recv(&mut self) -> Option<Message<Id, Value>> {
        if let Some((id, value)) = self.receiver.recv().await {
            Some(Message {
                id: id,
                value: value,
                storage: self.storage.clone(),
            })
        } else {
            None
        }
    }
}
pub struct Message<Id, Value> {
    pub id: Id,
    pub value: Value,
    storage: Arc<Storage<Id, Value>>,
}

impl<Id: Serialize + DeserializeOwned + Eq + Hash + Clone, Value: Serialize + DeserializeOwned>
    Message<Id, Value>
{
    pub async fn ack(&self) -> Result<(), StorageError> {
        self.storage.remove(&self.id).await
    }
}

#[cfg(test)]
mod p_tests {
    use super::{persistent_channel, Message};
    use tempfile::NamedTempFile;
    use tokio::runtime::Runtime;

    #[test]
    fn persist_channel_no_ack_test() {
        let mut runtime = Runtime::new().unwrap();
        let data_file = NamedTempFile::new().unwrap();
        let ack_file = NamedTempFile::new().unwrap();

        let (data_path, ack_path) = (
            data_file.path().to_path_buf(),
            ack_file.path().to_path_buf(),
        );

        {
            let (tx, mut rx) = persistent_channel(data_path, ack_path, 1000).unwrap();
            tx.send((1i32, 1i32), true).unwrap();

            let f = async move {
                let m = rx.recv().await?;
                let (id, value) = (m.id, m.value);
                Some((id, value))
            };
            let m: (i32, i32) = runtime.block_on(f).unwrap();
            assert_eq!((1, 1), (m.0, m.1))
        }

        {
            let (_, mut rx) = persistent_channel(
                data_file.path().to_path_buf(),
                ack_file.path().to_path_buf(),
                1000,
            )
            .unwrap();
            let f = async move {
                let m = rx.recv().await.unwrap();
                m
            };
            let m: Message<i32, i32> = runtime.block_on(f);
            assert_eq!((1, 1), (m.id, m.value))
        }
    }

    #[test]
    fn persist_channel_ack_test() {
        let mut runtime = Runtime::new().unwrap();
        let data_file = NamedTempFile::new().unwrap();
        let ack_file = NamedTempFile::new().unwrap();

        let (data_path, ack_path) = (
            data_file.path().to_path_buf(),
            ack_file.path().to_path_buf(),
        );

        {
            let (tx, mut rx) = persistent_channel(data_path, ack_path, 1000).unwrap();
            tx.send((1i32, 1i32), true).unwrap();

            let f = async move {
                let m = rx.recv().await?;
                m.ack().await.ok()?;
                Some(m)
            };
            let m: Message<i32, i32> = runtime.block_on(f).unwrap();
            assert_eq!((1, 1), (m.id, m.value))
        }

        {
            let (_, mut rx) = persistent_channel(
                data_file.path().to_path_buf(),
                ack_file.path().to_path_buf(),
                1000,
            )
            .unwrap();
            let f = async move {
                let m = rx.recv().await;
                m
            };
            let m: Option<Message<i32, i32>> = runtime.block_on(f);
            assert!(m.is_none());
        }
    }
}
