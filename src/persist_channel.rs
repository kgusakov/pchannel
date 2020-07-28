#![allow(dead_code)]

use crate::{FromBytes, ToBytes};
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::{ Read, Write };
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::*;
use std::collections::HashSet;

pub fn persistent_channel<Id: ToBytes + FromBytes + Eq + Hash, Value: ToBytes + FromBytes>(
    data_file: PathBuf,
    ack_file: PathBuf,
) -> (PersistentSender<Id, Value>, PersistentReceiver<Id, Value>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let storage = Arc::new(Storage::new(data_file, ack_file));
    let p_tx = PersistentSender {
        sender: tx,
        storage: storage.clone(),
    };
    let p_rx = PersistentReceiver {
        receiver: rx,
        storage: storage.clone(),
    };
    (p_tx, p_rx)
}

pub struct PersistentSender<Id, Value> {
    sender: tokio::sync::mpsc::UnboundedSender<(Id, Value)>,
    storage: Arc<Storage<Id, Value>>,
}

impl<Id: ToBytes + FromBytes + Eq + Hash, Value: ToBytes + FromBytes> PersistentSender<Id, Value> {
    pub fn send(&self, t: (Id, Value)) -> Result<(), SendError<(Id, Value)>> {
        self.storage.persist(&t).unwrap();
        self.sender.send(t)
    }

    pub fn send_all(&self, t: Vec<(Id, Value)>) -> Result<(), SendError<(Id, Value)>> {
        self.storage.persist_all(&t).unwrap();
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

impl<Id: ToBytes + FromBytes, Value: ToBytes + FromBytes> PersistentReceiver<Id, Value> {
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

impl<Id: ToBytes + FromBytes + Eq + Hash, Value: ToBytes + FromBytes> Message<Id, Value> {
    pub async fn ack(self) {
        self.storage.remove(self.id).await.unwrap();
    }
}

struct Storage<Id, Value> {
    data_mutex: std::sync::Mutex<std::fs::File>,
    ack_mutex: tokio::sync::Mutex<std::fs::File>,
    phantom_id: PhantomData<Id>,
    phantom_value: PhantomData<Value>,
}

impl<Id: ToBytes + FromBytes + Eq + Hash, Value: ToBytes + FromBytes> Storage<Id, Value> {
    pub fn new(data_path: PathBuf, ack_path: PathBuf) -> Self {
        Self {
            data_mutex: std::sync::Mutex::new(OpenOptions::new().write(true).open(data_path).unwrap()),
            ack_mutex: tokio::sync::Mutex::new(OpenOptions::new().write(true).open(ack_path).unwrap()),
            phantom_id: PhantomData,
            phantom_value: PhantomData,
        }
    }

    pub fn load(
        data_path: PathBuf,
        ack_path: PathBuf,
    ) -> Result<Vec<(Id, Value)>, Box<dyn std::error::Error>> {
        let acked_ids = Self::read_ack(ack_path)?;
        Self::read_data(data_path, acked_ids)
    }

    fn read_ack(ack_path: PathBuf) -> Result<HashSet<Id>, Box<dyn std::error::Error>> {
        let mut init_data = HashSet::new();
        {
            let mut f = OpenOptions::new().read(true).open(&ack_path)?;
            let mut ack_size_buf: [u8; 8] = [0; 8];
            while f.read(&mut ack_size_buf)? != 0 {
                let ack_size= usize::from_be_bytes(ack_size_buf);
                assert!(ack_size > 0);
                let mut ack_buf = vec![0 as u8; ack_size];
                f.read(&mut ack_buf)?;
                let id = Id::from_bytes(ack_buf);
                init_data.insert(id);
            }
        }
        Ok(init_data)
    }

    fn read_data(data_path: PathBuf, removed_ids: HashSet<Id>) -> Result<Vec<(Id, Value)>, Box<dyn std::error::Error>> {
        let mut init_data = vec![];
        {
            let mut f = OpenOptions::new().read(true).open(&data_path)?;
            let mut id_size_buf: [u8; 8] = [0; 8];
            let mut data_size_buf: [u8; 8] = [0; 8];
            while f.read(&mut id_size_buf)? != 0 && f.read(&mut data_size_buf)? != 0 {
                let (id_size, data_size) = (
                    usize::from_be_bytes(id_size_buf),
                    usize::from_be_bytes(data_size_buf),
                );
                assert!(id_size > 0);
                assert!(data_size > 0);
                let (mut id_buf, mut data_buf) = (vec![0 as u8; id_size], vec![0 as u8; data_size]);
                f.read(&mut id_buf)?;
                f.read(&mut data_buf)?;
                let id = Id::from_bytes(id_buf);
                if !removed_ids.contains(&id) { init_data.push((id, Value::from_bytes(data_buf)))};
            }
        }
        Ok(init_data)
    }

    pub fn persist(&self, element: &(Id, Value)) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = Vec::<u8>::new();

        let (id_bytes, value_bytes) = (element.0.bytes(), element.1.bytes());
        let (id_size, value_size) = (id_bytes.len().to_be_bytes(), value_bytes.len().to_be_bytes());
        data.write_all(&id_size)?;
        data.write_all(&value_size)?;
        data.write_all(&id_bytes)?;
        data.write_all(&value_bytes)?;
        {
            let mut data_file = self.data_mutex.lock().unwrap();
            data_file.write_all(&data)?;
            data_file.sync_data()?;
        }
        Ok(())
    }

    pub fn persist_all(
        &self,
        elements: &Vec<(Id, Value)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = Vec::<u8>::new();

        for element in elements {
            let (id_bytes, value_bytes) = (element.0.bytes(), element.1.bytes());
            let (id_size, value_size) = (id_bytes.len().to_be_bytes(), value_bytes.len().to_be_bytes());
            data.write_all(&id_size)?;
            data.write_all(&value_size)?;
            data.write_all(&id_bytes)?;
            data.write_all(&value_bytes)?;
        }
        {
            let mut data_file = self.data_mutex.lock().unwrap();
            data_file.write_all(&data)?;
            data_file.sync_data()?;
        }
        Ok(())
    }

    pub async fn remove(&self, id: Id) -> Result<(), Box<dyn std::error::Error>> {
        let mut data = Vec::<u8>::new();

        let id_bytes = id.bytes();
        let id_size = id_bytes.len().to_be_bytes();
        data.write_all(&id_size)?;
        data.write_all(&id_bytes)?;
        {
            let mut ack_file = self.ack_mutex.lock().await;
            ack_file.write_all(&data)?;
            ack_file.sync_data()?;
        }
        Ok(())
        
    }

    fn compaction(&self) -> Result<(), Box<dyn std::error::Error>> {
        unimplemented!()
    }
}

#[test]
fn persist_channel_test() {}
