use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::PathBuf;

use std::hash::Hash;
use thiserror::Error;

//  TODO Enrich error with the (id, value) which force it
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("data serialization inside storage failed")]
    BincodeError(#[from] bincode::Error),
    #[error("the `{0}` lock is poisoned")]
    AsyncMutexPoisonError(String),
    #[error("io error inside storage")]
    IoError(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, StorageError>;

pub(crate) struct Storage<Id, Value> {
    data_path: PathBuf,
    ack_path: PathBuf,
    data_mutex: std::sync::Mutex<std::fs::File>,
    ack_mutex: tokio::sync::Mutex<std::fs::File>,
    compaction_threshold: u64,
    compaction_records_counter: std::sync::Mutex<u64>,
    phantom_id: PhantomData<Id>,
    phantom_value: PhantomData<Value>,
}

impl<'a, Id: Serialize + DeserializeOwned + Eq + Hash, Value: Serialize + DeserializeOwned>
    Storage<Id, Value>
{
    pub(crate) fn new(
        data_path: PathBuf,
        ack_path: PathBuf,
        compaction_threshold: u64,
        uncompacted_records: u64,
    ) -> Result<Self> {
        Ok(Self {
            data_path: data_path.clone(),
            ack_path: ack_path.clone(),
            data_mutex: std::sync::Mutex::new(OpenOptions::new().append(true).open(data_path)?),
            ack_mutex: tokio::sync::Mutex::new(OpenOptions::new().append(true).open(ack_path)?),
            compaction_threshold,
            compaction_records_counter: std::sync::Mutex::new(uncompacted_records),
            phantom_id: PhantomData,
            phantom_value: PhantomData,
        })
    }

    pub(crate) fn load(data_path: PathBuf, ack_path: PathBuf) -> Result<(Vec<(Id, Value)>, u64)> {
        let acked_ids = Self::read_ack(ack_path)?;
        let acked_size = acked_ids.len();
        Ok((Self::read_data(data_path, acked_ids)?, acked_size as u64))
    }

    fn read_ack<T>(ack_path: PathBuf) -> std::io::Result<HashSet<T>>
    where
        T: Serialize + DeserializeOwned + Eq + Hash,
    {
        let mut init_data = HashSet::new();
        {
            let mut f = OpenOptions::new().read(true).open(&ack_path)?;
            let mut ack_size_buf: [u8; 8] = [0; 8];
            while f.read(&mut ack_size_buf)? != 0 {
                let ack_size = usize::from_be_bytes(ack_size_buf);
                assert!(ack_size > 0);
                let mut ack_buf = vec![0 as u8; ack_size];
                f.read(&mut ack_buf)?;
                let id = bincode::deserialize(&ack_buf).unwrap();
                init_data.insert(id);
            }
        }
        Ok(init_data)
    }

    fn read_data(data_path: PathBuf, removed_ids: HashSet<Id>) -> Result<Vec<(Id, Value)>> {
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
                let id = bincode::deserialize(&id_buf)?;
                if !removed_ids.contains(&id) {
                    init_data.push((id, bincode::deserialize(&data_buf)?))
                };
            }
        }
        Ok(init_data)
    }

    pub(crate) fn persist(&self, element: &(Id, Value)) -> Result<()> {
        let mut data = Vec::<u8>::new();
        let (id_bytes, value_bytes) = (
            bincode::serialize(&element.0)?,
            bincode::serialize(&element.1)?,
        );
        let (id_size, value_size) = (
            id_bytes.len().to_be_bytes(),
            value_bytes.len().to_be_bytes(),
        );
        data.write_all(&id_size)?;
        data.write_all(&value_size)?;
        data.write_all(&id_bytes)?;
        data.write_all(&value_bytes)?;
        {
            let mut data_file = self
                .data_mutex
                .lock()
                .map_err(|_| StorageError::AsyncMutexPoisonError("data_file".to_string()))?;
            data_file.write_all(&data)?;
            data_file.sync_data()?;
        }
        Ok(())
    }

    fn serialize_all(elements: &Vec<(Id, Value)>) -> Result<Vec<u8>> {
        let mut data = Vec::<u8>::new();

        for element in elements {
            let (id_bytes, value_bytes) = (
                bincode::serialize(&element.0)?,
                bincode::serialize(&element.1)?,
            );
            let (id_size, value_size) = (
                id_bytes.len().to_be_bytes(),
                value_bytes.len().to_be_bytes(),
            );
            data.write_all(&id_size)?;
            data.write_all(&value_size)?;
            data.write_all(&id_bytes)?;
            data.write_all(&value_bytes)?;
        }
        Ok(data)
    }

    pub(crate) fn persist_all(&self, elements: &Vec<(Id, Value)>) -> Result<()> {
        let data = Self::serialize_all(elements)?;
        {
            let mut data_file = self
                .data_mutex
                .lock()
                .map_err(|_| StorageError::AsyncMutexPoisonError("data_file".to_string()))?;
            data_file.write_all(&data)?;
            data_file.sync_data()?;
        }
        Ok(())
    }

    pub(crate) async fn remove(&self, id: Id) -> Result<()> {
        {
            let counter = self
                .compaction_records_counter
                .lock()
                .map_err(|_| StorageError::AsyncMutexPoisonError("counter".to_string()))?;
            if *counter > self.compaction_threshold {
                self.compaction().await?;
            }
        }

        let mut data = Vec::<u8>::new();

        let id_bytes = bincode::serialize(&id)?;
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

    async fn compaction(&self) -> Result<()> {
        let mut dl = self
            .data_mutex
            .lock()
            .map_err(|_| StorageError::AsyncMutexPoisonError("data_file".to_string()))?;
        let mut al = self.ack_mutex.lock().await;
        let alive_data =
            Self::serialize_all(&Self::load(self.data_path.clone(), self.ack_path.clone())?.0)?;
        let tmp_path = self.data_path.join(".tmp");
        let mut tmp_file = OpenOptions::new().write(true).open(tmp_path.clone())?;
        tmp_file.write_all(&alive_data)?;
        tmp_file.sync_data()?;
        std::fs::rename(tmp_path, self.data_path.clone())?;
        *dl = OpenOptions::new()
            .append(true)
            .open(self.data_path.clone())?;
        *al = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.data_path.clone())?;
        Ok(())
    }
}
