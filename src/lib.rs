use std::collections::VecDeque;
use std::error::Error;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

mod persist_channel;

pub trait ToBytes {
    fn bytes(&self) -> Vec<u8>;
}

pub trait FromBytes {
    fn from_bytes(bytes: Vec<u8>) -> Self;
}

// struct EQueue<T> {
//     first_segment: Segment<T>,
//     last_segment: Segment<T>,
//     file_dir: String,
//     file_prefix: String,
//     records_per_segment: i32,
// }

// impl<T: ToBytes + FromBytes> EQueue<T> {
//     fn new(file_dir: String, file_prefix: String, records_per_segment: i32) -> Self {
//         unimplemented!()
//     }

// }

pub struct Segment<T> {
    file: File,
    data: VecDeque<T>,
    mutex: Mutex<bool>,
}

impl<T: ToBytes + FromBytes + Send + Debug> Segment<T> {
    pub fn new(file_path: PathBuf) -> Self {
        let file = OpenOptions::new().append(true).open(file_path).unwrap();
        Self {
            file,
            data: VecDeque::<T>::new(),
            mutex: Mutex::new(false),
        }
    }

    pub fn load(file_path: PathBuf) -> Result<Self, Box<dyn Error>> {
        let mut init_data = vec![];
        {
            let mut f = OpenOptions::new().read(true).open(&file_path)?;
            let mut size_buf: [u8; 8] = [0; 8];
            while f.read(&mut size_buf)? != 0 {
                let size = usize::from_be_bytes(size_buf);
                if size == 0 {
                    init_data.remove(0);
                } else {
                    let mut d = vec![0 as u8; size];
                    f.read(&mut d).unwrap();
                    init_data.push(T::from_bytes(d));
                }
            }
        }
        let mut segment = Self::new(file_path);
        for e in init_data.into_iter() {
            segment.data.push_back(e)
        }
        Ok(segment)
    }

    pub fn add_batch(&mut self, elements: Vec<T>) -> Result<(), Box<dyn Error>> {
        let mut data = Vec::<u8>::new();
        for element in &elements {
            let l = element.bytes().len().to_be_bytes();
            data.write_all(&l)?;
            data.write_all(&element.bytes())?;
        }

        {
            let _ = self.mutex.lock();
            self.file.write_all(&data)?;
            self.file.sync_data()?;

            for element in elements {
                self.data.push_back(element)
            }
        }
        Ok(())
    }

    pub fn add(&mut self, element: T) -> Result<(), Box<dyn Error>> {
        let mut data = Vec::<u8>::new();
        let l = element.bytes().len().to_be_bytes();
        data.write_all(&l)?;
        data.write_all(&element.bytes())?;
        {
            let _ = self.mutex.lock();
            self.file.write_all(&data)?;
            self.file.sync_data()?;
            self.data.push_back(element);
        }
        Ok(())
    }

    pub fn remove(&mut self) -> T {
        let mut element = self.pop_front_with_lock();

        while element.is_none() {
            element = self.pop_front_with_lock()
        }
        let delete_marker = &[0 as u8; 8];
        self.file.write(delete_marker).unwrap();
        self.file.sync_data().unwrap();
        element.unwrap()
    }

    fn pop_front_with_lock(&mut self) -> Option<T> {
        let _ = self.mutex.lock();
        self.data.pop_front()
    }

    // fn find_first_sgm(&self) -> Result<Segment<T>, Box<dyn Error>> {
    //     let first = fs::read_dir(self.file_dir)?
    //         .map(|e| e?.file_name())
    //         .map(|e| e.to_str()?[self.file_prefix.len()+1..])
    //         .map(|f| f.parse::<i32>()?).max()?;
    //     Ok(Self::load(format!("{}_{}", self.file_prefix, first)))
    // }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::thread;
    use tempfile::NamedTempFile;

    #[test]
    fn segment_append() {
        let mut segment = Segment::<i32>::new(NamedTempFile::new().unwrap().path().to_path_buf());
        segment.add(1).unwrap();
        assert_eq!(segment.remove(), 1);
    }

    #[test]
    fn segment_batch_append() {
        let mut segment = Segment::<i32>::new(NamedTempFile::new().unwrap().path().to_path_buf());
        let data = vec![0, 1, 2, 3];
        segment.add_batch(data).unwrap();
        assert_eq!(segment.remove(), 0);
        assert_eq!(segment.remove(), 1);
        assert_eq!(segment.remove(), 2);
        assert_eq!(segment.remove(), 3);
    }

    #[test]
    fn segment_mixed_append() {
        let mut segment = Segment::<i32>::new(NamedTempFile::new().unwrap().path().to_path_buf());
        let data = vec![0, 1, 2, 3];
        segment.add_batch(data).unwrap();
        assert_eq!(segment.remove(), 0);
        assert_eq!(segment.remove(), 1);
        segment.add(4).unwrap();
        assert_eq!(segment.remove(), 2);
        segment.add(5).unwrap();
        assert_eq!(segment.remove(), 3);
        assert_eq!(segment.remove(), 4);
        assert_eq!(segment.remove(), 5);
    }

    #[test]
    fn segment_append_multi() {
        let mut segment = Segment::<i32>::new(NamedTempFile::new().unwrap().path().to_path_buf());
        segment.add(1).unwrap();
        segment.add(2).unwrap();
        assert_eq!(segment.remove(), 1);
        assert_eq!(segment.remove(), 2);
    }

    #[test]
    fn segment_load() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut segment = Segment::<i32>::new(file.path().to_path_buf());
            segment.add(1).unwrap();
            segment.add(2).unwrap();
            segment.add(3).unwrap();
        }
        let mut segment = Segment::<i32>::load(file.path().to_path_buf()).unwrap();
        assert_eq!(segment.remove(), 1);
        assert_eq!(segment.remove(), 2);
        assert_eq!(segment.remove(), 3);
    }

    #[test]
    fn segment_load_with_deletes() {
        let file = NamedTempFile::new().unwrap();
        {
            let mut segment = Segment::<i32>::new(file.path().to_path_buf());
            segment.add(1).unwrap();
            segment.add(2).unwrap();
            segment.add(3).unwrap();
            segment.remove();
            segment.remove();
        }
        let mut segment = Segment::<i32>::load(file.path().to_path_buf()).unwrap();
        assert_eq!(segment.remove(), 3);
    }

    #[test]
    fn segment_load_with_deletes_big_range() {
        let file = NamedTempFile::new().unwrap();
        let end = 10;
        let range = std::ops::Range { start: 0, end };
        {
            let mut segment = Segment::<i32>::new(file.path().to_path_buf());
            for e in range {
                // println!("Add {:?}", e);
                segment.add(e).unwrap();
            }
            for i in 0..(end - 1) {
                // println!("Remove {:?}", i);
                segment.remove();
            }
        }
        let mut segment = Segment::<i32>::load(file.path().to_path_buf()).unwrap();
        assert_eq!(segment.remove(), end - 1);
    }

    // #[test]
    // fn segment_multithread_test() {
    //     let file = NamedTempFile::new().unwrap();
    //     let mut segment = Segment::<i32>::new(file.path().to_path_buf());
    //     thread::spawn(move || {
    //         println!("{:?}", segment.remove());
    //     });
    //     segment.add(1).unwrap();
    //     segment.add(2).unwrap();
    // }
}

impl ToBytes for i32 {
    fn bytes(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl FromBytes for i32 {
    fn from_bytes(bytes: Vec<u8>) -> Self {
        assert!(bytes.len() == 4);
        i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    }
}

pub struct Storage<Id, Value> {
    data_path: std::path::PathBuf,
    ack_path: std::path::PathBuf,
    data_mutex: std::sync::Mutex<bool>,
    ack_mutex: tokio::sync::Mutex<bool>,
    phantom_id: PhantomData<Id>,
    phantom_value: PhantomData<Value>,
}

impl<Id: ToBytes + FromBytes, Value: ToBytes + FromBytes> Storage<Id, Value> {
    pub fn new(data_path: PathBuf, ack_path: PathBuf) -> Self {
        Self {
            data_path,
            ack_path,
            data_mutex: std::sync::Mutex::new(false),
            ack_mutex: tokio::sync::Mutex::new(false),
            phantom_id: PhantomData,
            phantom_value: PhantomData,
        }
    }

    pub fn load(&self) -> Vec<(Id, Value)> {
        unimplemented!()
    }

    pub fn persist(&self, element: (Id, Value)) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.data_mutex.lock();
        unimplemented!()
    }

    pub fn persist_all(
        &self,
        elements: Vec<(Id, Value)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.data_mutex.lock();
        unimplemented!()
    }

    pub async fn remove(&self, id: Id) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.ack_mutex.lock().await;
        unimplemented!()
    }

    fn compaction(&self) -> Result<(), Box<dyn std::error::Error>> {
        unimplemented!()
    }
}
