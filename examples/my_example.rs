extern crate equeue;

use std::error::Error;
use std::fmt::Debug;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::iter::Iterator;
use std::path::PathBuf;
use std::sync::mpsc::*;
use std::time::Instant;

fn main() {
    let end = 1000;

    {
        let range = std::ops::Range { start: 0, end };
        let mut segment = equeue::Segment::<i32>::new(PathBuf::from("/tmp/segment.data"));
        let now = Instant::now();
        {
            for e in range {
                // println!("Add {:?}", e);
                segment.add(e).unwrap();
            }
        }
        println!(
            "Per element fsync time elapsed: {:?}",
            now.elapsed().as_millis()
        );
    }

    {
        let range: Vec<i32> = std::ops::Range { start: 0, end }.collect();
        let mut segment = equeue::Segment::<i32>::new(PathBuf::from("/tmp/segment.data"));
        let now = Instant::now();
        {
            segment.add_batch(range);
        }
        println!("Batching Time elapsed: {:?}", now.elapsed().as_millis());
    }
    let mut segment = equeue::Segment::<i32>::load(PathBuf::from("/tmp/segment.data")).unwrap();
    assert_eq!(segment.remove(), end - 1);
}
