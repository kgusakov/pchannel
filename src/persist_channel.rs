use std::sync::mpsc::*;
use std::thread;

struct PersistentChannel {}

impl PersistentChannel {
    fn channel() -> (SyncSender<i32>, Receiver<i32>) {
        let (sync_sender, receiver) = sync_channel(0);
        let (target_sender, target_receiver) = channel();
        thread::spawn(move || {
            let val = receiver.recv().unwrap();
            PersistentChannel::store(val);
            target_sender.send(val).unwrap();
        });
        (sync_sender, target_receiver)
    }
    
    fn store(val: i32) {
        println!("Storing {:?}", val)
    }
    
    
}
#[test]
fn persist_channel_test() {
    let (sender, receive) = PersistentChannel::channel();
    sender.send(2);
    thread::spawn(move || {
        assert_eq!(2, receive.recv().unwrap());
    });
}

struct AsyncModuleWrapper {}

impl AsyncModuleWrapper {
    fn start(processingModule: ProcessingModule) -> SyncSender<i32> {
        let (sender, receiver) = PersistentChannel::channel();
        thread::spawn(move || {
            let v = receiver.recv().unwrap();
            processingModule.process(v);
        });
        sender
    }
}

struct ProcessingModule {}

impl ProcessingModule {
    fn process(&self, message: i32) {}
    fn new() -> Self { ProcessingModule {} }
}