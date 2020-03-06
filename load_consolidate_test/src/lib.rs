#![allow(dead_code)]
#![allow(unused_imports)]

use serde_derive::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::*;
use std::error::Error;
use std::fs::*;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::iter::*;
use std::path::*;
use std::sync::*;
use std::*;

use shared_lib::*;

pub struct DB {
    loaded_payloads: HashMap<u64, Weak<Vec<u8>>>,
    location: PathBuf,
}

impl Storage for DB {
    fn write(&mut self, session_id: &str, publish: &[Publish]) -> Result<(), Box<dyn Error>> {
        let bytes = bincode::serialize(publish)?;

        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(self.location.join(session_id))?
            .write_all(&bytes)?;

        Ok(())
    }

    fn read(&mut self, session_id: &str) -> Result<Vec<Publish>, Box<dyn Error>> {
        let mut publishes: Vec<Publish> =
            bincode::deserialize_from(File::open(self.location.join(session_id))?)?;

        for mut publish in publishes.iter_mut() {
            if let Some(payload) = self.loaded_payloads.get(&publish.payload.id) {
                if let Some(payload) = payload.upgrade() {
                    publish.payload.bytes = payload;
                    continue;
                }
            }

            self.loaded_payloads
                .insert(publish.payload.id, Arc::downgrade(&publish.payload.bytes));
        }

        Ok(publishes)
    }
}

impl DB {
    pub fn new(location: &Path) -> Self {
        Self {
            location: location.to_owned(),
            loaded_payloads: HashMap::new(),
        }
    }

    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_read_write() {
        let path = tempdir().unwrap().into_path();
        let mut db = DB::new(&path);
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", &[publish]).expect("Publish 1");

        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload, Arc::new(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_dedupe() {
        let path = tempdir().unwrap().into_path();
        let mut db = DB::new(&path);
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", &[publish.clone()])
            .expect("Publish 1");
        db.write("Session 2", &[publish]).expect("Publish 2");

        db.read("Session 1").unwrap();
        db.read("Session 2").unwrap();

        assert_eq!(db.loaded_payloads.len(), 1);
    }

    struct Faker {
        packet_id: u16,
    }

    impl Faker {
        fn new() -> Self {
            Faker { packet_id: 100 }
        }

        fn make_fake_publish(&mut self, payload: Vec<u8>) -> Publish {
            self.packet_id += 2;

            Publish {
                packet_id: self.packet_id,
                payload: Arc::new(payload),
                retain: true,
                topic_name: "fake".to_owned(),
            }
        }
    }
}
