#![allow(dead_code)]
#![allow(unused_imports)]

use rocksdb::*;
use serde_derive::{Deserialize, Serialize};
use std::collections::*;
use std::error::Error;
use std::fs::*;
use std::io::prelude::*;
use std::iter::*;
use std::path::*;
use std::sync::*;
use std::*;

use shared_lib::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DiskPublish {
    packet_id: u16,
    retain: bool,
    topic_name: String,
    payload_id: u64,
}

pub struct GCStorage {
    db: DB,
}

impl Storage for GCStorage {
    fn write(&mut self, session_id: &str, publishes: &[Publish]) -> Result<(), Box<dyn Error>> {
        let mut batch = WriteBatch::default();
        let payloads_cf = self
            .db
            .cf_handle("Payloads")
            .expect("Payloads should exist");

        let messages: Vec<DiskPublish> = publishes
            .iter()
            .map(|publish| {
                let bytes: &Vec<u8> = &(publish.payload.bytes);

                batch.put_cf(&payloads_cf, publish.payload.id.to_ne_bytes(), bytes);

                DiskPublish {
                    packet_id: publish.packet_id,
                    retain: publish.retain,
                    topic_name: publish.topic_name.clone(),
                    payload_id: publish.payload.id,
                }
            })
            .collect();

        batch.put_cf(
            self.db
                .cf_handle("Messages")
                .expect("Messages should exist"),
            session_id,
            bincode::serialize(&messages)?,
        );

        self.db.write(batch);

        Ok(())
    }

    fn read(&mut self, session_id: &str) -> Result<Vec<Publish>, Box<dyn Error>> {
        let bodies: Vec<u8> = self
            .db
            .get_cf(
                self.db
                    .cf_handle("Messages")
                    .expect("Messages should exist"),
                session_id,
            )?
            .unwrap_or_default();

        let bodies: Vec<DiskPublish> = bincode::deserialize_from(&bodies[..])?;

        let payloads_cf = self
            .db
            .cf_handle("Payloads")
            .expect("Payloads should exist");
        let result: Vec<Publish> = bodies
            .iter()
            .map(|body| {
                let payload: Vec<u8> = self
                    .db
                    .get_cf(payloads_cf, body.payload_id.to_ne_bytes())
                    .unwrap()
                    .unwrap();

                Publish {
                    packet_id: body.packet_id,
                    retain: body.retain,
                    topic_name: body.topic_name.clone(),
                    payload: Payload {
                        id: body.payload_id,
                        bytes: Arc::new(payload),
                    },
                }
            })
            .collect();

        Ok(result)
    }
}

impl GCStorage {
    pub fn new(location: &Path) -> Result<Self, Box<dyn Error>> {
        let options = Options::default();

        let mut db = DB::open_default(location)?;

        db.create_cf("Payloads", &options);
        db.create_cf("Messages", &options);

        Ok(GCStorage { db })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn rocks_db_works() {
        let path = tempdir().unwrap().into_path();

        let db = DB::open_default(path).unwrap();
        db.put(b"my key", b"my value").unwrap();

        let test = db.get(b"my key").unwrap().unwrap();
        assert_eq!(test, b"my value");

        db.delete(b"my key").unwrap();
        assert_eq!(None, db.get(b"my key").unwrap());
    }

    #[test]
    fn test_read_write() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make storage");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", &[publish]).expect("Publish 1");

        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));
    }

    struct Faker {
        packet_id: u16,
        payload_id: u64,
    }

    impl Faker {
        fn new() -> Self {
            Faker {
                packet_id: 100,
                payload_id: 1000,
            }
        }

        fn make_fake_publish(&mut self, payload: Vec<u8>) -> Publish {
            self.packet_id += 2;
            self.payload_id += 1;

            Publish {
                packet_id: self.packet_id,
                payload: Payload {
                    id: self.payload_id,
                    bytes: Arc::new(payload),
                },
                retain: true,
                topic_name: "fake".to_owned(),
            }
        }
    }
}
