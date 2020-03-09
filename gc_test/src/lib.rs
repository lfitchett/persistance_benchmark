#![allow(dead_code)]
#![allow(unused_imports)]

use bytes::*;
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
    payloads: PathBuf,
    sessions: PathBuf,
    loaded_payloads: HashMap<u64, Weak<Vec<u8>>>,
}

impl Storage for GCStorage {
    fn write(&mut self, session_id: &str, publishes: &[Publish]) -> Result<(), Box<dyn Error>> {
        for publish in publishes {
            let payload = publish.payload.clone();

            let body = DiskPublish {
                packet_id: publish.packet_id,
                retain: publish.retain,
                topic_name: publish.topic_name.clone(),
                payload_id: payload.id,
            };

            self.write_body(session_id, body)?;
            self.write_payload_if_empty(payload)?;
        }
        Ok(())
    }

    fn read(&mut self, session_id: &str) -> Result<Vec<Publish>, Box<dyn Error>> {
        let session_root = self.sessions.join(session_id);
        if !session_root.exists() {
            return Ok(Vec::new());
        }

        let messages = session_root.join("Messages");

        let result = fs::read_dir(messages)?
            .filter_map(|f| if let Ok(f) = f { Some(f.path()) } else { None })
            .map(|p| self.parse_body(&p))
            .filter_map(|p| if let Ok(p) = p { Some(p) } else { None })
            .collect();

        Ok(result)
    }
}

impl GCStorage {
    pub fn new(location: &Path) -> Result<Self, Box<dyn Error>> {
        let payloads = location.join("Payloads");
        let sessions = location.join("Sessions");

        if !payloads.exists() {
            create_dir(&payloads)?;
        }
        if !sessions.exists() {
            create_dir(&sessions)?;
        }
        Ok(GCStorage {
            payloads,
            sessions,
            loaded_payloads: HashMap::new(),
        })
    }

    pub fn clean(&mut self) -> Result<(), Box<dyn Error>> {
        // Collect stored and referenced payload ids
        let stored_ids: HashSet<u64> = HashSet::from_iter(self.get_payload_ids()?);

        let mut referenced_ids: HashSet<u64> = HashSet::new();
        for session_id in self.get_session_ids()? {
            for payload_id in self.get_session_payload_ids(&session_id)? {
                referenced_ids.insert(payload_id);
            }
        }

        // Delete unreferenced payloads
        let ids_to_remove = stored_ids.difference(&referenced_ids);
        for id_to_remove in ids_to_remove {
            self.delete_payload(*id_to_remove)?;
        }

        // Clean loaded payloads
        self.loaded_payloads.retain(|_, val| val.strong_count() > 0);

        Ok(())
    }

    fn write_body(&self, session_id: &str, body: DiskPublish) -> Result<(), Box<dyn Error>> {
        let dir = self.sessions.join(session_id).join("Messages");
        if !dir.exists() {
            create_dir_all(&dir)?;
        }

        let bytes = bincode::serialize(&body)?;

        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(dir.join(body.payload_id.to_string()))?
            .write_all(&bytes)?;

        Ok(())
    }

    fn write_payload_if_empty(&self, payload: Payload) -> Result<(), Box<dyn Error>> {
        let path = self.payloads.join(payload.id.to_string());
        if !path.exists() {
            File::create(path)?.write_all(&payload.bytes)?;
        }

        Ok(())
    }

    fn parse_body(&mut self, path: &Path) -> Result<Publish, Box<dyn Error>> {
        let body: DiskPublish = bincode::deserialize_from(File::open(path)?)?;
        let payload = self.get_payload(body.payload_id)?;

        Ok(Publish {
            packet_id: body.packet_id,
            retain: body.retain,
            topic_name: body.topic_name,
            payload,
        })
    }

    fn get_payload(&mut self, payload_id: u64) -> Result<Payload, Box<dyn Error>> {
        if let Some(bytes) = self.loaded_payloads.get(&payload_id) {
            if let Some(b) = bytes.upgrade() {
                return Ok(Payload {
                    id: payload_id,
                    bytes: b,
                });
            }
        }

        let mut file = File::open(self.payloads.join(payload_id.to_string()))?;
        let mut buffer: Vec<u8> = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buffer)?;
        let bytes = Arc::new(buffer);

        self.loaded_payloads
            .insert(payload_id, Arc::downgrade(&bytes));

        Ok(Payload {
            id: payload_id,
            bytes,
        })
    }

    fn delete_payload(&mut self, payload_id: u64) -> Result<(), Box<dyn Error>> {
        remove_file(self.payloads.join(payload_id.to_string()))?;
        self.loaded_payloads.remove(&payload_id);

        Ok(())
    }

    fn get_session_ids(&self) -> Result<Vec<String>, Box<dyn Error>> {
        Self::list_children(&self.sessions)
    }

    fn get_session_payload_ids(&self, session_id: &str) -> Result<Vec<u64>, Box<dyn Error>> {
        let path = self.sessions.join(session_id).join("Messages");
        let result: Result<Vec<u64>, _> = Self::list_children(path)?
            .iter()
            .map(|p| p.parse())
            .collect();

        Ok(result?)
    }

    fn get_payload_ids(&self) -> Result<Vec<u64>, Box<dyn Error>> {
        let result: Result<Vec<u64>, _> = Self::list_children(&self.payloads)?
            .iter()
            .map(|p| p.parse())
            .collect();

        Ok(result?)
    }

    fn list_children<P>(path: P) -> Result<Vec<String>, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let result = fs::read_dir(path)?
            .filter_map(|f| if let Ok(f) = f { Some(f.path()) } else { None })
            .filter_map(|f| f.file_name().map(|f| f.to_owned()))
            .filter_map(|f| {
                if let Ok(f) = f.into_string() {
                    Some(f)
                } else {
                    None
                }
            })
            .collect();

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_clean() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", publish.clone()).expect("Publish 1");

        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));

        // payload still has referances, clean does nothing
        db.clean().expect("Clean");
        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));

        remove_file(
            path.join("Sessions")
                .join("Session 1")
                .join("Messages")
                .join(publish.payload.id.to_string()),
        )
        .unwrap();

        // payload has no referances, clean deletes
        db.clean().expect("Clean");
        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 0);
    }

    #[test]
    fn test_read_write() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        db.write("Session 1", faker.make_fake_publish(vec![1, 2, 3, 4, 5]))
            .expect("Publish 1");

        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_read_write_multiple() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        db.write("Session 1", faker.make_fake_publish(vec![1, 2, 3]))
            .expect("Publish 1");
        db.write("Session 1", faker.make_fake_publish(vec![4, 5, 6]))
            .expect("Publish 2");

        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 2);
        assert!(stored
            .iter()
            .any(|p| p.payload.bytes == Arc::new(vec![1, 2, 3])));
        assert!(stored
            .iter()
            .any(|p| p.payload.bytes == Arc::new(vec![4, 5, 6])));
    }

    #[test]
    fn test_shared_payload() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", publish.clone()).expect("Publish 1");
        db.write("Session 2", publish).expect("Publish 2");

        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));

        let stored = db.read("Session 2").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_shared_payload_in_memory() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", publish.clone()).expect("Publish 1");
        db.write("Session 2", publish.clone()).expect("Publish 2");

        let stored = db.read("Session 1").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));

        // Ensure it is using cached data by deleting from disk.
        // Normally the referance in cache should never be deleted from disk before all referances are gone.
        remove_file(path.join("Payloads").join(publish.payload.id.to_string())).unwrap();

        let stored = db.read("Session 2").unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_add_remove_payload() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", publish.clone()).expect("Publish 1");

        let stored = db.read("Session 1").expect("Get Payload");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].topic_name, "fake");
        assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));

        db.delete_payload(publish.payload.id)
            .expect("Delete Payload");

        let stored = db.read("Session 1").expect("Get Payload");
        assert_eq!(stored.len(), 0);
    }

    #[test]
    fn test_get_session_ids() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", publish.clone()).expect("Publish 1");
        db.write("Session 2", publish).expect("Publish 2");

        assert_eq!(
            db.get_session_ids().unwrap(),
            vec!["Session 1", "Session 2"]
        )
    }

    #[test]
    fn test_get_session_payload_ids() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", publish.clone()).expect("Publish 1");

        assert_eq!(
            db.get_session_payload_ids("Session 1").unwrap(),
            vec![publish.payload.id]
        )
    }

    #[test]
    fn test_get_payload_ids() {
        let path = tempdir().unwrap().into_path();
        let mut db = GCStorage::new(&path).expect("Make db");
        let mut faker = Faker::new();

        let publish = faker.make_fake_publish(vec![1, 2, 3, 4, 5]);
        db.write("Session 1", publish.clone()).expect("Publish 1");

        assert_eq!(db.get_payload_ids().unwrap(), vec![publish.payload.id])
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
