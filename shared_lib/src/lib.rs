use std::error::Error;
use std::sync::Arc;
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Publish {
    pub packet_id: u16,
    pub retain: bool,
    pub topic_name: String,
    pub payload: Payload,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    pub bytes: Arc<Vec<u8>>,
    pub id: u64,
}

impl Clone for Payload {
    fn clone(&self) -> Self {
        Payload {
            id: self.id,
            bytes: self.bytes.clone(),
        }
    }
}

pub trait Storage {
    fn write(&mut self, session_id: &str, publishes: &[Publish]) -> Result<(), Box<dyn Error>>;
    fn read(&mut self, session_id: &str) -> Result<Vec<Publish>, Box<dyn Error>>;
}
