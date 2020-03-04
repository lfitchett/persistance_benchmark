use std::sync::*;

use criterion::*;
use tempfile::tempdir;

use gc_test::{Payload as GcPayload, Publish as GcPublish, DB as GcDB};

fn gc_read_write(c: &mut Criterion) {
    c.bench_function("read/write", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = GcDB::new(dir.as_ref()).expect("Make db");
                let faker = Faker::new();

                (dir, db, faker)
            },
            |(_dir, mut db, mut faker)| {
                db.write("Session 1", faker.gc_publish(vec![1, 2, 3, 4, 5]))
                    .expect("Publish 1");

                let stored = db.read("Session 1").unwrap();

                assert_eq!(stored.len(), 1);
                assert_eq!(stored[0].topic_name, "fake");
                assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, gc_read_write);
criterion_main!(benches);

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

    fn gc_publish(&mut self, payload: Vec<u8>) -> GcPublish {
        self.packet_id += 2;
        self.payload_id += 1;

        GcPublish {
            packet_id: self.packet_id,
            payload: GcPayload {
                id: self.payload_id,
                bytes: Arc::new(payload),
            },
            retain: true,
            topic_name: "fake".to_owned(),
        }
    }
}
