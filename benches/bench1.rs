use std::sync::*;

use criterion::*;
use tempfile::tempdir;

use gc_test::{Payload as GcPayload, Publish as GcPublish, DB as GcDB};
use load_consolidate_test::{Publish as LcPublish, DB as LcGC};

fn gc_read_write_single(c: &mut Criterion) {
    c.bench_function("gc_read_write_single", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = GcDB::new(dir.as_ref()).expect("Make db");
                let data = Faker::new().gc_publish(&[vec![1, 2, 3, 4, 5]]);

                (dir, db, data)
            },
            |(_dir, mut db, data)| {
                db.write("Session 1", &data).expect("Publish 1");

                let stored = db.read("Session 1").unwrap();

                assert_eq!(stored.len(), 1);
                assert_eq!(stored[0].topic_name, "fake");
                assert_eq!(stored[0].payload.bytes, Arc::new(vec![1, 2, 3, 4, 5]));
            },
            BatchSize::SmallInput,
        );
    });
}

fn gc_read_write_many_small_payload(c: &mut Criterion) {
    c.bench_function("gc_read_write_many_small_payload", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = GcDB::new(dir.as_ref()).expect("Make db");

                let payloads: Vec<Vec<u8>> = (0..5).map(|i| vec![i]).collect();
                let data = Faker::new().gc_publish(&payloads);

                (dir, db, data)
            },
            |(_dir, mut db, data)| {
                db.write("Session 1", &data).expect("Publish 1");
                db.read("Session 1").unwrap()
            },
            BatchSize::SmallInput,
        );
    });
}

fn lc_read_write_single(c: &mut Criterion) {
    c.bench_function("lc_read_write_single", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = LcGC::new(dir.as_ref());
                let data = Faker::new().lc_publish(&[vec![1, 2, 3, 4, 5]]);

                (dir, db, data)
            },
            |(_dir, mut db, data)| {
                db.write("Session 1", &data).expect("Publish 1");

                let stored = db.read("Session 1").unwrap();

                assert_eq!(stored.len(), 1);
                assert_eq!(stored[0].topic_name, "fake");
                assert_eq!(stored[0].payload, Arc::new(vec![1, 2, 3, 4, 5]));
            },
            BatchSize::SmallInput,
        );
    });
}

fn lc_read_write_many_small_payload(c: &mut Criterion) {
    c.bench_function("lc_read_write_many_small_payload", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = LcGC::new(dir.as_ref());

                let payloads: Vec<Vec<u8>> = (0..100).map(|i| vec![i]).collect();
                let data = Faker::new().lc_publish(&payloads);

                (dir, db, data)
            },
            |(_dir, mut db, data)| {
                db.write("Session 1", &data).expect("Publish 1");
                db.read("Session 1").unwrap()
            },
            BatchSize::SmallInput,
        );
    });
}

fn lc_read_write_many_small_payload_many_session(c: &mut Criterion) {
    c.bench_function("lc_read_write_many_small_payload_many_session", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = LcGC::new(dir.as_ref());

                let payloads: Vec<Vec<u8>> = (0..100).map(|i| vec![i]).collect();
                let data = Faker::new().lc_publish(&payloads);

                (dir, db, data)
            },
            |(_dir, mut db, data)| {
                let mut test = Vec::with_capacity(10);

                for i in 0..5 {
                    let name = format!("Session {}", i);
                    db.write(&name, &data).expect("Publish 1");
                    let result = db.read(&name).unwrap();

                    test.push(result);
                }

                test
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    garbage_collection,
    gc_read_write_single,
    gc_read_write_many_small_payload
);
criterion_group!(
    load_consolidation,
    lc_read_write_single,
    lc_read_write_many_small_payload,
    lc_read_write_many_small_payload_many_session
);

// criterion_main!(garbage_collection, load_consolidation);
criterion_main!(load_consolidation);

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

    fn gc_publish(&mut self, payloads: &[Vec<u8>]) -> Vec<GcPublish> {
        payloads
            .iter()
            .map(|payload| {
                self.packet_id += 2;
                self.payload_id += 1;

                GcPublish {
                    packet_id: self.packet_id,
                    payload: GcPayload {
                        id: self.payload_id,
                        bytes: Arc::new(payload.clone()),
                    },
                    retain: true,
                    topic_name: "fake".to_owned(),
                }
            })
            .collect()
    }

    fn lc_publish(&mut self, payloads: &[Vec<u8>]) -> Vec<LcPublish> {
        payloads
            .iter()
            .map(|payload| {
                self.packet_id += 2;
                self.payload_id += 1;

                LcPublish {
                    packet_id: self.packet_id,
                    payload: Arc::new(payload.clone()),
                    retain: true,
                    topic_name: "fake".to_owned(),
                }
            })
            .collect()
    }
}
