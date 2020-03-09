use std::any::type_name;
use std::path::Path;
use std::sync::*;

use criterion::*;
use tempfile::tempdir;

use gc_test::GCStorage;
use load_consolidate_test::LoadConsolidateStorage;
use shared_lib::*;

fn test_write<S, F>(c: &mut Criterion, get_db: F, num_messages: u8, num_sessions: u8)
where
    S: Storage,
    F: Fn(&Path) -> S,
{
    let name = format!(
        "{}: write {} messages for {} sessions",
        type_name::<S>(),
        num_messages,
        num_sessions
    );

    c.bench_function(&name, |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = get_db(dir.as_ref());

                let raw_data: Vec<Vec<u8>> = (0..num_messages).map(|i| (0..i).collect()).collect();
                let publishes = Faker::new().make_publishes(&raw_data);
                let data = (0..num_sessions).map(move |i| {
                    (
                        format!("Session {}", i),
                        publishes.clone(),
                    )
                });

                (dir, db, data)
            },
            |(_dir, mut db, data)| {
                for (session, messages) in data {
                    db.write(&session, &messages).expect("Publish 1");
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn test_read<S, F>(c: &mut Criterion, get_db: F, num_messages: u8, num_sessions: u8)
where
    S: Storage,
    F: Fn(&Path) -> S,
{
    let name = format!(
        "{}: read {} messages for {} sessions",
        type_name::<S>(),
        num_messages,
        num_sessions
    );
    c.bench_function(&name, |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let mut db = get_db(dir.as_ref());

                let raw_data: Vec<Vec<u8>> = (0..num_messages).map(|i| (0..i).collect()).collect();
                let publishes = Faker::new().make_publishes(&raw_data);

                let sessions: Vec<String> = (0..num_sessions)
                    .map(|i| format!("Session {}", i))
                    .collect();
                for session in &sessions {
                    db.write(&session, &publishes).expect("Publish 1");
                }

                (dir, db, sessions)
            },
            |(_dir, mut db, sessions)| {
                for session in sessions {
                    black_box(db.read(&session)).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn lc_write_single(c: &mut Criterion) {
    test_write(c, LoadConsolidateStorage::new, 1, 1)
}

fn gc_write_single(c: &mut Criterion) {
    test_write(c, |p| GCStorage::new(p).expect("Make db"), 1, 1)
}

fn lc_read_single(c: &mut Criterion) {
    test_read(c, LoadConsolidateStorage::new, 1, 1)
}

fn lc_write_many(c: &mut Criterion) {
    test_write(c, LoadConsolidateStorage::new, 100, 1)
}

fn lc_read_many(c: &mut Criterion) {
    test_read(c, LoadConsolidateStorage::new, 100, 1)
}

fn gc_read_single(c: &mut Criterion) {
    test_read(c, |p| GCStorage::new(p).expect("Make db"), 1, 1)
}

fn gc_write_many(c: &mut Criterion) {
    test_write(c, |p| GCStorage::new(p).expect("Make db"), 100, 1)
}

fn gc_read_many(c: &mut Criterion) {
    test_read(c, |p| GCStorage::new(p).expect("Make db"), 100, 1)
}

criterion_group!(
    load_consolidation,
    lc_read_single,
    lc_write_single,
    lc_read_many,
    lc_write_many
);

criterion_group!(
    garbage_collection,
    gc_read_single,
    gc_write_single,
    gc_read_many,
    gc_write_many
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

    fn make_publishes(&mut self, payloads: &[Vec<u8>]) -> Vec<Publish> {
        payloads
            .iter()
            .map(|payload| {
                self.packet_id += 2;
                self.payload_id += 1;

                Publish {
                    packet_id: self.packet_id,
                    payload: Payload {
                        id: self.payload_id,
                        bytes: Arc::new(payload.clone()),
                    },
                    retain: true,
                    topic_name: "fake".to_owned(),
                }
            })
            .collect()
    }
}
