use rocksdb::{Options, DB};

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
}
