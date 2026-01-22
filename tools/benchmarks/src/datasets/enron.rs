use crate::datasets::Dataset;
use crate::loaders::gzip::GzipDataset;
use crate::loaders::http::HttpLoader;
use crate::loaders::{DatasetLoader, LoadedDataset, emails};
use hex_literal::hex;
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Value};
use std::io;
use std::io::Write;

const ENRON_LOADER: HttpLoader<GzipDataset> = HttpLoader::new(
    "enron",
    "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz",
    Some(hex!(
        "b3da1b3fe0369ec3140bb4fbce94702c33b7da810ec15d718b3fadf5cd748ca7"
    )),
);

pub struct Enron;

impl Dataset for Enron {
    fn init_database(&mut self, conn: &mut Conn) {
        conn.query_drop(
            "CREATE TABLE enron_emails(\
            id INTEGER PRIMARY KEY, \
            subject TEXT ENCRYPTED, \
            body TEXT ENCRYPTED, \
            email_from TEXT ENCRYPTED, \
            email_to TEXT ENCRYPTED \
        )",
        )
        .expect("Failed creating table");

        conn.query_drop("CREATE INDEX blind_from ON enron_emails USING blind_bits_12 (email_from)")
            .expect("Failed blind from index");

        conn.query_drop("CREATE INDEX blind_to ON enron_emails USING blind_bits_12 (email_to)")
            .expect("Failed blind to index");
    }

    fn drop_database(&mut self, conn: &mut Conn) {
        conn.query_drop("DROP TABLE enron_emails")
            .expect("Can't drop table");
    }

    fn post_insert_data(&mut self, conn: &mut Conn) {
        conn.query_drop("CREATE INDEX fts ON enron_emails USING inverted_index (subject, body)")
            .expect("Failed blind from index");
    }

    fn load_insert_data(&mut self, conn: &mut Conn) {
        let mut data = ENRON_LOADER.skip_hash_check().load();
        let statement = conn
            .prep("INSERT INTO enron_emails VALUES (?, ?, ?, ?, ?)")
            .expect("Failed preparing insert data");

        print!("Inserting: 0");
        io::stdout().flush().expect("flushing stdout");

        let start = std::time::Instant::now();

        let iter = data
            .iter()
            .map(emails::parse_email)
            .enumerate()
            .map(|(id, email)| {
                if id % 100 == 0 {
                    let dur = start.elapsed();
                    let items_per_sec = (id as f64) / dur.as_secs_f64();

                    print!("\rInserting: {id} ({items_per_sec} i/sec)");
                    io::stdout().flush().expect("flushing stdout");
                }

                Params::Positional(vec![
                    Value::Int(id as i64),
                    Value::Bytes(
                        email
                            .header
                            .subject
                            .unwrap_or_else(|| "unknown".to_string())
                            .into_bytes(),
                    ),
                    Value::Bytes(email.body.into_bytes()),
                    Value::Bytes(
                        email
                            .header
                            .from
                            .unwrap_or_else(|| "unknown".to_string())
                            .into_bytes(),
                    ),
                    Value::Bytes(
                        email
                            .header
                            .to
                            .unwrap_or_else(|| "unknown".to_string())
                            .into_bytes(),
                    ),
                ])
            });

        conn.exec_batch(statement, iter)
            .expect("Failed executing insert batch");
        println!();
        println!("Done");
    }
}
