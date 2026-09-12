use crate::datasets::Dataset;
use crate::loaders::gzip::GzipDataset;
use crate::loaders::http::HttpLoader;
use crate::loaders::{DatasetLoader, LoadedDataset, emails};
use hex_literal::hex;
use mysql::prelude::Queryable;
use mysql::{Conn, Params, Value};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::AddAssign;
use std::time::Duration;
use std::{fs, io};

const ENRON_LOADER: HttpLoader<GzipDataset> = HttpLoader::new(
    "enron",
    "https://www.cs.cmu.edu/~enron/enron_mail_20150507.tar.gz",
    Some(hex!(
        "b3da1b3fe0369ec3140bb4fbce94702c33b7da810ec15d718b3fadf5cd748ca7"
    )),
);

mod queries {
    /// A keyword with selectivity of 15
    pub(super) const ALPHA: &str = "greyhawk";

    /// A keyword with a selectivity of 1948
    pub(super) const BETA: &str = "assess";

    pub(super) const VARIABLE_TERMS: [(&str, usize); 17] = [
        ("pricecontrol", 2),
        ("eurofund", 8),
        ("retrain", 16),
        ("differentiation", 64),
        ("maneuvers", 128),
        ("intimate", 256),
        ("veterans", 512),
        ("withdraw", 1024),
        ("framework", 2048),
        ("ground", 4095),        // no word ==4096
        ("court", 8194),         // wo word ==8192
        ("requirements", 12282), // 12 288
        ("tell", 16359),         // 16 384
        ("while", 32879),        // rougly 2^15
        ("contact", 65942),      // roughly 2^16
        ("know", 135390),        // roughly 2^17
        ("have", 258662),        // roughly 2^18
    ];
}

pub struct Enron {
    pub use_proxy: bool,
}

impl Dataset for Enron {
    fn init_database(&mut self, conn: &mut Conn) {
        let tables = conn
            .query::<String, _>("SHOW TABLES")
            .expect("Failed to list tables");

        if tables.contains(&"enron_emails".to_string()) {
            println!("table already exists, dropping");
            self.drop_database(conn);
        }

        if self.use_proxy {
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

            /*
            conn.query_drop(
                "CREATE INDEX blind_from ON enron_emails USING blind_bits_12 (email_from)",
            )
            .expect("Failed creating index");

            conn.query_drop("CREATE INDEX blind_to ON enron_emails USING blind_bits_12 (email_to)")
                .expect("Failed creating index"); */
        } else {
            conn.query_drop(
                "CREATE TABLE enron_emails(\
            id INTEGER PRIMARY KEY, \
            subject TEXT, \
            body MEDIUMTEXT, \
            email_from TEXT, \
            email_to TEXT \
        )",
            )
            .expect("Failed creating table");
        }
    }

    fn drop_database(&mut self, conn: &mut Conn) {
        conn.query_drop("DROP TABLE enron_emails")
            .expect("Can't drop table");
    }

    fn post_insert_data(&mut self, conn: &mut Conn) {
        if self.use_proxy {
            conn.query_drop(
                "CREATE INDEX fts ON enron_emails USING inverted_index (subject, body)",
            )
            .expect("Failed creating index");
        } else {
            conn.query_drop("CREATE FULLTEXT INDEX fts ON enron_emails (subject, body)")
                .expect("Failed creating index");
        }
    }

    fn load_insert_data(&mut self, conn: &mut Conn) {
        let mut data = ENRON_LOADER.skip_hash_check().load();
        let statement = conn
            .prep("INSERT INTO enron_emails VALUES (?, ?, ?, ?, ?)")
            .expect("Failed preparing insert data");

        print!("Inserting: 0");
        io::stdout().flush().expect("flushing stdout");

        let start = std::time::Instant::now();
        let mut frequency_list: HashMap<String, usize> = HashMap::new(); // Used to determine term selectivity

        let iter = data
            .iter()
            .map(emails::parse_email)
            .enumerate()
            .map(|(id, email)| {
                let subject = email.header.subject.unwrap_or_else(|| "".to_string());

                let words = extract_keywords(&email.body)
                    .into_iter()
                    .chain(extract_keywords(&subject).into_iter())
                    .collect::<HashSet<_>>();

                for word in words {
                    frequency_list.entry(word).or_default().add_assign(1);
                }

                if id % 100 == 0 {
                    let dur = start.elapsed();
                    let items_per_sec = (id as f64) / dur.as_secs_f64();

                    print!("\rInserting: {id} ({items_per_sec} i/sec)");
                    io::stdout().flush().expect("flushing stdout");
                }

                Params::Positional(vec![
                    Value::Int(id as i64),
                    Value::Bytes(subject.into_bytes()),
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
        sort_dump_frequency_list(frequency_list);

        println!("Done");
    }

    fn run_benchmark(&self, conn: &mut Conn) {
        println!("Running single term benchmarks");
        let mut full_results = String::from("query,mean,stdev,p95,p99\n");
        for (term, count) in queries::VARIABLE_TERMS {
            full_results += &benchmark_query(conn, count.to_string().as_str(), &[term]);
        }

        results_to_file(full_results, "single_term.csv");

        println!("Running ALPHA benchmarks");
        let mut full_results = String::from("query,mean,stdev,p95,p99\n");
        for (term, count) in queries::VARIABLE_TERMS {
            full_results +=
                &benchmark_query(conn, count.to_string().as_str(), &[queries::ALPHA, term]);
        }

        results_to_file(full_results, "alpha_term.csv");

        println!("Running BETA benchmarks");
        let mut full_results = String::from("query,mean,stdev,p95,p99\n");
        for (term, count) in queries::VARIABLE_TERMS {
            full_results +=
                &benchmark_query(conn, count.to_string().as_str(), &[queries::BETA, term]);
        }

        results_to_file(full_results, "beta_term.csv");

        println!("Running ALPHA + BETA benchmarks");
        let mut full_results = String::from("query,mean,stdev,p95,p99\n");
        for (term, count) in queries::VARIABLE_TERMS {
            full_results += &benchmark_query(
                conn,
                count.to_string().as_str(),
                &[queries::ALPHA, queries::BETA, term],
            );
        }

        results_to_file(full_results, "alpha_beta_terms.csv");
    }
}

macro_rules! timed {
    ($($token:tt)+) => {
         {
             let _instant = std::time::Instant::now();
             let _ = std::hint::black_box($($token)+);
             _instant.elapsed()
         }
    };
}

fn benchmark_query(conn: &mut Conn, label: &str, query: &[&str]) -> String {
    let keywords = query
        .iter()
        .map(|s| format!("+{s}"))
        .collect::<Vec<_>>()
        .join(" ");

    let query = format!(
        "SELECT COUNT(id) FROM enron_emails WHERE MATCH (subject, body) AGAINST ('{keywords}' IN BOOLEAN MODE)"
    );

    println!(
        "Benchmarking query {label}: {keywords}. Running 10 iterations to estimate runtime and warmup..."
    );

    let mut total_time: Duration = Duration::from_secs(0);
    for _ in 0..10 {
        total_time += timed!(conn.query_drop(query.clone()).unwrap());
    }

    // target 15 seconds runtime for each query
    let time_per_iter = total_time.as_micros() / 10;
    let num_iter = if time_per_iter == 0 {
        15_000_000usize // less than 1us per iteration
    } else {
        max(100, (15_000_000 / time_per_iter) as usize)
    };

    println!("Completed 10 iterations in {total_time:?}... Running {num_iter} iterations.");
    let mut results = Vec::new();
    for _ in 0..num_iter {
        results.push(timed!(conn.query_drop(query.clone()).unwrap()).as_micros());
    }

    results.sort();
    let len = results.len();

    let average = results.iter().sum::<u128>() as f64 / len as f64;

    let average_of_squares = (results.iter().map(|v| v * v).sum::<u128>() as f64) / len as f64;
    let std_dev = (average_of_squares - average * average).sqrt();

    let p95 = results[(len / 100) * 95];
    let p99 = results[(len / 100) * 99];

    format!("{label},{average},{std_dev},{p95},{p99}\n")
}

fn results_to_file(results: String, file_name: &str) {
    let path = format!("./results/{file_name}");
    let path = std::path::Path::new(&path);
    let parent_dir = path.parent().unwrap();
    if !fs::exists(parent_dir).expect("Critical: failed to check if dataset directory exists") {
        fs::create_dir_all(parent_dir).expect("Critical: failed to create parent dir");
    }

    OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("Critical: failed to open file")
        .write_all(results.as_bytes())
        .expect("Critical: failed to write to file");
}
fn sort_dump_frequency_list(frequency_list: HashMap<String, usize>) {
    println!("Sorting frequency list...");
    let mut frequency_list: Vec<_> = frequency_list.into_iter().collect();

    frequency_list.sort_by_key(|(_, frequency)| -(*frequency as isize));

    println!("Collecting frequency list...");
    let mut frequency_list = frequency_list
        .into_iter()
        .map(|(word, freq)| format!("{}\t\t{}\n", word, freq))
        .collect::<String>();

    let path = std::path::Path::new("./selectivity/enron.txt");
    let parent_dir = path.parent().unwrap();
    if !fs::exists(parent_dir).expect("Critical: failed to check if dataset directory exists") {
        fs::create_dir_all(parent_dir).expect("Critical: failed to create parent dir");
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("Critical: failed to open file");

    println!("Writing frequency list...");
    file.write_all(frequency_list.as_bytes())
        .expect("Critical: failed to write to file");
}
fn extract_keywords(string: &str) -> Vec<String> {
    let string = string.trim().to_lowercase();
    string
        .split(|c: char| c.is_ascii_punctuation() || c.is_ascii_control() || c.is_whitespace())
        .filter(|word| {
            !word.is_empty() && (word.len() <= 32) && (word.chars().all(char::is_alphabetic))
        })
        .map(|s| s.to_string())
        .collect()
}
