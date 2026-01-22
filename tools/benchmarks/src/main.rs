use crate::datasets::Dataset;
use crate::datasets::enron::Enron;

mod datasets;
mod loaders;

fn main() {
    let mut conn = mysql::Conn::new("mysql://root:root@127.0.0.1:10200/benchmarks")
        .expect("Failed connecting to mysql");

    let mut enron = Enron;
    enron.init_database(&mut conn);
    enron.load_insert_data(&mut conn);
    enron.post_insert_data(&mut conn);
}
