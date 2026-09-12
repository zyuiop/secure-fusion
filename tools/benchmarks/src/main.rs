use crate::datasets::Dataset;
use crate::datasets::enron::Enron;
use clap::Parser;

mod datasets;
mod loaders;

#[derive(Parser)]
struct Command {
    #[arg(short = 'H', long)]
    db_host: Option<String>,

    #[arg(short = 'u', long)]
    db_user: Option<String>,

    #[arg(short = 'p', long)]
    db_password: Option<String>,

    #[arg(short = 'P', long)]
    db_port: Option<u16>,

    db_name: String,

    #[arg(short, long)]
    encrypted: bool,

    #[arg(short, long)]
    init: bool,

    #[arg(short, long)]
    load: bool,

    #[arg(short, long)]
    run: bool,
}

fn main() {
    let command = Command::parse();

    let mut conn = mysql::Conn::new(
        format!(
            "mysql://{}:{}@{}:{}/{}",
            command.db_user.unwrap_or("root".to_string()),
            command.db_password.unwrap_or("password".to_string()),
            command.db_host.unwrap_or("127.0.0.1".to_string()),
            command.db_port.unwrap_or(3306),
            command.db_name
        )
        .as_str(),
    )
    .expect("Failed connecting to mysql");

    let mut enron = Enron {
        use_proxy: command.encrypted,
    };

    if command.init {
        enron.init_database(&mut conn);
    }

    if command.load {
        enron.load_insert_data(&mut conn);
        enron.post_insert_data(&mut conn);
    }

    if command.run {
        enron.run_benchmark(&mut conn);
    }
}
