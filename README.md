# Secure Fusion

A transparent database encryption proxy, built on top of Apache DataFusion.

## Disclaimer

This is an experimental research prototype. DO NOT USE IN PRODUCTION. YOU MAY EXPERIENCE DATA LOSS. 

THIS PROGRAM HAS NOT BEEN AUDITED AND SHOULD NOT BE RELIED UPPON FOR SECURITY.

## Set-up

Compile a release using `cargo build --release`.

Available feature flags:

- `snmalloc` (default): Use `snmalloc` as allocator
- `jemalloc`: use `jemalloc` as allocator
- `tracing`: enables tracing support for queries

Edit the configuration (`config.toml`) to match your server, then start the proxy using `./target/release/server`. You must have an already running MySQL/MariaDB server.

Connect to it using the `mysql` or `mariadb` client, as you would with any other MySQL server. The default port is `13306`.

### Note on client support

Some client expect a certain minimal version number anounced by the server. For example, the Python MySQL client has problems with the version announced by the proxy. You can change that version by editing file `./frontends/mysql/src/connection_phase/connection_phase_state.rs`, line 152 (which contains the version string), and recompiling.

## Missing features

- TLS support. Has been implemented in a still-internal branch.
- Join Pushdown only works in some specific cases.

## Usage

You can use the server as you would use any MySQL server. When creating tables, you may add the `ENCRYPTED` or `DECRYPTED` keywords, like so:

```sql

CREATE TABLE item (
  i_id    int,
  i_name  varchar(24) ENCRYPTED,
  i_price decimal(5, 2) ENCRYPTED,
  i_data  varchar(50) ENCRYPTED,
  i_im_id int ENCRYPTED,
  PRIMARY KEY (i_id)
)

```

To create encrypted indices:

- Blind index: append `USING blind_bits_<num bits>` after the index creation, like so: `CREATE INDEX customer_c_last ON customer USING blind_bits_13 (c_last);`

- Full text search: append `USING inverted_index` after the index creation, like so: `CREATE INDEX fts ON enron_emails USING inverted_index (subject, body)`. Use normal `MATCH` queries to use, or alternatively, the `LIKE` keyword