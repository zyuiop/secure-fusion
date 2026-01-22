use mysql_backend::MySqlBackendConfig;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;
use toml::macros::Deserialize;

macro_rules! gen_config {
    ($backend_config: ty, $name: ident) => {
        #[derive(serde::Deserialize, serde::Serialize)]
        pub struct $name {
            pub backend_config: $backend_config,
            pub secret_key: String,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    backend_config: Default::default(),
                    secret_key: "b6fd00728958b706fde7f9d5fde9637a5feadab81688b480ae66dcc3393f1284"
                        .to_string(),
                }
            }
        }
    };
}

gen_config!(MySqlBackendConfig, MySqlProxyConfig);

pub(super) fn load_config<T: for<'a> Deserialize<'a> + Serialize + Default>(
    config_path: &str,
) -> T {
    let path = Path::new(config_path);

    if path.exists() {
        // Load and return
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .expect("Failed to create config file.");

        let mut target = String::new();
        file.read_to_string(&mut target)
            .expect("failed to read config file");

        toml::from_str::<T>(target.as_str()).expect("failed to parse config file")
    } else {
        let default_config = T::default();

        let mut out = OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .expect("Failed to create config file.");

        out.write(
            toml::to_string(&default_config)
                .expect("cannot serialize config")
                .as_bytes(),
        )
        .expect("failed to write config");
        out.flush().unwrap();

        panic!(
            "Wrote default config to '{}', please edit and restart!",
            config_path
        );
    }
}
