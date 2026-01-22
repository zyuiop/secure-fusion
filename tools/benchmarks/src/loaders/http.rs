use crate::loaders::{DatasetLoader, LoadedDataset};
use sha2::{Digest, Sha256};
use std::io::{Seek, SeekFrom};
use std::marker::PhantomData;
use std::{fs, io};

pub struct HttpLoader<T: LoadedDataset> {
    url: &'static str,
    target_name: &'static str,
    hash: Option<[u8; 32]>,

    _output_type: PhantomData<T>,
}

impl<T: LoadedDataset> HttpLoader<T> {
    pub const fn new(target_name: &'static str, url: &'static str, hash: Option<[u8; 32]>) -> Self {
        Self {
            url,
            target_name,
            hash,
            _output_type: PhantomData::<T> {},
        }
    }

    pub fn skip_hash_check(mut self) -> Self {
        self.hash = None;
        self
    }
}

impl<T: LoadedDataset> DatasetLoader<T> for HttpLoader<T> {
    fn load(&self) -> T {
        let path = format!("./datasets/{}", self.target_name);
        let path = std::path::Path::new(&path);

        if fs::exists(path).expect("Critical: failed to check if dataset file exists") {
            let Some(hash) = self.hash else {
                println!("⏩ Dataset found, returning without checking hash...");
                let f = fs::File::open(path).expect("Critical: failed to open file");

                return T::new(f);
            };

            println!("⌛ Dataset found, checking hash...");
            let mut sha = Sha256::default();
            let mut file = fs::File::open(path).expect("Critical: failed to open file");
            io::copy(&mut file, &mut sha).expect("Critical: failed to read file");
            file.seek(SeekFrom::Start(0))
                .expect("Critical: failed to read file");

            let digest = sha.finalize();

            if digest[..] == hash[..] {
                println!("✅ Dataset found and hash matched!");
                return T::new(file);
            } else {
                println!("❌ Dataset hash not matched! Deleting and downloading...");
            }

            fs::remove_file(path).expect("Critical: failed to remove file.");
        }

        let parent_dir = path.parent().unwrap();
        if !fs::exists(parent_dir).expect("Critical: failed to check if dataset directory exists") {
            fs::create_dir_all(parent_dir).expect("Critical: failed to create parent dir");
        }

        let mut body = reqwest::blocking::get(self.url).unwrap();
        let mut file = fs::File::create(path).expect("Critical: failed to open file");

        println!("💽 Downloading enron dataset to {path:?}...");
        io::copy(&mut body, &mut file).expect("Critical: failed to copy file");
        println!("💽 Download finished, trying to load!");

        self.load()
    }
}
