use crate::datasets::LoadedFile;
use crate::loaders::LoadedDataset;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::Read;
use std::iter::FilterMap;
use tar::{Archive, Entries, Entry};

pub struct GzipDataset {
    file: Archive<GzDecoder<File>>,
}

impl LoadedDataset for GzipDataset {
    type IterType<'a> = FilterMap<
        Entries<'a, GzDecoder<File>>,
        fn(std::io::Result<Entry<GzDecoder<File>>>) -> Option<LoadedFile>,
    >;

    fn new(f: File) -> Self {
        let file = GzDecoder::new(f);
        let file = Archive::new(file);

        Self { file }
    }

    fn iter<'a>(&'a mut self) -> Self::IterType<'a> {
        let iter = self.file.entries().unwrap();

        iter.filter_map(|entry| {
            let mut entry = entry.unwrap();

            if entry.header().size().unwrap() == 0 {
                return None;
            }

            let file_path = entry.header().path().unwrap();
            let file_path = file_path.to_str().unwrap();

            let mut loaded = LoadedFile {
                file_path: String::from(file_path),
                file_contents: String::new(),
            };

            let read = entry.read_to_string(&mut loaded.file_contents);

            if read.is_err() {
                eprintln!(
                    "[skip] Failed to read from email: {}\n{:?}",
                    &loaded.file_path,
                    read.unwrap_err()
                );
                return None;
            }

            Some(loaded)
        })
    }
}
