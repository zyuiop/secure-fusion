use crate::datasets::LoadedFile;
use std::fs::File;

#[cfg(feature = "email")]
pub mod emails;
pub mod gzip;
pub mod http;

pub trait LoadedDataset {
    type IterType<'a>: Iterator<Item = LoadedFile>
    where
        Self: 'a;

    fn new(f: File) -> Self;

    fn iter<'a>(&'a mut self) -> Self::IterType<'a>;
}

pub trait DatasetLoader<T: LoadedDataset> {
    fn load(&self) -> T;
}
