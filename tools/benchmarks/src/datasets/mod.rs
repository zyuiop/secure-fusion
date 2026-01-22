use mysql::Conn;

pub mod enron;

pub struct LoadedFile {
    pub file_path: String,
    pub file_contents: String,
}

pub trait Dataset {
    fn init_database(&mut self, _conn: &mut Conn) {}

    fn post_insert_data(&mut self, _conn: &mut Conn) {}

    fn drop_database(&mut self, _conn: &mut Conn) {}

    fn load_insert_data(&mut self, _conn: &mut Conn) {}
}
