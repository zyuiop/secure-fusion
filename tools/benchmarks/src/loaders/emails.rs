use crate::datasets::LoadedFile;
use mail_parser::MessageParser;

pub struct ParsedEmailHeader {
    pub message_id: Option<String>,
    pub date: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub subject: Option<String>,
}

pub struct LoadedEmail {
    pub file_path: String,
    pub header: ParsedEmailHeader,
    pub body: String,
}

pub fn parse_email(file: LoadedFile) -> LoadedEmail {
    let email = MessageParser::default()
        .parse(&file.file_contents)
        .expect(format!("Failed to parse email: {}", file.file_path).as_str());
    let header = ParsedEmailHeader {
        message_id: email.header_raw("Message-ID").map(|s| s.trim().to_string()),
        date: email.header_raw("Date").map(|s| s.trim().to_string()),
        from: email.header_raw("From").map(|s| s.trim().to_string()),
        to: email.header_raw("To").map(|s| s.trim().to_string()),
        subject: email.header_raw("Subject").map(|s| s.trim().to_string()),
    };

    let body = email.body_text(0).unwrap().to_string();

    LoadedEmail {
        file_path: file.file_path,
        header,
        body,
    }
}
