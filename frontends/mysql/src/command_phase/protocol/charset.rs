use common::charset::Charset;
use mysql_common::collations::CollationId;

#[repr(transparent)]
pub struct CharsetOps(pub Charset);

impl From<CharsetOps> for CollationId {
    fn from(value: CharsetOps) -> Self {
        match value.0 {
            Charset::Utf8RestrictedCaseInsensitive => CollationId::UTF8MB3_UNICODE_CI,
            Charset::Utf8RestrictedCaseSensitive => CollationId::UTF8MB3_BIN,
            Charset::Utf8ExtendedCaseInsensitive => CollationId::UTF8MB4_UNICODE_CI,
            Charset::Utf8ExtendedCaseSensitive => CollationId::UTF8MB4_BIN,
        }
    }
}
