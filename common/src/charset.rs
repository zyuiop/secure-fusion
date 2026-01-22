use strum_macros::EnumString;

#[derive(Debug, Copy, Clone, PartialEq, EnumString)]
pub enum Charset {
    /// UTF-8, restricted to 3 bytes (extended charset unavailable), case-insensitive comparisons
    Utf8RestrictedCaseInsensitive,
    /// UTF-8, restricted to 3 bytes (extended charset unavailable), case-sensitive comparisons
    Utf8RestrictedCaseSensitive,
    /// UTF-8 with extended characters available, case-insensitive comparisons
    Utf8ExtendedCaseInsensitive,
    /// UTF-8 with extended characters available, case-sensitive comparisons
    Utf8ExtendedCaseSensitive,
}
