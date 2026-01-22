//! Some queries contain raw binary data, which sometimes are invalid UTF-8. Try to catch this.

use crate::command_phase::char_utils::auto_escape_vec::AutoEscapeVec;
use datafusion::common::{ParamValues, ScalarValue, plan_err};
use std::borrow::Cow;
use std::str::Utf8Chunk;

#[derive(PartialEq, Eq, Debug)]
struct ParserState {
    res: String,
    params: Vec<ScalarValue>,

    current_param: Option<AutoEscapeVec>,
}

mod auto_escape_vec {
    use datafusion::common::plan_err;
    use datafusion::error::DataFusionError;

    /// A vector that handles auto removal of characters on push
    #[derive(Clone, Eq, Debug, PartialEq)]
    pub(super) struct AutoEscapeVec {
        inner: Vec<u8>,
        has_dangling_escape: bool,
    }

    #[inline]
    const fn replace_escaped(source: u8) -> u8 {
        match source as char {
            ('0') => '\0' as u8,
            ('\'') => '\'' as u8,
            ('"') => '"' as u8,
            ('b') => 8u8, /* Backspace \b */
            ('n') => '\n' as u8,
            ('r') => '\r' as u8,
            ('t') => '\t' as u8,
            ('Z') => 26u8,
            ('\\') => '\\' as u8,
            ('%') => '%' as u8,
            ('_') => '_' as u8,
            other => other as u8,
        }
    }

    impl AutoEscapeVec {
        const ESCAPE_CHAR: u8 = '\\' as u8;

        pub fn extend_unescape(&mut self, bytes: &[u8]) {
            // Remove any potential dangling escape char
            let mut previous_was_escape = self.has_dangling_escape;

            let current_vec = &mut self.inner;
            current_vec.reserve(bytes.len());

            for character in bytes.iter() {
                if previous_was_escape {
                    current_vec.push(replace_escaped(*character));
                    previous_was_escape = false;
                    continue;
                }

                if *character == Self::ESCAPE_CHAR {
                    previous_was_escape = true;
                } else {
                    current_vec.push(character.clone());
                }
            }

            self.has_dangling_escape = previous_was_escape;
        }

        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                inner: Vec::with_capacity(capacity),
                has_dangling_escape: false,
            }
        }

        pub fn has_dangling_escape(&self) -> bool {
            self.has_dangling_escape
        }
    }

    impl TryFrom<AutoEscapeVec> for Vec<u8> {
        type Error = DataFusionError;

        fn try_from(value: AutoEscapeVec) -> Result<Self, Self::Error> {
            if value.has_dangling_escape {
                plan_err!("Failed to parse query: dangling escape character")
            } else {
                Ok(value.inner)
            }
        }
    }
}

impl ParserState {
    pub fn init() -> Self {
        Self {
            params: vec![],
            current_param: None,
            res: String::new(),
        }
    }

    pub fn finish(self) -> datafusion::common::Result<(String, ParamValues)> {
        let ParserState {
            res,
            params,
            current_param,
        } = self;

        if current_param.is_some() {
            return plan_err!(
                "Failed to parse binary chunks in query: unterminated chunk {current_param:?}"
            );
        }

        let params = ParamValues::List(params.into_iter().map(|param| param.into()).collect());

        Ok((res, params))
    }

    pub fn add_chunk(&mut self, chunk: Utf8Chunk) -> datafusion::common::Result<()> {
        if self.current_param.is_none() {
            self.add_chunk_start(chunk.valid(), chunk.invalid())
        } else {
            self.add_chunk_continue(chunk.valid(), chunk.invalid())
        }
    }

    fn add_chunk_start(
        &mut self,
        valid: &str,
        invalid_bytes: &[u8],
    ) -> datafusion::common::Result<()> {
        if invalid_bytes.is_empty() {
            // Ensure we still have to find an invalid value
            self.res.push_str(valid);
            return Ok(());
        }

        // 1. Find a `'` in the valid string
        let Some((position, _quote)) = valid
            .char_indices()
            .filter(|(_, chr)| *chr == '\'')
            .next_back()
        else {
            return plan_err!(
                "Failed to parse binary chunks in query: Failed to locate binary string start in `{valid}`"
            );
        };

        // 2. Append the sub-string to the result and insert a placeholder
        self.res.push_str(&valid[0..position]);
        self.res.push('?');

        // 3. Make a builder with the valid rest + the invalid part
        let valid_bytes = &valid.as_bytes()[position + 1..];

        let mut output = AutoEscapeVec::with_capacity(valid_bytes.len() + invalid_bytes.len());
        output.extend_unescape(valid_bytes);
        output.extend_unescape(invalid_bytes);

        self.current_param = Some(output);
        Ok(())
    }

    fn add_chunk_continue(
        &mut self,
        valid: &str,
        invalid: &[u8],
    ) -> datafusion::common::Result<()> {
        let Some(current_vec) = self.current_param.as_mut() else {
            unreachable!()
        };

        // 1. Find a `'` in the valid string
        let has_single_quote = valid.char_indices().filter(|(_, chr)| *chr == '\'').next();

        if has_single_quote.is_none() {
            // Just a continuation, consume entirely
            current_vec.extend_unescape(valid.as_bytes());
            current_vec.extend_unescape(invalid);

            return Ok(());
        }

        let Some((position, _quote)) = has_single_quote else {
            unreachable!()
        };

        // 2. Verify that the quote is not escaped
        if position > 0
            && valid.is_char_boundary(position - 1)
            && &valid[position - 1..position] == "\\"
        {
            // Escaped quote! Consume valid up to the escape, insert the quote, and retry!
            let (parsed, to_parse) = valid.split_at(position + 1);
            current_vec.extend_unescape(parsed.as_bytes());

            return self.add_chunk_continue(to_parse, invalid);
        } else if position == 0 && current_vec.has_dangling_escape() {
            // Escaped quote (from a previous dangling escape character)
            // Push the current character and continue
            current_vec.extend_unescape(&['\'' as u8]);
            return self.add_chunk_continue(&valid[1..], invalid);
        }

        // Append everything until the quote to the buffer, and push the value
        current_vec.extend_unescape(&valid.as_bytes()[0..position]);
        let current_vec = self.current_param.take().unwrap();
        self.params
            .push(ScalarValue::Binary(Some(current_vec.try_into()?)));

        // Append the rest, and detect potentially more invalid data
        self.add_chunk_start(&valid[position + 1..], invalid)
    }
}

pub fn extract_invalid_bytes<'a>(
    bytes: &'a [u8],
) -> datafusion::common::Result<(Cow<'a, str>, Option<ParamValues>)> {
    // This is mostly taken from String::from_utf_lossy
    let mut chunks_iter = bytes.utf8_chunks();
    let mut chunk = if let Some(chunk) = chunks_iter.next() {
        if chunk.invalid().is_empty() {
            // Short case: string is valid
            return Ok((Cow::Borrowed(chunk.valid()), None));
        }
        chunk
    } else {
        // Short case: string is empty
        return Ok((Cow::Borrowed(""), None));
    };

    let mut state = ParserState::init();

    loop {
        let _ = state.add_chunk(chunk)?;

        let Some(next) = chunks_iter.next() else {
            break;
        };
        chunk = next;
    }

    let (string, values) = state.finish()?;
    Ok((Cow::Owned(string), Some(values)))
}

#[cfg(test)]
mod test {
    use crate::command_phase::char_utils::extract_invalid_bytes;
    use datafusion::common::{ParamValues, ScalarValue};
    use log::LevelFilter;

    const EQ: u8 = '=' as u8;
    const SP: u8 = ' ' as u8;
    const QUOT: u8 = '\'' as u8;
    const COMMA: u8 = ',' as u8;

    #[test]
    fn test_rewrite_known_bad_query() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Debug)
            .try_init();

        let bad_query = [
            // INSERT ...
            105u8, 110, 115, 101, 114, 116, SP, 105, 110, 116, 111, SP, 80, 97, 112, 101, 114, 83,
            116, 111, 114, 97, 103, 101, SP, 115, 101, 116, SP, // paperId=
            112, 97, 112, 101, 114, 73, 100, EQ, 50, 49, 54, 49, COMMA, SP, // sha1=
            115, 104, 97, 49, EQ, QUOT, 115, 104, 97, 50, 45, 118, 64, 232, 251, 79, 7, 231, 105,
            124, 92, 39, 52, 1, 152, 144, 170, 55, 92, 48, 233, 118, 32, 200, 126, 131, 80, 219,
            92, 39, 174, 186, 85, 131, 123, 228, QUOT, COMMA, SP, // SHA1
            // timestamp=
            116, 105, 109, 101, 115, 116, 97, 109, 112, EQ, 49, 55, 54, 56, 50, 49, 52, 54, 49, 50,
            COMMA, SP, // size=
            115, 105, 122, 101, EQ, 50, 53, 50, 55, 49, 51, COMMA, SP, // mimetype=
            109, 105, 109, 101, 116, 121, 112, 101, EQ, QUOT, 97, 112, 112, 108, 105, 99, 97, 116,
            105, 111, 110, 47, 112, 100, 102, QUOT, COMMA, SP, // documentType=
            100, 111, 99, 117, 109, 101, 110, 116, 84, 121, 112, 101, EQ, 48, COMMA, SP,
            // inactive=
            105, 110, 97, 99, 116, 105, 118, 101, EQ, 48, COMMA, SP, // crc32=
            99, 114, 99, 51, 50, EQ, QUOT, 92, 48, 220, 192, 52, QUOT, COMMA, SP, // CRC SP
            // filename=
            102, 105, 108, 101, 110, 97, 109, 101, EQ, QUOT, 116, 109, 112, 68, 48, 66, 55, 46, 112,
            100, 102, QUOT,
        ];

        let (q, params) = extract_invalid_bytes(&bad_query[..]).unwrap();

        assert_eq!(
            q,
            "insert into PaperStorage set paperId=2161, sha1=?, timestamp=1768214612, size=252713, mimetype='application/pdf', documentType=0, inactive=0, crc32=?, filename='tmpD0B7.pdf'"
        );

        let ParamValues::List(params) = params.unwrap() else {
            panic!("invalid params response")
        };

        assert_eq!(
            params[0].value,
            ScalarValue::Binary(Some(vec![
                115u8, 104, 97, 50, 45, 118, 64, 232, 251, 79, 7, 231, 105, 124, 39, 52, 1, 152,
                144, 170, 55, 0, 233, 118, 32, 200, 126, 131, 80, 219, 39, 174, 186, 85, 131, 123,
                228
            ]))
        );
        assert_eq!(
            params[1].value,
            ScalarValue::Binary(Some(vec![0u8, 220, 192, 52]))
        );
    }

    #[test]
    fn test_rewrite_with_escaped_quote() {
        let _ = env_logger::builder()
            .filter_level(LevelFilter::Debug)
            .try_init();

        let bad_query = [
            // INSERT ...
            105u8, 110, 115, 101, 114, 116, SP, 105, 110, 116, 111, SP, 80, 97, 112, 101, 114, 83,
            116, 111, 114, 97, 103, 101, SP, 115, 101, 116, SP, // sha1=
            115, 104, 97, 49, EQ, QUOT, 84, 122, 227, 178, 92, QUOT, // Escaped quote
            153, 119, 194, 122, QUOT,
        ];

        let (q, params) = extract_invalid_bytes(&bad_query[..]).unwrap();
        assert_eq!(q, "insert into PaperStorage set sha1=?");

        let ParamValues::List(params) = params.unwrap() else {
            panic!("invalid params response")
        };

        assert_eq!(
            params[0].value,
            ScalarValue::Binary(Some(vec![84u8, 122, 227, 178, QUOT, 153, 119, 194, 122,]))
        );
    }
}
