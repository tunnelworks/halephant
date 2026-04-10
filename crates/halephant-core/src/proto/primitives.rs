use chumsky::prelude::*;

use crate::proto::parser::PgExtra;
use crate::proto::types;

/// Big-endian `i16`.
pub(in crate::proto) fn pg_int16<'src>() -> impl Parser<'src, &'src [u8], i16, PgExtra<'src>> + Clone
{
    any()
        .then(any())
        .map(|(hi, lo)| i16::from_be_bytes([hi, lo]))
}

/// Big-endian `i32`.
pub(in crate::proto) fn pg_int32<'src>() -> impl Parser<'src, &'src [u8], i32, PgExtra<'src>> + Clone
{
    any()
        .then(any())
        .then(any())
        .then(any())
        .map(|(((b0, b1), b2), b3)| i32::from_be_bytes([b0, b1, b2, b3]))
}

/// Null-terminated C string (UTF-8).
pub(in crate::proto) fn pg_cstring<'src>()
-> impl Parser<'src, &'src [u8], String, PgExtra<'src>> + Clone {
    any()
        .filter(|b| *b != 0)
        .repeated()
        .collect::<Vec<u8>>()
        .then_ignore(just(0u8))
        .try_map(|bytes, span| {
            String::from_utf8(bytes).map_err(|e| Rich::custom(span, format!("invalid UTF-8: {e}")))
        })
}

/// Length-prefixed nullable byte array (i32 length; -1 = NULL).
pub(in crate::proto) fn pg_nullable_bytes<'src>()
-> impl Parser<'src, &'src [u8], Option<Vec<u8>>, PgExtra<'src>> + Clone {
    custom(|inp| {
        let len: i32 = inp.parse(pg_int32())?;
        if len < 0 {
            Ok(None)
        } else {
            let len = len as usize;
            let mut bytes = Vec::with_capacity(len);
            for _ in 0..len {
                bytes.push(inp.parse(any())?);
            }
            Ok(Some(bytes))
        }
    })
}

/// Read an i16 count, then parse that many items with `item`.
pub(in crate::proto) fn pg_count_prefixed<'src, O: 'src>(
    item: impl Parser<'src, &'src [u8], O, PgExtra<'src>> + Clone + 'src,
) -> impl Parser<'src, &'src [u8], Vec<O>, PgExtra<'src>> + Clone {
    custom(move |inp| {
        let count = inp.parse(pg_int16())? as usize;
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(inp.parse(item.clone())?);
        }
        Ok(items)
    })
}

/// Format code (i16 → `FormatCode`).
pub(in crate::proto) fn pg_format_code<'src>()
-> impl Parser<'src, &'src [u8], types::FormatCode, PgExtra<'src>> + Clone {
    pg_int16().try_map(|raw, span| match raw {
        0 => Ok(types::FormatCode::Text),
        1 => Ok(types::FormatCode::Binary),
        _ => Err(Rich::custom(span, format!("invalid format code: {raw}"))),
    })
}

// ---------------------------------------------------------------------------
// Unit tests for chumsky primitives
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok<'a, O: std::fmt::Debug + PartialEq>(
        parser: impl Parser<'a, &'a [u8], O, PgExtra<'a>>,
        input: &'a [u8],
        expected: &O,
    ) {
        #[allow(clippy::unwrap_used)]
        let result = crate::proto::parser::run(parser, input).unwrap();
        assert_eq!(&result, expected);
    }

    #[test]
    fn int32() {
        parse_ok(pg_int32(), &42i32.to_be_bytes(), &42);
    }

    #[test]
    fn int16() {
        parse_ok(pg_int16(), &7i16.to_be_bytes(), &7);
    }

    #[test]
    fn cstring() {
        parse_ok(
            pg_cstring().then(pg_cstring()),
            b"hello\0world\0",
            &("hello".to_owned(), "world".to_owned()),
        );
    }

    #[test]
    fn cstring_empty() {
        parse_ok(pg_cstring(), b"\0", &String::new());
    }

    #[test]
    fn cstring_unterminated() {
        assert!(crate::proto::parser::run(pg_cstring(), b"hello").is_err());
    }

    #[test]
    fn count_prefixed() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3i16.to_be_bytes());
        buf.extend_from_slice(&10i32.to_be_bytes());
        buf.extend_from_slice(&20i32.to_be_bytes());
        buf.extend_from_slice(&30i32.to_be_bytes());

        parse_ok(pg_count_prefixed(pg_int32()), &buf, &vec![10, 20, 30]);
    }

    #[test]
    fn nullable_bytes_present() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&5i32.to_be_bytes());
        buf.extend_from_slice(b"hello");
        parse_ok(pg_nullable_bytes(), &buf, &Some(b"hello".to_vec()));
    }

    #[test]
    fn nullable_bytes_null() {
        parse_ok(pg_nullable_bytes(), &(-1i32).to_be_bytes(), &None);
    }

    #[test]
    fn format_code_valid() {
        parse_ok(
            pg_format_code(),
            &0i16.to_be_bytes(),
            &types::FormatCode::Text,
        );
        parse_ok(
            pg_format_code(),
            &1i16.to_be_bytes(),
            &types::FormatCode::Binary,
        );
    }

    #[test]
    fn format_code_invalid() {
        assert!(crate::proto::parser::run(pg_format_code(), &99i16.to_be_bytes()).is_err());
    }

    #[test]
    fn trailing_bytes_rejected() {
        assert!(crate::proto::parser::run(pg_int32(), &[0, 0, 0, 1, 0xFF]).is_err());
    }

    #[test]
    fn truncated_int32() {
        assert!(crate::proto::parser::run(pg_int32(), &[0, 0]).is_err());
    }
}
