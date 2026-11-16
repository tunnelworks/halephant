//! Imperative byte cursor for decoding wire-protocol payloads.
//!
//! PostgreSQL messages are fixed-shape: the type byte determines exactly
//! which fields follow, there is no backtracking, and every malformed
//! payload is a hard error rather than an alternative to try. That
//! profile calls for a hand-written cursor, not a combinator library —
//! the same trade-off `sql/parser.rs` documents for the statement
//! walker. The cursor reads big-endian integers, C strings, and count-
//! or length-prefixed fields, and rejects hostile counts and lengths
//! against the remaining input before any allocation happens
//! (robustness review H1).

use crate::errors::ProtocolError;
use crate::proto::types;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Runs `decode` over `input`, requiring that every byte is consumed.
///
/// The caller hands the cursor exactly one message payload, so leftover
/// bytes after a fully decoded value mean the payload and the decoder
/// disagree about the message shape — a framing error, not padding.
///
/// The cursor's lifetime is tied to `input` (not higher-ranked) so that
/// method paths such as `Cursor::read_i32` are accepted as `decode`.
pub(in crate::proto) fn run<'a, O>(
    input: &'a [u8],
    decode: impl FnOnce(&mut Cursor<'a>) -> Result<O, ProtocolError>,
) -> Result<O, ProtocolError> {
    let mut cur = Cursor::new(input);
    let value = decode(&mut cur)?;
    cur.finish()?;
    Ok(value)
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

/// Linear reader over a single message payload.
///
/// Every read is bounds-checked: running past the end of the input
/// returns a [`ProtocolError`] instead of panicking, and counts or
/// lengths taken from the wire are validated against the remaining
/// input before any reservation. Decoding is a single forward pass with
/// no backtracking.
pub(in crate::proto) struct Cursor<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(in crate::proto) fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }

    /// Byte offset of the next unread byte, for error positions.
    pub(in crate::proto) fn position(&self) -> usize {
        self.pos
    }

    /// Number of unread bytes.
    fn remaining(&self) -> usize {
        self.input.len() - self.pos
    }

    /// Builds the error for a read that runs past the end of the input.
    /// The reported position is the end of the input — the offset at
    /// which more data was expected.
    fn end_of_input(&self, needed: usize) -> ProtocolError {
        ProtocolError::InvalidValue {
            position: self.input.len(),
            message: format!(
                "unexpected end of input: need {needed} bytes, have {remaining}",
                remaining = self.remaining(),
            ),
        }
    }

    /// Consumes exactly `len` bytes and returns them as a slice of the
    /// original input.
    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        if len > self.remaining() {
            return Err(self.end_of_input(len));
        }
        let start = self.pos;
        self.pos += len;
        Ok(&self.input[start..self.pos])
    }

    /// Reads a single byte.
    pub(in crate::proto) fn read_u8(&mut self) -> Result<u8, ProtocolError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    /// Reads a big-endian `i16`.
    pub(in crate::proto) fn read_i16(&mut self) -> Result<i16, ProtocolError> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    /// Reads a big-endian `i32`.
    pub(in crate::proto) fn read_i32(&mut self) -> Result<i32, ProtocolError> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Reads a null-terminated C string (UTF-8), consuming the
    /// terminator byte.
    pub(in crate::proto) fn read_cstring(&mut self) -> Result<String, ProtocolError> {
        let start = self.pos;
        let Some(len) = self.input[self.pos..].iter().position(|&b| b == 0) else {
            return Err(ProtocolError::InvalidValue {
                position: self.input.len(),
                message: "unterminated C string".to_owned(),
            });
        };
        // Validate before copying so invalid UTF-8 never allocates.
        let value = std::str::from_utf8(&self.input[start..start + len]).map_err(|e| {
            ProtocolError::InvalidValue {
                position: start,
                message: format!("invalid UTF-8: {e}"),
            }
        })?;
        self.pos = start + len + 1;
        Ok(value.to_owned())
    }

    /// Reads a length-prefixed nullable byte array (`i32` length).
    ///
    /// A negative length denotes NULL. The wire value for NULL is `-1`,
    /// but any negative length is accepted, matching the previous
    /// decoder.
    pub(in crate::proto) fn read_nullable_bytes(
        &mut self,
    ) -> Result<Option<Vec<u8>>, ProtocolError> {
        let start = self.pos;
        let len = self.read_i32()?;
        if len < 0 {
            return Ok(None);
        }
        // The length comes straight off the wire, so check it against
        // the remaining input before reserving: a tiny hostile frame
        // must not trigger a multi-gigabyte allocation.
        let len = len as usize;
        let remaining = self.remaining();
        if len > remaining {
            return Err(ProtocolError::InvalidValue {
                position: start,
                message: format!(
                    "declared length {len} exceeds remaining input ({remaining} bytes)"
                ),
            });
        }
        Ok(Some(self.read_bytes(len)?.to_vec()))
    }

    /// Reads an `i16` item count, then decodes that many items with
    /// `item`.
    pub(in crate::proto) fn read_count_prefixed<O>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> Result<O, ProtocolError>,
    ) -> Result<Vec<O>, ProtocolError> {
        let start = self.pos;
        // The count is a signed wire value: reject negatives before the
        // cast so `-1` cannot become `usize::MAX` and panic
        // `Vec::with_capacity`.
        let raw = self.read_i16()?;
        if raw < 0 {
            return Err(ProtocolError::InvalidValue {
                position: start,
                message: format!("negative item count: {raw}"),
            });
        }
        // Every item consumes at least one input byte, so a count
        // beyond the remaining input is malformed; rejecting it here
        // bounds the reservation by the bytes actually received.
        let count = raw as usize;
        let remaining = self.remaining();
        if count > remaining {
            return Err(ProtocolError::InvalidValue {
                position: start,
                message: format!(
                    "declared count {count} exceeds remaining input ({remaining} bytes)"
                ),
            });
        }
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(item(self)?);
        }
        Ok(items)
    }

    /// Reads an `i16` format code: `0` for text, `1` for binary.
    pub(in crate::proto) fn read_format_code(
        &mut self,
    ) -> Result<types::FormatCode, ProtocolError> {
        let start = self.pos;
        let raw = self.read_i16()?;
        match raw {
            0 => Ok(types::FormatCode::Text),
            1 => Ok(types::FormatCode::Binary),
            _ => Err(ProtocolError::InvalidValue {
                position: start,
                message: format!("invalid format code: {raw}"),
            }),
        }
    }

    /// Consumes and returns all remaining bytes, possibly none.
    pub(in crate::proto) fn take_rest(&mut self) -> &'a [u8] {
        let rest = &self.input[self.pos..];
        self.pos = self.input.len();
        rest
    }

    /// Requires that the input is fully consumed.
    fn finish(&self) -> Result<(), ProtocolError> {
        let remaining = self.remaining();
        if remaining > 0 {
            return Err(ProtocolError::InvalidValue {
                position: self.pos,
                message: format!("expected end of input ({remaining} trailing bytes)"),
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ---- Integers --------------------------------------------------------

    #[test]
    fn read_u8_single_byte() {
        assert_eq!(run(&[7], Cursor::read_u8).unwrap(), 7);
    }

    #[test]
    fn read_u8_empty_input() {
        let err = run(&[], Cursor::read_u8).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidValue { .. }));
    }

    #[test]
    fn read_i16_big_endian() {
        assert_eq!(run(&7i16.to_be_bytes(), Cursor::read_i16).unwrap(), 7);
        assert_eq!(run(&(-2i16).to_be_bytes(), Cursor::read_i16).unwrap(), -2);
    }

    #[test]
    fn read_i16_truncated() {
        let err = run(&[0], Cursor::read_i16).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidValue { .. }));
    }

    #[test]
    fn read_i32_big_endian() {
        assert_eq!(run(&42i32.to_be_bytes(), Cursor::read_i32).unwrap(), 42);
        assert_eq!(run(&(-1i32).to_be_bytes(), Cursor::read_i32).unwrap(), -1);
    }

    #[test]
    fn read_i32_truncated() {
        let err = run(&[0, 0], Cursor::read_i32).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidValue { .. }));
    }

    // ---- C strings -------------------------------------------------------

    #[test]
    fn read_cstring_sequence() {
        let result = run(b"hello\0world\0", |cur| {
            Ok((cur.read_cstring()?, cur.read_cstring()?))
        })
        .unwrap();
        assert_eq!(result, ("hello".to_owned(), "world".to_owned()));
    }

    #[test]
    fn read_cstring_empty() {
        assert_eq!(run(b"\0", Cursor::read_cstring).unwrap(), String::new());
    }

    #[test]
    fn read_cstring_unterminated() {
        let err = run(b"hello", Cursor::read_cstring).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidValue { .. }));
    }

    #[test]
    fn read_cstring_invalid_utf8() {
        let err = run(&[0xFF, 0xFE, 0x00], Cursor::read_cstring).unwrap_err();
        assert!(
            matches!(err, ProtocolError::InvalidValue { .. })
                && err.to_string().contains("invalid UTF-8"),
            "expected an invalid-UTF-8 error, got: {err}"
        );
    }

    // ---- Count-prefixed fields -------------------------------------------

    #[test]
    fn read_count_prefixed_items() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3i16.to_be_bytes());
        buf.extend_from_slice(&10i32.to_be_bytes());
        buf.extend_from_slice(&20i32.to_be_bytes());
        buf.extend_from_slice(&30i32.to_be_bytes());

        let items = run(&buf, |cur| cur.read_count_prefixed(Cursor::read_i32)).unwrap();
        assert_eq!(items, vec![10, 20, 30]);
    }

    #[test]
    fn read_count_prefixed_negative_count() {
        // A count of 0xFFFF (-1 as i16) must fail before the cast can
        // become `usize::MAX` and panic `Vec::with_capacity`.
        let err = run(&[0xFF, 0xFF], |cur| {
            cur.read_count_prefixed(Cursor::read_i32)
        })
        .unwrap_err();
        assert!(
            matches!(err, ProtocolError::InvalidValue { .. })
                && err.to_string().contains("negative item count"),
            "expected a negative-count guard error, got: {err}"
        );
    }

    #[test]
    fn read_count_prefixed_count_exceeds_remaining() {
        let buf = i16::MAX.to_be_bytes();
        let err = run(&buf, |cur| cur.read_count_prefixed(Cursor::read_i32)).unwrap_err();
        assert!(
            matches!(err, ProtocolError::InvalidValue { .. })
                && err.to_string().contains("exceeds"),
            "expected a declared-count guard error, got: {err}"
        );
    }

    #[test]
    fn read_count_prefixed_guard_error_position_is_count_start() {
        // The guard error points at the count field, not at the end of
        // the input, so logs identify the hostile field directly.
        let err = run(&[0x00, 0xFF, 0xFF], |cur| {
            cur.read_u8()?;
            cur.read_count_prefixed(Cursor::read_i32)
        })
        .unwrap_err();
        assert!(
            matches!(err, ProtocolError::InvalidValue { position: 1, .. }),
            "expected the error at byte 1, got: {err}"
        );
    }

    // ---- Length-prefixed nullable bytes ----------------------------------

    #[test]
    fn read_nullable_bytes_present() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&5i32.to_be_bytes());
        buf.extend_from_slice(b"hello");
        let value = run(&buf, Cursor::read_nullable_bytes).unwrap();
        assert_eq!(value, Some(b"hello".to_vec()));
    }

    #[test]
    fn read_nullable_bytes_null() {
        let value = run(&(-1i32).to_be_bytes(), Cursor::read_nullable_bytes).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn read_nullable_bytes_any_negative_is_null() {
        // The previous decoder treated every negative length as NULL,
        // not just the documented `-1`; that behavior is preserved.
        let value = run(&(-5i32).to_be_bytes(), Cursor::read_nullable_bytes).unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn read_nullable_bytes_length_exceeds_remaining() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&i32::MAX.to_be_bytes());
        buf.extend_from_slice(b"hi");
        let err = run(&buf, Cursor::read_nullable_bytes).unwrap_err();
        assert!(
            matches!(err, ProtocolError::InvalidValue { .. })
                && err.to_string().contains("exceeds"),
            "expected a declared-length guard error, got: {err}"
        );
    }

    #[test]
    fn read_nullable_bytes_truncated_value() {
        // Declares five bytes but delivers three: the declared length
        // fails the remaining-input check before any allocation.
        let mut buf = Vec::new();
        buf.extend_from_slice(&5i32.to_be_bytes());
        buf.extend_from_slice(b"abc");
        let err = run(&buf, Cursor::read_nullable_bytes).unwrap_err();
        assert!(
            matches!(err, ProtocolError::InvalidValue { .. })
                && err.to_string().contains("exceeds"),
            "expected a declared-length guard error, got: {err}"
        );
    }

    // ---- Format codes ----------------------------------------------------

    #[test]
    fn read_format_code_valid() {
        assert_eq!(
            run(&0i16.to_be_bytes(), Cursor::read_format_code).unwrap(),
            types::FormatCode::Text
        );
        assert_eq!(
            run(&1i16.to_be_bytes(), Cursor::read_format_code).unwrap(),
            types::FormatCode::Binary
        );
    }

    #[test]
    fn read_format_code_invalid() {
        let err = run(&99i16.to_be_bytes(), Cursor::read_format_code).unwrap_err();
        assert!(
            matches!(err, ProtocolError::InvalidValue { .. })
                && err.to_string().contains("invalid format code"),
            "expected a format-code error, got: {err}"
        );
    }

    // ---- Remaining input -------------------------------------------------

    #[test]
    fn take_rest_returns_remaining() {
        let rest = run(&[0, 1, b'x'], |cur| {
            cur.read_i16()?;
            Ok(cur.take_rest().to_vec())
        })
        .unwrap();
        assert_eq!(rest, b"x".to_vec());
    }

    #[test]
    fn take_rest_empty() {
        let rest = run(b"", |cur| Ok(cur.take_rest().to_vec())).unwrap();
        assert_eq!(rest, Vec::<u8>::new());
    }

    // ---- Exhaustion ------------------------------------------------------

    #[test]
    fn trailing_bytes_rejected() {
        let err = run(&[0, 0, 0, 1, 0xFF], Cursor::read_i32).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidValue { .. }));
    }

    #[test]
    fn exact_consumption_succeeds() {
        assert_eq!(run(&1i32.to_be_bytes(), Cursor::read_i32).unwrap(), 1);
    }
}
