use bytes::{BufMut, BytesMut};

/// Write a type-byte + length-prefixed message. The length field includes its
/// own 4 bytes but not the type byte, matching PostgreSQL wire format.
pub(in crate::proto) fn encode_message(
    dst: &mut BytesMut,
    msg_type: u8,
    f: impl FnOnce(&mut BytesMut),
) {
    dst.put_u8(msg_type);
    let length_pos = dst.len();
    dst.put_i32(0); // placeholder
    f(dst);
    let length = (dst.len() - length_pos) as i32;
    dst[length_pos..length_pos + 4].copy_from_slice(&length.to_be_bytes());
}

/// Write a length-prefixed initial message (no type byte). The length field
/// includes its own 4 bytes.
pub(in crate::proto) fn encode_initial(dst: &mut BytesMut, f: impl FnOnce(&mut BytesMut)) {
    let length_pos = dst.len();
    dst.put_i32(0); // placeholder
    f(dst);
    let length = (dst.len() - length_pos) as i32;
    dst[length_pos..length_pos + 4].copy_from_slice(&length.to_be_bytes());
}

/// Write a null-terminated C string.
pub(in crate::proto) fn encode_cstring(dst: &mut BytesMut, s: &str) {
    dst.extend_from_slice(s.as_bytes());
    dst.put_u8(0);
}

/// Write an i16 count followed by items produced by `f`.
pub(in crate::proto) fn encode_count_prefixed<T>(
    dst: &mut BytesMut,
    items: &[T],
    mut f: impl FnMut(&mut BytesMut, &T),
) {
    dst.put_i16(items.len() as i16);
    for item in items {
        f(dst, item);
    }
}
