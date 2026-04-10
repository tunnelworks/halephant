/// Check if a TCP socket is still alive using a non-blocking peek.
///
/// Returns:
/// - `true` if the connection appears healthy
/// - `false` if it's dead (EOF, error, or keepalive timeout).
pub fn is_alive(stream: &tokio::net::TcpStream) -> bool {
    match stream.try_io(tokio::io::Interest::READABLE, || {
        let sock = socket2::SockRef::from(stream);
        let mut buf = [std::mem::MaybeUninit::uninit()];
        sock.peek(&mut buf)
    }) {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => true, // no data — alive
        Ok(0) | Err(_) => false,                                      // EOF or error — dead
        Ok(_) => true,                                                // data pending — alive
    }
}
