use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::trace;

use crate::connections::server::ServerConn;
use crate::proto::codec::FrontendCodec;
use crate::proto::frontend::FrontendMessage;

/// Forward messages between client and server for the lifetime of the session.
///
/// `Terminate` from the client is intercepted so the server connection can be
/// returned to the pool rather than closed.
pub async fn forward(
    client: &mut Framed<TcpStream, FrontendCodec>,
    server: &mut ServerConn,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            msg = client.next() => match msg.transpose()? {
                Some(FrontendMessage::Terminate) => {
                    trace!("client sent Terminate");
                    return Ok(());
                }
                Some(msg) => {
                    trace!(?msg, "client -> server");
                    server.framed.send(msg).await?;
                }
                None => return Ok(()),
            },
            msg = server.framed.next() => match msg.transpose()? {
                Some(msg) => {
                    trace!(?msg, "server -> client");
                    client.send(msg).await?;
                }
                None => anyhow::bail!("upstream server closed unexpectedly"),
            },
        }
    }
}
