//! Post-authentication startup frames sent to the downstream client
//! once `Authenticator::authenticate` succeeds and the pool has
//! produced a server connection.

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::connections::server::ServerConn;
use crate::proto::backend::BackendMessage;
use crate::proto::codec::FrontendCodec;
use crate::proto::types::TransactionStatus;

/// Send post-authentication startup messages to the client: every
/// `ParameterStatus` the upstream reported during its own startup,
/// followed by `BackendKeyData` and `ReadyForQuery(Idle)`.
///
/// This is the last step of the `proxy.setup` span — after this
/// returns, the client is fully connected and may send queries.
pub async fn send_post_auth_startup(
    client: &mut Framed<TcpStream, FrontendCodec>,
    server: &ServerConn,
) -> anyhow::Result<()> {
    for (name, value) in &server.params {
        client
            .feed(BackendMessage::ParameterStatus {
                name: name.clone(),
                value: value.clone(),
            })
            .await?;
    }
    client
        .feed(BackendMessage::BackendKeyData {
            process_id: server.backend_key.0,
            secret_key: server.backend_key.1,
        })
        .await?;
    client
        .send(BackendMessage::ReadyForQuery(TransactionStatus::Idle))
        .await?;
    Ok(())
}
