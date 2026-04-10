// The `OpenApi` derive macro generates code that uses `for_each`.
#![allow(clippy::needless_for_each)]
use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::Router;
use axum::extract::State;
use axum::response::Json;
use axum::routing::get;
use serde::Serialize;
use tokio::net::TcpListener;
use utoipa::{OpenApi, ToSchema};
use utoipa_scalar::{Scalar, Servable};

use crate::clients::ClientRegistry;
use crate::config::Config;
use crate::pool::PoolManager;
use crate::topology::TopologyManager;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    config: Arc<ArcSwap<Config>>,
    pools: Arc<PoolManager>,
    topology: Arc<TopologyManager>,
    clients: Arc<ClientRegistry>,
}

// ---------------------------------------------------------------------------
// OpenAPI
// ---------------------------------------------------------------------------

#[derive(OpenApi)]
#[openapi(
    paths(health, ready, get_pools, get_servers, get_clients, get_queues),
    components(schemas(
        HealthResponse,
        HealthStatus,
        ReadyResponse,
        ClusterReadiness,
        PoolInfo,
        ClusterServers,
        ServerInfo,
        ServerRole,
        ClientInfo,
        ClientStateDto,
        WaitTargetDto,
        WaitRoleDto,
        QueueInfoDto,
    )),
    info(
        title = "Halephant Admin API",
        version = "0.1.0",
        description = "Operational endpoints for health checks and pool introspection."
    )
)]
struct ApiDoc;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

/// Health check response.
#[derive(Serialize, ToSchema)]
struct HealthResponse {
    status: HealthStatus,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum HealthStatus {
    Ok,
}

/// Readiness check response.
#[derive(Serialize, ToSchema)]
struct ReadyResponse {
    ready: bool,
    clusters: Vec<ClusterReadiness>,
}

#[derive(Serialize, ToSchema)]
struct ClusterReadiness {
    name: String,
    has_primary: bool,
}

/// Pool connection statistics.
#[derive(Serialize, ToSchema)]
struct PoolInfo {
    node: String,
    database: String,
    user: String,
    active: u32,
    idle: u32,
    resetting: u32,
}

/// Upstream server status within a cluster.
#[derive(Serialize, ToSchema)]
struct ServerInfo {
    address: String,
    role: ServerRole,
}

/// Upstream server role.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum ServerRole {
    Primary,
    Replica,
    Unreachable,
}

/// Cluster server topology.
#[derive(Serialize, ToSchema)]
struct ClusterServers {
    cluster: String,
    servers: Vec<ServerInfo>,
}

/// Point-in-time state of a single connected client.
#[derive(Serialize, ToSchema)]
struct ClientInfo {
    /// Stable client identifier within this halephant process.
    id: u64,
    /// Remote address of the client's TCP socket.
    remote: String,
    /// High-level state of the client.
    state: ClientStateDto,
    /// Seconds since the client was accepted.
    uptime_secs: f64,
    /// Seconds the client has been in its current state.
    state_since_secs: f64,
    /// Database name from the client's startup message, if known.
    database: Option<String>,
    /// PostgreSQL user from the client's startup message, if known.
    user: Option<String>,
    /// Populated only while `state` is `waiting` — identifies the
    /// specific role queue the client is blocked on.
    #[serde(skip_serializing_if = "Option::is_none")]
    waiting_for: Option<WaitTargetDto>,
}

/// Client state category as exposed by the admin API.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum ClientStateDto {
    Negotiating,
    Authenticating,
    Idle,
    InTransaction,
    Waiting,
}

impl From<crate::clients::ClientState> for ClientStateDto {
    fn from(s: crate::clients::ClientState) -> Self {
        use crate::clients::ClientState;
        match s {
            ClientState::Negotiating => Self::Negotiating,
            ClientState::Authenticating => Self::Authenticating,
            ClientState::Idle => Self::Idle,
            ClientState::InTransaction => Self::InTransaction,
            ClientState::Waiting => Self::Waiting,
        }
    }
}

/// The `(database, user, role)` queue key a client is blocked on while
/// waiting for pool capacity.
#[derive(Serialize, ToSchema)]
struct WaitTargetDto {
    database: String,
    user: String,
    role: WaitRoleDto,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
enum WaitRoleDto {
    Primary,
    Replica,
}

impl From<crate::pool::Routing> for WaitRoleDto {
    fn from(role: crate::pool::Routing) -> Self {
        use crate::pool::Routing;
        match role {
            Routing::Primary => WaitRoleDto::Primary,
            Routing::Replica => WaitRoleDto::Replica,
        }
    }
}

impl From<crate::clients::WaitTarget> for WaitTargetDto {
    fn from(t: crate::clients::WaitTarget) -> Self {
        use crate::clients::WaitRole;
        Self {
            database: t.database,
            user: t.user,
            role: match t.role {
                WaitRole::Primary => WaitRoleDto::Primary,
                WaitRole::Replica => WaitRoleDto::Replica,
            },
        }
    }
}

/// Snapshot of one wait queue: how many clients are currently blocked
/// on this `(database, user, role)` and how long the oldest has been
/// waiting. Only active (non-empty) queues are returned.
#[derive(Serialize, ToSchema)]
struct QueueInfoDto {
    database: String,
    user: String,
    role: WaitRoleDto,
    /// Number of clients currently blocked on this queue.
    depth: u32,
    /// Seconds the oldest queued waiter has been blocked.
    oldest_wait_secs: f64,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Liveness probe. Always returns 200.
#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, body = HealthResponse)),
    tag = "health"
)]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: HealthStatus::Ok,
    })
}

/// Readiness probe. Returns 200 when all clusters have a discovered primary,
/// 503 otherwise.
#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, body = ReadyResponse, description = "All clusters have a primary"),
        (status = 503, body = ReadyResponse, description = "One or more clusters lack a primary")
    ),
    tag = "health"
)]
async fn ready(State(state): State<AppState>) -> (axum::http::StatusCode, Json<ReadyResponse>) {
    let cfg = state.config.load_full();
    let clusters: Vec<ClusterReadiness> = cfg
        .cluster
        .keys()
        .map(|name| {
            let has_primary = state.topology.primary(name).is_some();
            ClusterReadiness {
                name: name.clone(),
                has_primary,
            }
        })
        .collect();

    let all_ready = clusters.iter().all(|c| c.has_primary);
    let status = if all_ready {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(ReadyResponse {
            ready: all_ready,
            clusters,
        }),
    )
}

/// Pool connection statistics per node, database, and user.
#[utoipa::path(
    get,
    path = "/admin/pools",
    responses((status = 200, body = Vec<PoolInfo>)),
    tag = "admin"
)]
async fn get_pools(State(state): State<AppState>) -> Json<Vec<PoolInfo>> {
    let stats = state
        .pools
        .pool_stats()
        .into_iter()
        .map(|(key, stats)| PoolInfo {
            node: key.node,
            database: key.database,
            user: key.user,
            active: stats.active,
            idle: stats.idle,
            resetting: stats.resetting,
        })
        .collect();
    Json(stats)
}

/// Upstream server topology per cluster.
#[utoipa::path(
    get,
    path = "/admin/servers",
    responses((status = 200, body = Vec<ClusterServers>)),
    tag = "admin"
)]
async fn get_servers(State(state): State<AppState>) -> Json<Vec<ClusterServers>> {
    let cfg = state.config.load_full();
    let result = cfg
        .cluster
        .keys()
        .map(|name| {
            let mut servers = Vec::new();
            if let Some(topo) = state.topology.get(name) {
                if let Some(ref primary) = topo.primary {
                    servers.push(ServerInfo {
                        address: primary.clone(),
                        role: ServerRole::Primary,
                    });
                }
                for replica in &topo.replicas {
                    servers.push(ServerInfo {
                        address: replica.clone(),
                        role: ServerRole::Replica,
                    });
                }
                for node in &topo.unreachable {
                    servers.push(ServerInfo {
                        address: node.clone(),
                        role: ServerRole::Unreachable,
                    });
                }
            }
            ClusterServers {
                cluster: name.clone(),
                servers,
            }
        })
        .collect();
    Json(result)
}

/// Currently connected clients with their state and identity, if known.
#[utoipa::path(
    get,
    path = "/admin/clients",
    responses((status = 200, body = Vec<ClientInfo>)),
    tag = "admin"
)]
async fn get_clients(State(state): State<AppState>) -> Json<Vec<ClientInfo>> {
    let now = std::time::Instant::now();
    let result = state
        .clients
        .snapshot()
        .into_iter()
        .map(|entry| ClientInfo {
            id: entry.id.as_u64(),
            remote: entry.remote.to_string(),
            state: entry.state.into(),
            uptime_secs: now
                .saturating_duration_since(entry.accepted_at)
                .as_secs_f64(),
            state_since_secs: now
                .saturating_duration_since(entry.state_since)
                .as_secs_f64(),
            database: entry.database,
            user: entry.user,
            waiting_for: entry.waiting_for.map(Into::into),
        })
        .collect();
    Json(result)
}

/// Active wait queues — one entry per `(database, user, role)` with
/// current depth and oldest wait duration. Empty queues are omitted so
/// the response only reflects live contention.
#[utoipa::path(
    get,
    path = "/admin/queues",
    responses((status = 200, body = Vec<QueueInfoDto>)),
    tag = "admin"
)]
async fn get_queues(State(state): State<AppState>) -> Json<Vec<QueueInfoDto>> {
    let result = state
        .pools
        .queue_stats()
        .into_iter()
        .map(|q| QueueInfoDto {
            database: q.database,
            user: q.user,
            role: WaitRoleDto::from(q.role),
            depth: q.depth,
            oldest_wait_secs: q.oldest_wait_secs,
        })
        .collect();
    Json(result)
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Build the admin API router.
pub fn router(
    config: Arc<ArcSwap<Config>>,
    pools: Arc<PoolManager>,
    topology: Arc<TopologyManager>,
    clients: Arc<ClientRegistry>,
) -> Router {
    let state = AppState {
        config,
        pools,
        topology,
        clients,
    };

    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/admin/pools", get(get_pools))
        .route("/admin/servers", get(get_servers))
        .route("/admin/clients", get(get_clients))
        .route("/admin/queues", get(get_queues))
        .merge(Scalar::with_url("/docs", ApiDoc::openapi()))
        .with_state(state)
}

/// Start the admin HTTP server on the given address.
pub async fn serve(
    addr: &str,
    config: Arc<ArcSwap<Config>>,
    pools: Arc<PoolManager>,
    topology: Arc<TopologyManager>,
    clients: Arc<ClientRegistry>,
) -> std::io::Result<()> {
    let app = router(config, pools, topology, clients);
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "admin API listening");
    axum::serve(listener, app).await?;
    Ok(())
}
