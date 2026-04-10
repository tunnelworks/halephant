#![cfg_attr(test, allow(clippy::unwrap_used))]
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use clap::Parser;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{error, info, warn};
use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use halephant_core::auth::Authenticator;
use halephant_core::clients;
use halephant_core::config::Config;
use halephant_core::listener::ListenerManager;
use halephant_core::pool::PoolManager;
use halephant_core::proxy;
use halephant_core::topology::TopologyManager;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "halephant",
    version,
    about = "PostgreSQL connection pooler and proxy"
)]
struct Cli {
    /// Path to configuration file.
    #[arg(short, long, default_value = "halephant.toml")]
    config: PathBuf,

    /// Validate configuration and exit.
    #[arg(long)]
    check: bool,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = Config::load(&cli.config)
        .with_context(|| format!("failed to load config from {}", cli.config.display()))?;

    if cli.check {
        #[allow(clippy::print_stdout)]
        {
            println!("configuration is valid");
        }
        return Ok(());
    }

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| format!("halephant={}", config.logging.level).into());
    let fmt_layer = tracing_subscriber::fmt::layer().with_filter(env_filter);

    // Shared target filter applied to every OTel exporter layer. Inherits
    // the default level from `logging.level` so operators control OTel
    // volume from the same knob as stdout, and silences noisy upstream
    // dependencies to prevent the OTel exporter (which uses tonic/h2/
    // hyper internally) from logging its own shipping activity back into
    // itself.
    let otel_default_level: tracing::Level =
        config.logging.level.parse().unwrap_or(tracing::Level::INFO);
    let otel_target_filter = || {
        tracing_subscriber::filter::Targets::new()
            .with_default(otel_default_level)
            .with_target("opentelemetry", tracing::level_filters::LevelFilter::OFF)
            .with_target("tonic", tracing::level_filters::LevelFilter::OFF)
            .with_target("h2", tracing::level_filters::LevelFilter::OFF)
            .with_target("hyper", tracing::level_filters::LevelFilter::OFF)
    };

    let mut trace_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider> = None;
    let mut meter_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider> = None;
    let mut logger_provider: Option<opentelemetry_sdk::logs::SdkLoggerProvider> = None;
    let mut otel_trace_layer = None;
    let mut otel_log_layer = None;
    if let Some(ref endpoint) = config.otel.endpoint {
        let resource = opentelemetry_sdk::Resource::builder()
            .with_service_name(config.otel.service_name.clone())
            .build();

        // Traces.
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .context("failed to build OTLP span exporter")?;
        let tp = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_resource(resource.clone())
            .build();
        let tracer = tp.tracer("halephant");
        trace_provider = Some(tp);

        // Metrics.
        let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .context("failed to build OTLP metric exporter")?;
        let metric_reader = opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter)
            .with_interval(std::time::Duration::from_secs(15))
            .build();
        let mp = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(metric_reader)
            .with_resource(resource.clone())
            .build();
        opentelemetry::global::set_meter_provider(mp.clone());
        meter_provider = Some(mp);

        // Logs. Every tracing event becomes an OTel `LogRecord` with a
        // populated `SeverityText` / `SeverityNumber`, so HyperDX and other
        // OTel-native log backends show proper levels instead of
        // "undefined". The bridge attaches the current trace context to
        // each record, so jumping from a trace to its logs works.
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .context("failed to build OTLP log exporter")?;
        let lp = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
            .with_batch_exporter(log_exporter)
            .with_resource(resource)
            .build();
        otel_log_layer = Some(
            opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&lp)
                .with_filter(otel_target_filter()),
        );
        logger_provider = Some(lp);

        otel_trace_layer = Some(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_level(true)
                .with_filter(otel_target_filter()),
        );
    }

    tracing_subscriber::Registry::default()
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .with(fmt_layer)
        .init();

    // Wrap the loaded config in an `ArcSwap` so a future SIGHUP reload
    // can swap a new snapshot in atomically without touching any of
    // the long-lived handles (pools, topology, admin, listeners, …).
    let config = Arc::new(ArcSwap::from_pointee(config));

    let pgpass = {
        use std::path::PathBuf;
        let cfg = config.load();
        let path = cfg.server.pgpass.clone().unwrap_or_else(|| {
            std::env::var("PGPASSFILE").map_or_else(
                |_| {
                    std::env::var("HOME")
                        .map(PathBuf::from)
                        .unwrap_or_default()
                        .join(".pgpass")
                },
                PathBuf::from,
            )
        });
        Arc::new(halephant_core::auth::pgpass::Pgpass::load(&path))
    };

    let topology = Arc::new(TopologyManager::new(
        Arc::clone(&config),
        Arc::clone(&pgpass),
    ));

    // Run initial topology discovery before accepting connections so warm-up
    // sees accurate primary/replica roles. The periodic refresh loop starts
    // below.
    info!("discovering cluster topology...");
    let _ = topology.refresh().await;

    let pools = Arc::new(PoolManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::clone(&pgpass),
    ));
    pools.warm_up().await;

    // Tracks every accepted client from accept through cleanup. Shared
    // with the admin HTTP API and the client-state metric gauge.
    let clients = Arc::new(clients::ClientRegistry::new());

    // Register pool metrics via the global meter. When OTel is disabled the
    // global meter returns no-op instruments (zero overhead).
    let _metrics = halephant_core::o11y::gauges::register_pool_gauges(
        &pools,
        Arc::clone(&config),
        Arc::clone(&topology),
        &clients,
    );

    // Start background topology refresh loop (drains pools on role changes).
    let topo_loop = Arc::clone(&topology);
    let pools_for_topo = Arc::clone(&pools);
    tokio::spawn(async move { topo_loop.run_loop(&pools_for_topo).await });

    // Start background idle connection scavenger.
    let pools_for_scavenger = Arc::clone(&pools);
    tokio::spawn(async move { pools_for_scavenger.run_scavenger().await });

    // Start admin HTTP API if configured.
    if let Some(addr) = config.load().admin.listen.clone() {
        let admin_config = Arc::clone(&config);
        let admin_pools = Arc::clone(&pools);
        let admin_topo = Arc::clone(&topology);
        let admin_clients = Arc::clone(&clients);
        tokio::spawn(async move {
            if let Err(e) = halephant_core::admin::serve(
                &addr,
                admin_config,
                admin_pools,
                admin_topo,
                admin_clients,
            )
            .await
            {
                error!(%e, "admin API failed");
            }
        });
    }

    let listeners = Arc::new(ListenerManager::new(
        Arc::clone(&config),
        Arc::clone(&topology),
        Arc::clone(&pgpass),
    ));
    let auth = Arc::new(Authenticator::new());

    // Snapshot the startup-time config for listen addresses. These
    // socket bindings happen once and cannot be rebound while the
    // process is running, so `server.listen` is in the restart-required
    // set. Other fields like `shutdown_timeout` are read from the live
    // `ArcSwap` at their point of use so hot-reloads take effect.
    let startup_cfg = config.load_full();
    // Spawn one accept task per configured listen address and funnel
    // accepted sockets through a shared channel. Lets the tokio
    // scheduler race listeners fairly — an MPSC recv inside the main
    // `select!` avoids hand-rolled polling over a runtime-sized
    // `Vec<TcpListener>` (which would bias toward whichever listener
    // is iterated first). Buffer is deliberately small: if the main
    // loop falls behind, applying backpressure to the accept side is
    // preferable to queueing sockets that would be force-closed on
    // shutdown anyway.
    let (accept_tx, mut accept_rx) = tokio::sync::mpsc::channel::<(TcpStream, SocketAddr)>(32);
    for addr in &startup_cfg.server.listen {
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("failed to bind to {addr}"))?;
        info!(%addr, "listening");
        let tx = accept_tx.clone();
        let addr = addr.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, client_addr)) => {
                        if tx.send((stream, client_addr)).await.is_err() {
                            // Main loop dropped its receiver — shutting down.
                            return;
                        }
                    }
                    Err(e) => {
                        // Accept failures are rare but recoverable (for
                        // example, fd exhaustion under load). Log and
                        // back off briefly so a sustained failure
                        // doesn't busy-spin the task.
                        warn!(%addr, %e, "accept failed");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });
    }
    // Drop the main thread's handle so the channel closes once every
    // spawned accept task exits (which happens when the listener is
    // dropped at process shutdown via task cancellation).
    drop(accept_tx);
    info!(clusters = startup_cfg.cluster.len(), "halephant ready",);
    drop(startup_cfg);

    let mut connections = JoinSet::new();

    // Hot-reload counter. Incremented once per SIGHUP attempt with an
    // `outcome` attribute so dashboards can alert on parse_failed /
    // restart_required spikes without breaking down the success series.
    let reload_counter = opentelemetry::global::meter("halephant")
        .u64_counter("halephant.config.reloads")
        .with_description("SIGHUP config reload attempts, grouped by outcome")
        .build();

    // Install SIGTERM handler so `docker stop` and similar trigger a
    // graceful drain instead of waiting out the grace period and being
    // SIGKILL'd (exit code 137). SIGINT still works for interactive users.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;

    // Install SIGHUP handler for live config reload. Operators send
    // SIGHUP after editing the config file on disk; the select branch
    // below re-reads, validates, and atomically swaps it in.
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .context("failed to install SIGHUP handler")?;

    loop {
        tokio::select! {
            Some((client_stream, client_addr)) = accept_rx.recv() => {
                client_stream.set_nodelay(true)?;
                // Snapshot the current config for the whole client
                // session. A concurrent hot-reload doesn't retarget an
                // already-connected client; their resolved database
                // mode, `otel.query_text`, and authentication settings
                // stay consistent for the duration of the session.
                // New connections accepted after the swap pick up the
                // new snapshot immediately.
                let session_cfg = config.load_full();
                let pools = Arc::clone(&pools);
                let listeners = Arc::clone(&listeners);
                let auth = Arc::clone(&auth);
                let clients = Arc::clone(&clients);
                connections.spawn(async move {
                    info!(%client_addr, "accepted");
                    if let Err(e) = proxy::frontend::forward(
                        client_stream,
                        client_addr,
                        session_cfg.as_ref(),
                        &pools,
                        &listeners,
                        &auth,
                        &clients,
                    )
                    .await
                    {
                        error!(%client_addr, %e, "connection error");
                    }
                    info!(%client_addr, "closed");
                });
            }
            _ = tokio::signal::ctrl_c() => {
                info!("received SIGINT, shutting down");
                break;
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                break;
            }
            _ = sighup.recv() => {
                info!("received SIGHUP, reloading config");
                use halephant_core::config::reload::{reload_config, ReloadOutcome};
                use opentelemetry::KeyValue;
                match reload_config(&cli.config, &config).await {
                    ReloadOutcome::Success => {
                        reload_counter.add(1, &[KeyValue::new("outcome", "success")]);
                        info!("config reloaded");
                        // Side effects after the atomic swap, spawned
                        // as a background task so the accept loop is
                        // not blocked while `topology.refresh()` waits
                        // on slow or unreachable nodes (each probe can
                        // take up to `timeout`). The swap
                        // itself is already visible to every component
                        // — these steps just bring the running state
                        // in line with the new config.
                        //
                        // Order matters: refresh topology first so
                        // `warm_up` sees the updated cluster node list
                        // when deciding which primaries and replicas
                        // to open idle connections against.
                        let topo = Arc::clone(&topology);
                        let pools_for_reload = Arc::clone(&pools);
                        tokio::spawn(async move {
                            let changed = topo.refresh().await;
                            for node in &changed {
                                pools_for_reload.drain_node(node);
                            }
                            pools_for_reload.warm_up().await;
                        });
                    }
                    ReloadOutcome::RestartRequired(field) => {
                        reload_counter.add(
                            1,
                            &[KeyValue::new("outcome", "restart_required")],
                        );
                        warn!(
                            field,
                            "config reload rejected: change to this field requires a process restart"
                        );
                    }
                    ReloadOutcome::ParseFailed(e) => {
                        reload_counter.add(
                            1,
                            &[KeyValue::new("outcome", "parse_failed")],
                        );
                        error!(%e, "config reload failed");
                    }
                }
            }
        }

        // Reap finished tasks without blocking.
        while connections.try_join_next().is_some() {}
    }

    // Graceful shutdown.
    // 1. Stop accepting (listeners dropped above).
    // 2. Wake every queued waiter with a shutting-down error so tasks
    //    blocked in `checkout()` return immediately instead of waiting
    //    out the full `checkout_timeout`. Without this, waiters would
    //    sit until their individual timeouts expire or the drain
    //    deadline aborts them.
    pools.shutdown();
    // 3. Drain in-flight client connections with a timeout. Read the
    //    timeout from the current config so a hot-reloaded value takes
    //    effect on shutdown — makes `server.shutdown_timeout`
    //    hot-reloadable even though `server.listen` is not.
    let shutdown_timeout = config.load().server.shutdown_timeout;
    let active = connections.len();
    if active > 0 {
        info!(active, timeout = ?shutdown_timeout, "shutting down, draining connections...");
        let deadline = tokio::time::sleep(shutdown_timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                result = connections.join_next() => {
                    if result.is_none() {
                        break; // all done
                    }
                    let remaining = connections.len();
                    if remaining > 0 {
                        info!(remaining, "draining...");
                    }
                }
                () = &mut deadline => {
                    let remaining = connections.len();
                    warn!(remaining, "shutdown timeout reached, dropping remaining connections");
                    connections.abort_all();
                    break;
                }
            }
        }
    }

    // 3. Close all idle pool connections.
    pools.drain_all();

    // 4. Flush pending OTel data before exiting.
    if let Some(provider) = trace_provider
        && let Err(e) = provider.shutdown()
    {
        warn!("failed to flush OpenTelemetry spans: {e}");
    }
    if let Some(provider) = meter_provider
        && let Err(e) = provider.shutdown()
    {
        warn!("failed to flush OpenTelemetry metrics: {e}");
    }
    if let Some(provider) = logger_provider
        && let Err(e) = provider.shutdown()
    {
        warn!("failed to flush OpenTelemetry logs: {e}");
    }

    info!("halephant stopped");
    Ok(())
}
