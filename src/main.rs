//! modem2sip - expose one LTE modem as a SIP endpoint.
//!
//! * modem control:  ModemManager over D-Bus (no AT commands)
//! * voice:          the ALSA card the modem exposes, bridged to RTP
//! * SMS/MMS:        SQLite storage plus SIP MESSAGE notifications
//!
//! One process serves exactly one modem, selected in the config file.

mod audio;
mod config;
mod db;
mod gateway;
mod http_api;
mod media;
mod mm;
mod mms;
mod sip;
mod state;
mod vendor;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "modem2sip", version, about = "Expose an LTE modem as a SIP endpoint")]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "/etc/modem2sip/config.toml")]
    config: PathBuf,

    /// List the modems ModemManager knows about and exit.
    #[arg(long)]
    list_modems: bool,

    /// List the ALSA cards on this system and exit.
    #[arg(long)]
    list_cards: bool,

}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.list_cards {
        init_tracing("info");
        for card in audio::list_cards() {
            println!(
                "card {:<3} id={:<16} playback={:<5} capture={:<5} device={} ({})",
                card.index,
                card.id,
                card.has_playback,
                card.has_capture,
                card.device_path.display(),
                card.name
            );
        }
        return Ok(());
    }

    if args.list_modems {
        init_tracing("info");
        return list_modems().await;
    }

    let cfg = Arc::new(config::Config::load(&args.config)?);
    init_tracing(&cfg.general.log);


    info!(
        version = env!("CARGO_PKG_VERSION"),
        config = %args.config.display(),
        "starting modem2sip"
    );

    // --- storage -----------------------------------------------------------
    tokio::fs::create_dir_all(&cfg.storage.dir)
        .await
        .with_context(|| format!("creating {}", cfg.storage.dir.display()))?;
    let db = db::Db::open(&cfg.storage.db_path(), &cfg.storage.attachments_dir()).await?;
    info!(path = %cfg.storage.db_path().display(), "database ready");

    let mms = Arc::new(mms::MmsManager::new(cfg.clone(), db.clone()));
    let shared = state::Shared::new(cfg.clone(), db, mms);

    // --- wiring ------------------------------------------------------------
    let (sip_tx, sip_rx) = mpsc::channel(64);
    let (modem_tx, modem_rx) = mpsc::channel(64);

    let core = sip::SipCore::new(cfg.clone(), sip_tx, shared.modem_ready.clone()).await?;

    let mut tasks = Vec::new();
    tasks.push(tokio::spawn({
        let core = core.clone();
        async move {
            if let Err(e) = core.run().await {
                error!(error = %format!("{e:#}"), "SIP transport stopped");
            }
        }
    }));
    tasks.push(tokio::spawn({
        let core = core.clone();
        async move { sip::register::run(core).await }
    }));
    tasks.push(tokio::spawn({
        let cfg = cfg.clone();
        async move { mm::watcher::run(cfg, modem_tx).await }
    }));
    tasks.push(tokio::spawn({
        let shared = shared.clone();
        async move {
            if let Err(e) = http_api::run(shared).await {
                error!(error = %format!("{e:#}"), "HTTP API stopped");
            }
        }
    }));
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let gateway = tokio::spawn({
        let shared = shared.clone();
        let core = core.clone();
        async move { gateway::run(shared, core, sip_rx, modem_rx, shutdown_rx).await }
    });

    info!("modem2sip is running; SIP answers 503 until the modem is ready");
    shutdown_signal().await;
    warn!("shutting down");

    // Let the gateway hang up whatever is in progress before anything else
    // goes away; a call abandoned here stays connected on the network.
    // It only sees the signal between events, so a handler that is blocked on
    // a wedged D-Bus call will miss the deadline - say so, because the mobile
    // leg is then still up when the process exits.
    let _ = shutdown_tx.send(());
    let abort = gateway.abort_handle();
    if tokio::time::timeout(Duration::from_secs(5), gateway).await.is_err() {
        warn!("the gateway did not finish in time; a call in progress may still be connected");
        abort.abort();
    }
    for t in tasks {
        t.abort();
    }
    Ok(())
}

fn init_tracing(filter: &str) {
    let env = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));
    let _ = tracing_subscriber::fmt().with_env_filter(env).with_target(false).try_init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut s) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

/// `--list-modems`: print what ModemManager sees, with the values that can be
/// used in the `[modem]` section of the config file.
async fn list_modems() -> Result<()> {
    use mm::proxies::{ModemProxy, SimProxy, MM_PATH, MM_SERVICE};
    use zbus::fdo::ObjectManagerProxy;

    let conn = zbus::Connection::system().await.context("connecting to the system bus")?;
    let om = ObjectManagerProxy::builder(&conn)
        .destination(MM_SERVICE)?
        .path(MM_PATH)?
        .build()
        .await
        .context("is ModemManager running?")?;

    let objects = om.get_managed_objects().await?;
    let mut paths: Vec<_> = objects
        .into_iter()
        .filter(|(_, ifaces)| ifaces.contains_key("org.freedesktop.ModemManager1.Modem"))
        .map(|(p, _)| p)
        .collect();
    paths.sort_by_key(|p| p.as_str().to_string());

    if paths.is_empty() {
        println!("ModemManager reports no modems.");
        return Ok(());
    }

    for path in paths {
        let modem = ModemProxy::builder(&conn).path(path.clone())?.build().await?;
        let device = modem.device().await.unwrap_or_default();
        println!("{}", path.as_str());
        println!("  model         = {}", modem.model().await.unwrap_or_default());
        println!("  manufacturer  = {}", modem.manufacturer().await.unwrap_or_default());
        println!("  imei          = {}", modem.equipment_identifier().await.unwrap_or_default());
        println!("  device        = {device}");
        println!("  device_id     = {}", modem.device_identifier().await.unwrap_or_default());
        println!("  primary_port  = {}", modem.primary_port().await.unwrap_or_default());
        println!(
            "  state         = {}",
            mm::modem_state::modem_state_name(modem.state().await.unwrap_or(0))
        );
        if let Ok(sim_path) = modem.sim().await {
            if sim_path.as_str() != "/" {
                if let Ok(sim) = SimProxy::builder(&conn).path(sim_path)?.build().await {
                    println!("  sim_id        = {}", sim.sim_identifier().await.unwrap_or_default());
                    println!("  imsi          = {}", sim.imsi().await.unwrap_or_default());
                }
            }
        }
        match audio::find_for_modem(&device, None) {
            Some(card) => println!(
                "  alsa          = {} (card {}, {})",
                card.device_string(true),
                card.index,
                card.name
            ),
            None => println!("  alsa          = <not found - set audio.device manually>"),
        }
        println!();
    }
    Ok(())
}
