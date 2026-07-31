//! Modem discovery and supervision.
//!
//! Requirement: the modem may be missing at start-up, may show up later, and
//! may vanish and come back at any time.  This task therefore never gives up:
//! it reconnects to the system bus, re-scans on every ObjectManager change,
//! and re-runs the whole activation sequence each time the device returns.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use zbus::fdo::ObjectManagerProxy;
use zbus::Connection;
use zvariant::OwnedObjectPath;

use crate::audio;
use crate::config::{Config, ModemMatch};

use super::proxies::{state as mstate, Modem3gppProxy, ModemProxy, SimProxy, MM_PATH, MM_SERVICE};
use super::ModemHandle;

const MODEM_IFACE: &str = "org.freedesktop.ModemManager1.Modem";

#[derive(Debug)]
pub enum ModemEvent {
    /// The configured modem is present and usable.
    Up(Arc<ModemHandle>),
    /// The modem is gone or no longer usable.  SIP answers 503 until Up.
    Down { reason: String },
    CallAdded(OwnedObjectPath),
    CallDeleted(OwnedObjectPath),
    CallState { path: OwnedObjectPath, old: i32, new: i32, reason: u32 },
    Dtmf { path: OwnedObjectPath, digit: String },
    SmsAdded { path: OwnedObjectPath, received: bool },
}

pub async fn run(cfg: Arc<Config>, tx: mpsc::Sender<ModemEvent>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match Connection::system().await {
            Ok(conn) => {
                info!("connected to the system bus");
                let connected_at = tokio::time::Instant::now();
                if let Err(e) = session(&cfg, conn, &tx).await {
                    warn!(error = %format!("{e:#}"), "ModemManager session ended");
                }
                let _ = tx.send(ModemEvent::Down { reason: "bus session ended".into() }).await;
                // Resetting the backoff on a successful *connect* undoes it
                // entirely when the failure comes just after connecting -
                // ModemManager restarting in a loop, say - and turns this
                // into a reconnect flood.  Only a session that actually
                // lasted counts as progress.
                if connected_at.elapsed() > Duration::from_secs(30) {
                    backoff = Duration::from_secs(1);
                }
            }
            Err(e) => {
                warn!(error = %format!("{e:#}"), "cannot connect to the system bus, retrying");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

/// One D-Bus connection's worth of work.  Returns when the bus or
/// ModemManager needs a full reconnect.
async fn session(cfg: &Arc<Config>, conn: Connection, tx: &mpsc::Sender<ModemEvent>) -> Result<()> {
    let om = ObjectManagerProxy::builder(&conn)
        .destination(MM_SERVICE)?
        .path(MM_PATH)?
        .build()
        .await
        .context("building ObjectManager proxy")?;

    loop {
        // ---- discovery: wait for our modem to show up -------------------
        let path = wait_for_modem(cfg, &conn, &om).await?;
        info!(path = path.as_str(), "modem matched");

        // ---- activation --------------------------------------------------
        match activate(cfg, &conn, &path).await {
            Ok(handle) => {
                let info = handle.info.clone();
                info!(
                    path = handle.path.as_str(),
                    model = %info.model,
                    imei = %info.equipment_id,
                    port = %info.primary_port,
                    alsa = %handle.alsa.as_ref().map(|c| c.device_string(true)).unwrap_or_else(|| "-".into()),
                    "modem ready"
                );
                if tx.send(ModemEvent::Up(handle.clone())).await.is_err() {
                    return Err(anyhow!("gateway channel closed"));
                }
                let reason = supervise(&conn, &om, handle, tx).await;
                warn!(%reason, "modem no longer usable");
                if tx.send(ModemEvent::Down { reason }).await.is_err() {
                    return Err(anyhow!("gateway channel closed"));
                }
            }
            Err(e) => {
                warn!(error = %format!("{e:#}"), "modem activation failed, retrying");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

async fn wait_for_modem(
    cfg: &Arc<Config>,
    conn: &Connection,
    om: &ObjectManagerProxy<'_>,
) -> Result<OwnedObjectPath> {
    let mut added = om.receive_interfaces_added().await?;
    let mut announced = false;
    loop {
        match find_modem(conn, om, &cfg.modem).await {
            Ok(Some(path)) => return Ok(path),
            Ok(None) => {
                if !announced {
                    info!("no modem matches the configuration yet, waiting");
                    announced = true;
                }
            }
            Err(e) => {
                // ModemManager may have died; let the caller reconnect.
                return Err(e);
            }
        }
        // React to new objects immediately, but re-scan periodically anyway:
        // signals can be missed while ModemManager restarts.
        tokio::select! {
            sig = added.next() => {
                if sig.is_none() {
                    return Err(anyhow!("ObjectManager signal stream closed"));
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
        }
    }
}

pub async fn find_modem(
    conn: &Connection,
    om: &ObjectManagerProxy<'_>,
    sel: &ModemMatch,
) -> Result<Option<OwnedObjectPath>> {
    let objects = om.get_managed_objects().await.context("GetManagedObjects")?;
    let mut paths: Vec<OwnedObjectPath> = objects
        .into_iter()
        .filter(|(_, ifaces)| ifaces.contains_key(MODEM_IFACE))
        .map(|(path, _)| path)
        .collect();
    paths.sort_by_key(|p| path_index(p).unwrap_or(u32::MAX));

    for path in paths {
        match modem_matches(conn, &path, sel).await {
            Ok(true) => return Ok(Some(path)),
            Ok(false) => {}
            Err(e) => debug!(path = path.as_str(), error = %format!("{e:#}"), "skipping modem that could not be inspected"),
        }
    }
    Ok(None)
}

fn path_index(path: &OwnedObjectPath) -> Option<u32> {
    path.as_str().rsplit('/').next()?.parse().ok()
}

/// Every configured criterion must match.  An empty `[modem]` section
/// matches the first modem, which is only sane on single-modem systems.
async fn modem_matches(conn: &Connection, path: &OwnedObjectPath, sel: &ModemMatch) -> Result<bool> {
    let modem = ModemProxy::builder(conn).path(path.clone())?.build().await?;

    // An empty string in the config means "not set" - otherwise a stub like
    // `imei = ""` would silently match nothing at all.
    let want = |field: &Option<String>| -> Option<String> {
        field.as_ref().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
    };
    let (sel_device, sel_device_id, sel_imei, sel_primary_port, sel_sim_id, sel_imsi) = (
        want(&sel.device),
        want(&sel.device_id),
        want(&sel.imei),
        want(&sel.primary_port),
        want(&sel.sim_id),
        want(&sel.imsi),
    );

    if let Some(want) = &sel.index {
        if path_index(path) != Some(*want) {
            return Ok(false);
        }
    }
    if let Some(want) = &sel_device {
        let device = modem.device().await.unwrap_or_default();
        // Prefix match so a config can name the USB device while MM reports
        // a deeper path (or vice versa).
        if !(device.starts_with(want.as_str()) || want.starts_with(&device)) || device.is_empty() {
            return Ok(false);
        }
    }
    if let Some(want) = &sel_device_id {
        if !eq_ci(&modem.device_identifier().await.unwrap_or_default(), want) {
            return Ok(false);
        }
    }
    if let Some(want) = &sel_imei {
        let mut equipment = modem.equipment_identifier().await.unwrap_or_default();
        if equipment.is_empty() {
            if let Ok(p) = Modem3gppProxy::builder(conn).path(path.clone())?.build().await {
                equipment = p.imei().await.unwrap_or_default();
            }
        }
        if !eq_ci(&equipment, want) {
            return Ok(false);
        }
    }
    if let Some(want) = &sel_primary_port {
        if !eq_ci(&modem.primary_port().await.unwrap_or_default(), want) {
            return Ok(false);
        }
    }
    if sel_sim_id.is_some() || sel_imsi.is_some() {
        let Ok(sim_path) = modem.sim().await else { return Ok(false) };
        if sim_path.as_str() == "/" {
            return Ok(false);
        }
        let sim = SimProxy::builder(conn).path(sim_path)?.build().await?;
        if let Some(want) = &sel_sim_id {
            if !eq_ci(&sim.sim_identifier().await.unwrap_or_default(), want) {
                return Ok(false);
            }
        }
        if let Some(want) = &sel_imsi {
            if !eq_ci(&sim.imsi().await.unwrap_or_default(), want) {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn eq_ci(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

/// Enable the modem if needed, wait until it is usable, resolve its ALSA card.
async fn activate(
    cfg: &Arc<Config>,
    conn: &Connection,
    path: &OwnedObjectPath,
) -> Result<Arc<ModemHandle>> {
    let modem = ModemProxy::builder(conn).path(path.clone())?.build().await?;

    // Wait (bounded) for the modem to become usable, enabling it if allowed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let state = modem.state().await.context("reading Modem.State")?;
        if state >= mstate::MODEM_ENABLED {
            break;
        }
        match state {
            mstate::MODEM_FAILED => {
                let reason = modem.state_failed_reason().await.unwrap_or(0);
                return Err(anyhow!("modem is in the failed state (reason {reason})"));
            }
            mstate::MODEM_LOCKED => {
                return Err(anyhow!("SIM is locked; unlock it with mmcli or NetworkManager"));
            }
            mstate::MODEM_DISABLED if cfg.modem.enable => {
                info!("modem is disabled, enabling");
                if let Err(e) = modem.enable(true).await {
                    warn!(error = %format!("{e:#}"), "Modem.Enable failed");
                }
            }
            _ => {}
        }
        if tokio::time::Instant::now() > deadline {
            return Err(anyhow!(
                "modem did not become usable (state: {})",
                mstate::modem_state_name(modem.state().await.unwrap_or(0))
            ));
        }
        debug!(state = mstate::modem_state_name(state), "waiting for the modem to become usable");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let device = modem.device().await.unwrap_or_default();
    let alsa = resolve_alsa(cfg, &device);

    let handle = ModemHandle::build(conn.clone(), path.clone(), alsa).await?;

    // Vendor audio path (Quectel: AT+QPCMV=1,2).  Not fatal: SMS still works
    // on a modem whose voice path we could not switch on.
    if crate::vendor::applies(cfg.audio.vendor_audio_setup, &handle.info) {
        match crate::vendor::enable_usb_audio(&handle.info, false).await {
            Ok(port) => debug!(port, "USB voice path ready"),
            Err(e) => warn!(
                error = %format!("{e:#}"),
                "could not enable the modem's USB voice path; calls may be silent"
            ),
        }
    }

    run_ready_command(cfg, &handle).await;
    Ok(handle)
}

/// Vendor setup that ModemManager cannot do for us (see
/// `config::ModemMatch::ready_command`).  A failure is logged, not fatal: the
/// modem is still good for SMS even if its audio path stays off.
async fn run_ready_command(cfg: &Arc<Config>, handle: &Arc<ModemHandle>) {
    let Some(command) = cfg.modem.ready_command.as_deref() else { return };
    let info = &handle.info;
    let mut cmd = tokio::process::Command::new("sh");
    cmd.arg("-c")
        .arg(command)
        .env("M2S_MODEM_PATH", &info.path)
        .env("M2S_DEVICE", &info.device)
        .env("M2S_IMEI", &info.equipment_id)
        .env("M2S_PRIMARY_PORT", &info.primary_port)
        .env("M2S_AT_PORT", info.at_ports.first().cloned().unwrap_or_default())
        .env("M2S_AT_PORTS", info.at_ports.join(" "))
        .env("M2S_AUDIO_PORTS", info.audio_ports.join(" "))
        .env(
            "M2S_ALSA_DEVICE",
            handle.alsa.as_ref().map(|c| c.device_string(true)).unwrap_or_default(),
        );

    info!(command, "running modem.ready_command");
    match cmd.output().await {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !stdout.is_empty() {
                info!(output = %stdout, "modem.ready_command succeeded");
            }
        }
        Ok(out) => warn!(
            status = %out.status,
            stderr = %String::from_utf8_lossy(&out.stderr).trim(),
            "modem.ready_command failed; vendor setup (e.g. the USB voice path) may be missing"
        ),
        Err(e) => warn!(error = %format!("{e:#}"), "could not run modem.ready_command"),
    }
}

fn resolve_alsa(cfg: &Arc<Config>, device: &str) -> Option<audio::AlsaCard> {
    if cfg.audio.device.is_some()
        || (cfg.audio.capture_device.is_some() && cfg.audio.playback_device.is_some())
    {
        // Explicitly configured; no detection needed.
        return None;
    }
    if !cfg.audio.auto {
        warn!("audio.auto is disabled but no explicit ALSA device is configured; calls will fail");
        return None;
    }
    match audio::find_for_modem(device, cfg.audio.card_hint.as_deref()) {
        Some(card) => {
            info!(
                card = card.index,
                id = %card.id,
                name = %card.name,
                "ALSA card resolved for this modem"
            );
            Some(card)
        }
        None => {
            error!(
                device,
                "no ALSA card could be matched to this modem; set audio.device explicitly"
            );
            None
        }
    }
}

/// Watch the modem until it becomes unusable.  Returns the reason.
async fn supervise(
    conn: &Connection,
    om: &ObjectManagerProxy<'_>,
    handle: Arc<ModemHandle>,
    tx: &mpsc::Sender<ModemEvent>,
) -> String {
    let mut removed = match om.receive_interfaces_removed().await {
        Ok(s) => s,
        Err(e) => return format!("cannot watch ObjectManager: {e}"),
    };
    // ModemManager exiting does not necessarily emit InterfacesRemoved, so
    // watch the bus name too: that turns a 10 s detection delay into an
    // instant one.
    let mut name_changes = match zbus::fdo::DBusProxy::new(conn).await {
        Ok(dbus) => match dbus.receive_name_owner_changed().await {
            Ok(s) => Some(s),
            Err(e) => {
                debug!(error = %format!("{e:#}"), "cannot watch NameOwnerChanged");
                None
            }
        },
        Err(e) => {
            debug!(error = %format!("{e:#}"), "cannot reach the bus driver");
            None
        }
    };
    let mut state_changes = match handle.modem.receive_modem_state_changed().await {
        Ok(s) => s,
        Err(e) => return format!("cannot watch Modem.StateChanged: {e}"),
    };
    let mut call_added = match handle.voice.receive_call_added().await {
        Ok(s) => s,
        Err(e) => return format!("cannot watch Voice.CallAdded: {e}"),
    };
    let mut call_deleted = match handle.voice.receive_call_deleted().await {
        Ok(s) => s,
        Err(e) => return format!("cannot watch Voice.CallDeleted: {e}"),
    };
    let mut sms_added = match handle.messaging.receive_added().await {
        Ok(s) => s,
        Err(e) => return format!("cannot watch Messaging.Added: {e}"),
    };

    let mut call_tasks: HashMap<String, JoinHandle<()>> = HashMap::new();

    // Adopt whatever already exists: calls that survived a restart and
    // messages stored on the SIM/modem.  An adopted call needs the same
    // state subscription as one that arrives by signal, or its hangup is
    // never noticed and the SIP leg hangs on dead air.
    if let Ok(calls) = handle.list_calls().await {
        for c in calls {
            spawn_call_watch(&handle, &c, tx, &mut call_tasks).await;
            let _ = tx.send(ModemEvent::CallAdded(c)).await;
        }
    }
    if let Ok(messages) = handle.list_sms().await {
        for m in messages {
            let _ = tx.send(ModemEvent::SmsAdded { path: m, received: false }).await;
        }
    }

    let mut health = tokio::time::interval(Duration::from_secs(10));
    health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    health.tick().await; // fires immediately

    let reason = loop {
        tokio::select! {
            sig = removed.next() => {
                let Some(sig) = sig else { break "ObjectManager stream closed".to_string() };
                let Ok(args) = sig.args() else { continue };
                if args.object_path().as_str() == handle.path.as_str()
                    && args.interfaces().contains(&MODEM_IFACE)
                {
                    break "modem removed from ModemManager".to_string();
                }
            }
            sig = state_changes.next() => {
                let Some(sig) = sig else { break "StateChanged stream closed".to_string() };
                let Ok(args) = sig.args() else { continue };
                let (old, new, why) = (*args.old_state(), *args.new_state(), *args.reason());
                debug!(
                    old = mstate::modem_state_name(old),
                    new = mstate::modem_state_name(new),
                    why,
                    "modem state changed"
                );
                if new < mstate::MODEM_ENABLED {
                    break format!("modem state dropped to {}", mstate::modem_state_name(new));
                }
            }
            sig = call_added.next() => {
                let Some(sig) = sig else { break "CallAdded stream closed".to_string() };
                let Ok(args) = sig.args() else { continue };
                let path = args.path().clone();
                spawn_call_watch(&handle, &path, tx, &mut call_tasks).await;
                let _ = tx.send(ModemEvent::CallAdded(path)).await;
            }
            sig = call_deleted.next() => {
                let Some(sig) = sig else { break "CallDeleted stream closed".to_string() };
                let Ok(args) = sig.args() else { continue };
                let path = args.path().clone();
                if let Some(task) = call_tasks.remove(path.as_str()) {
                    task.abort();
                }
                let _ = tx.send(ModemEvent::CallDeleted(path)).await;
            }
            sig = sms_added.next() => {
                let Some(sig) = sig else { break "Messaging.Added stream closed".to_string() };
                let Ok(args) = sig.args() else { continue };
                let _ = tx.send(ModemEvent::SmsAdded {
                    path: args.path().clone(),
                    received: *args.received(),
                }).await;
            }
            sig = async {
                match name_changes.as_mut() {
                    Some(s) => s.next().await,
                    None => std::future::pending().await,
                }
            } => {
                let Some(sig) = sig else { name_changes = None; continue };
                if let Ok(args) = sig.args() {
                    if args.name().as_str() == MM_SERVICE && args.new_owner().is_none() {
                        break "ModemManager left the bus".to_string();
                    }
                }
            }
            _ = health.tick() => {
                match handle.modem.state().await {
                    Ok(s) if s < mstate::MODEM_ENABLED => {
                        break format!("modem state is {}", mstate::modem_state_name(s));
                    }
                    Ok(_) => {}
                    Err(e) => break format!("modem is unreachable on D-Bus: {e}"),
                }
            }
        }
    };

    for (_, task) in call_tasks {
        task.abort();
    }
    reason
}

async fn spawn_call_watch(
    handle: &Arc<ModemHandle>,
    path: &OwnedObjectPath,
    tx: &mpsc::Sender<ModemEvent>,
    tasks: &mut HashMap<String, JoinHandle<()>>,
) {
    let call = match handle.call_proxy(path).await {
        Ok(c) => c,
        Err(e) => {
            warn!(path = path.as_str(), error = %format!("{e:#}"), "cannot watch call");
            return;
        }
    };
    let tx = tx.clone();
    let path = path.clone();
    let key = path.to_string();
    let task = tokio::spawn(async move {
        let mut states = match call.receive_call_state_changed().await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %format!("{e:#}"), "Call.StateChanged subscription failed");
                return;
            }
        };
        let mut dtmf = call.receive_dtmf_received().await.ok();

        loop {
            tokio::select! {
                sig = states.next() => {
                    let Some(sig) = sig else { return };
                    let Ok(args) = sig.args() else { continue };
                    let _ = tx.send(ModemEvent::CallState {
                        path: path.clone(),
                        old: *args.old_state(),
                        new: *args.new_state(),
                        reason: *args.reason(),
                    }).await;
                }
                sig = async {
                    match dtmf.as_mut() {
                        Some(s) => s.next().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let Some(sig) = sig else { dtmf = None; continue };
                    if let Ok(args) = sig.args() {
                        let _ = tx.send(ModemEvent::Dtmf {
                            path: path.clone(),
                            digit: args.dtmf().to_string(),
                        }).await;
                    }
                }
            }
        }
    });
    // ModemManager reuses call object paths, so a stale watcher for the same
    // path has to go rather than being silently leaked.
    if let Some(previous) = tasks.insert(key, task) {
        previous.abort();
    }
}
