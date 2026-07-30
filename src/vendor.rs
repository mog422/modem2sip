//! Vendor audio-path setup over the modem's AT port.
//!
//! Everything else in this crate drives the modem through ModemManager.  This
//! module is the one deliberate exception, because there is no D-Bus API for
//! it: Quectel modems keep their USB voice stream switched off (`AT+QPCMV=0`)
//! even when the digital audio interface is already set to USB
//! (`AT+QDAI: 5`), so calls connect but carry no audio at all.
//! `asterisk-chan-quectel` solves it the same way.
//!
//! Only two commands are ever sent - a query and `AT+QPCMV=1,2` - and only on
//! a port ModemManager itself classified as AT.  Set
//! `audio.vendor_audio_setup = "never"` to disable this entirely.

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::mm::ModemInfo;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum VendorAudioSetup {
    /// Apply the fix on modems that need it (currently: Quectel).
    #[default]
    Auto,
    /// Always try, whatever the modem reports itself as.
    Always,
    /// Never touch an AT port.
    Never,
}

/// Does this modem need (and tolerate) the USB voice path being switched on?
pub fn applies(mode: VendorAudioSetup, info: &ModemInfo) -> bool {
    match mode {
        VendorAudioSetup::Never => false,
        VendorAudioSetup::Always => !info.at_ports.is_empty(),
        VendorAudioSetup::Auto => {
            !info.at_ports.is_empty()
                && (info.manufacturer.to_ascii_lowercase().contains("quectel")
                    || is_quectel_model(&info.model))
        }
    }
}

fn is_quectel_model(model: &str) -> bool {
    let m = model.to_ascii_uppercase();
    ["EC2", "EC1", "EP0", "EG2", "EG9", "EM0", "BG9", "RM5"]
        .iter()
        .any(|p| m.starts_with(p))
}

/// Make sure the modem streams call audio over its USB sound card.
///
/// Returns the port that accepted the command.  Best effort: a failure means
/// calls may be silent, not that the modem is unusable, so callers log and
/// carry on.
pub async fn enable_usb_audio(info: &ModemInfo, force: bool) -> Result<String> {
    if info.at_ports.is_empty() {
        bail!("ModemManager reports no AT port for this modem");
    }
    // Later ports first: ModemManager tends to keep the first AT port for
    // itself, and on Quectel the last one is the spare.
    let ports: Vec<String> = info.at_ports.iter().rev().cloned().collect();
    let mut last_error = None;

    for port in ports {
        let p = port.clone();
        let result = tokio::task::spawn_blocking(move || configure_port(&p, force)).await?;
        match result {
            Ok(Outcome::AlreadyOn) => {
                debug!(port, "USB voice path already enabled");
                return Ok(port);
            }
            Ok(Outcome::Enabled) => {
                info!(port, "enabled the modem's USB voice path (AT+QPCMV=1,2)");
                return Ok(port);
            }
            Err(e) => {
                debug!(port, error = %format!("{e:#}"), "AT port did not accept the command");
                last_error = Some(e);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("no usable AT port")))
}

enum Outcome {
    AlreadyOn,
    Enabled,
}

fn configure_port(port: &str, force: bool) -> Result<Outcome> {
    let mut at = AtPort::open(port)?;

    // The digital audio interface itself is a persistent setting; changing it
    // needs a modem reset, so only warn about it.
    match at.command("AT+QDAI?") {
        Ok(reply) => {
            if let Some(mode) = reply
                .lines()
                .find_map(|l| l.trim().strip_prefix("+QDAI:"))
                .and_then(|v| v.trim().split(',').next())
                .and_then(|v| v.trim().parse::<u32>().ok())
            {
                if mode != 5 {
                    warn!(
                        qdai = mode,
                        port,
                        "the modem's digital audio interface is not USB (expected 5); \
                         run AT+QDAI=5,0,0,4,0,0,1,1 once and reset the modem, \
                         otherwise call audio cannot reach the sound card"
                    );
                }
            }
        }
        Err(e) => debug!(error = %e, "AT+QDAI? not supported"),
    }

    if !force {
        if let Ok(reply) = at.command("AT+QPCMV?") {
            if let Some(v) = reply.lines().find_map(|l| l.trim().strip_prefix("+QPCMV:")) {
                if v.trim().starts_with('1') {
                    return Ok(Outcome::AlreadyOn);
                }
            }
        }
    }

    // 1 = enable, 2 = route the PCM stream over USB.
    at.command("AT+QPCMV=1,2").context("AT+QPCMV=1,2")?;
    Ok(Outcome::Enabled)
}

/// A minimal blocking AT transport: raw termios, one command at a time.
struct AtPort {
    file: std::fs::File,
}

impl AtPort {
    fn open(path: &str) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOCTTY)
            .open(path)
            .with_context(|| format!("opening {path}"))?;

        // 115200 8N1 raw; reads return after 0.5 s of silence.
        unsafe {
            let fd = file.as_raw_fd();
            let mut tio: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut tio) != 0 {
                bail!("tcgetattr({path}): {}", std::io::Error::last_os_error());
            }
            libc::cfmakeraw(&mut tio);
            libc::cfsetispeed(&mut tio, libc::B115200);
            libc::cfsetospeed(&mut tio, libc::B115200);
            tio.c_cflag |= libc::CLOCAL | libc::CREAD;
            tio.c_cflag &= !libc::CRTSCTS;
            tio.c_cc[libc::VMIN] = 0;
            tio.c_cc[libc::VTIME] = 5;
            if libc::tcsetattr(fd, libc::TCSANOW, &tio) != 0 {
                bail!("tcsetattr({path}): {}", std::io::Error::last_os_error());
            }
            libc::tcflush(fd, libc::TCIOFLUSH);
        }
        Ok(Self { file })
    }

    /// Send one command and collect the reply up to OK/ERROR.
    fn command(&mut self, cmd: &str) -> Result<String> {
        self.file
            .write_all(format!("{cmd}\r").as_bytes())
            .with_context(|| format!("writing {cmd}"))?;
        self.file.flush().ok();

        let deadline = Instant::now() + Duration::from_secs(3);
        let mut reply = String::new();
        let mut buf = [0u8; 256];
        while Instant::now() < deadline {
            match self.file.read(&mut buf) {
                Ok(0) => continue,
                Ok(n) => {
                    reply.push_str(&String::from_utf8_lossy(&buf[..n]));
                    if reply.contains("OK") {
                        return Ok(reply);
                    }
                    if reply.contains("ERROR") {
                        bail!("{cmd} -> {}", reply.trim().replace(['\r', '\n'], " "));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e).with_context(|| format!("reading the reply to {cmd}")),
            }
        }
        bail!("{cmd} timed out (no OK from the modem)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(manufacturer: &str, model: &str, ports: &[&str]) -> ModemInfo {
        ModemInfo {
            manufacturer: manufacturer.into(),
            model: model.into(),
            at_ports: ports.iter().map(|p| p.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn auto_targets_quectel_only() {
        let quectel = info("Quectel", "EP06-E", &["/dev/ttyUSB2", "/dev/ttyUSB3"]);
        let other = info("Sierra Wireless", "EM7455", &["/dev/ttyUSB0"]);
        assert!(applies(VendorAudioSetup::Auto, &quectel));
        assert!(!applies(VendorAudioSetup::Auto, &other));
        assert!(applies(VendorAudioSetup::Always, &other));
        assert!(!applies(VendorAudioSetup::Never, &quectel));
    }

    #[test]
    fn model_prefix_is_enough() {
        // Some firmwares leave the manufacturer string empty.
        let m = info("", "EC25-E", &["/dev/ttyUSB2"]);
        assert!(applies(VendorAudioSetup::Auto, &m));
    }

    #[test]
    fn no_at_port_means_nothing_to_do() {
        let m = info("Quectel", "EP06-E", &[]);
        assert!(!applies(VendorAudioSetup::Auto, &m));
        assert!(!applies(VendorAudioSetup::Always, &m));
    }
}
