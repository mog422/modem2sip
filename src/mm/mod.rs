//! ModemManager integration: discovery, supervision and the operations the
//! gateway performs on the modem.

pub mod proxies;
pub mod watcher;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::debug;
use zbus::Connection;
use zvariant::{OwnedObjectPath, Value};

use crate::audio::AlsaCard;

pub use proxies::{call as call_state, sms as sms_state, state as modem_state};
use proxies::{
    CallProxy, MessagingProxy, Modem3gppProxy, ModemProxy, SimProxy, SmsProxy, VoiceProxy,
};

/// Everything the gateway needs to know about the modem it owns.
#[derive(Debug, Clone, Default)]
pub struct ModemInfo {
    pub path: String,
    pub device: String,
    pub device_id: String,
    pub equipment_id: String,
    pub manufacturer: String,
    pub model: String,
    pub revision: String,
    pub primary_port: String,
    /// Ports ModemManager classified as AT (MM_MODEM_PORT_TYPE_AT).
    pub at_ports: Vec<String>,
    /// Ports ModemManager classified as audio, if the driver reports any.
    pub audio_ports: Vec<String>,
    pub own_number: Option<String>,
    pub sim_id: Option<String>,
    pub imsi: Option<String>,
    pub operator: Option<String>,
}

/// A live, matched modem.  Dropped (and replaced) whenever the device goes
/// away and comes back.
pub struct ModemHandle {
    pub conn: Connection,
    pub path: OwnedObjectPath,
    pub modem: ModemProxy<'static>,
    pub voice: VoiceProxy<'static>,
    pub messaging: MessagingProxy<'static>,
    pub info: ModemInfo,
    /// The ALSA card that belongs to *this* modem, if one was found.
    pub alsa: Option<AlsaCard>,
}

impl std::fmt::Debug for ModemHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModemHandle").field("path", &self.path).field("info", &self.info).finish()
    }
}

/// Where MMS traffic should be sourced from, and who resolves its names.
#[derive(Debug, Clone)]
pub struct BearerNet {
    pub interface: String,
    pub address: Option<std::net::IpAddr>,
    /// The operator's resolvers, which are usually the only ones that know
    /// the MMSC host names.
    pub dns: Vec<std::net::IpAddr>,
}

#[derive(Debug, Clone)]
pub struct CallInfo {
    pub path: String,
    pub number: String,
    pub direction: i32,
    pub state: i32,
    pub audio_port: Option<String>,
    pub audio_format: HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct SmsInfo {
    pub path: String,
    pub state: u32,
    pub pdu_type: u32,
    pub number: String,
    pub text: String,
    pub data: Vec<u8>,
    pub timestamp: Option<String>,
    pub smsc: Option<String>,
    pub class: i32,
}

impl SmsInfo {
    /// A binary SMS with no text is (almost always) a WAP push, i.e. an MMS
    /// notification.
    pub fn looks_like_wap_push(&self) -> bool {
        self.text.is_empty() && self.data.len() > 3 && crate::mms::is_wap_push(&self.data)
    }
}

impl ModemHandle {
    pub async fn build(
        conn: Connection,
        path: OwnedObjectPath,
        alsa: Option<AlsaCard>,
    ) -> Result<Arc<Self>> {
        let modem = ModemProxy::builder(&conn)
            .path(path.clone())?
            .build()
            .await
            .context("building Modem proxy")?;
        let voice = VoiceProxy::builder(&conn)
            .path(path.clone())?
            .build()
            .await
            .context("building Modem.Voice proxy")?;
        let messaging = MessagingProxy::builder(&conn)
            .path(path.clone())?
            .build()
            .await
            .context("building Modem.Messaging proxy")?;

        let info = read_info(&conn, &path, &modem).await;
        Ok(Arc::new(Self { conn, path, modem, voice, messaging, info, alsa }))
    }

    pub async fn state(&self) -> Result<i32> {
        Ok(self.modem.state().await?)
    }

    pub async fn signal_quality(&self) -> Option<u32> {
        self.modem.signal_quality().await.ok().map(|(q, _)| q)
    }

    /// Usable for calls/messages?  Anything from "enabled" upwards is fine;
    /// registration is checked separately because some networks report
    /// `enabled` while attached.
    pub async fn is_usable(&self) -> bool {
        matches!(self.state().await, Ok(s) if s >= modem_state::MODEM_ENABLED)
    }

    pub async fn call_proxy(&self, path: &OwnedObjectPath) -> Result<CallProxy<'static>> {
        Ok(CallProxy::builder(&self.conn).path(path.clone())?.build().await?)
    }

    pub async fn sms_proxy(&self, path: &OwnedObjectPath) -> Result<SmsProxy<'static>> {
        Ok(SmsProxy::builder(&self.conn).path(path.clone())?.build().await?)
    }

    pub async fn call_info(&self, path: &OwnedObjectPath) -> Result<CallInfo> {
        let call = self.call_proxy(path).await?;
        let audio_port = call.audio_port().await.ok().filter(|s| !s.is_empty());
        let audio_format = call
            .audio_format()
            .await
            .map(|m| m.into_iter().map(|(k, v)| (k, format!("{v:?}"))).collect())
            .unwrap_or_default();
        Ok(CallInfo {
            path: path.to_string(),
            number: call.number().await.unwrap_or_default(),
            direction: call.direction().await.unwrap_or(call_state::DIR_UNKNOWN),
            state: call.state().await.unwrap_or(call_state::UNKNOWN),
            audio_port,
            audio_format,
        })
    }

    /// Place an outgoing call.  Returns the object path of the MM call.
    pub async fn dial(&self, number: &str) -> Result<OwnedObjectPath> {
        let mut props: HashMap<&str, Value<'_>> = HashMap::new();
        props.insert("number", Value::from(number));
        let path = self.voice.create_call(props).await.context("Voice.CreateCall")?;
        let call = self.call_proxy(&path).await?;
        if let Err(e) = call.start().await {
            let _ = self.voice.delete_call(&path).await;
            return Err(anyhow::anyhow!("Call.Start failed: {e}"));
        }
        Ok(path)
    }

    pub async fn accept(&self, path: &OwnedObjectPath) -> Result<()> {
        self.call_proxy(path).await?.accept().await.context("Call.Accept")?;
        Ok(())
    }

    pub async fn hangup(&self, path: &OwnedObjectPath) -> Result<()> {
        let call = self.call_proxy(path).await?;
        // Hangup on an already terminated call is harmless but noisy.
        if let Err(e) = call.hangup().await {
            debug!(error = %e, "Call.Hangup failed (already gone?)");
        }
        let _ = self.voice.delete_call(path).await;
        Ok(())
    }

    pub async fn send_dtmf(&self, path: &OwnedObjectPath, digits: &str) -> Result<()> {
        let call = self.call_proxy(path).await?;
        for d in digits.chars().filter(|c| !c.is_whitespace()) {
            call.send_dtmf(&d.to_string()).await.context("Call.SendDtmf")?;
        }
        Ok(())
    }

    pub async fn sms_info(&self, path: &OwnedObjectPath) -> Result<SmsInfo> {
        let sms = self.sms_proxy(path).await?;
        Ok(SmsInfo {
            path: path.to_string(),
            state: sms.state().await.unwrap_or(sms_state::STATE_UNKNOWN),
            pdu_type: sms.pdu_type().await.unwrap_or(sms_state::PDU_UNKNOWN),
            number: sms.number().await.unwrap_or_default(),
            text: sms.text().await.unwrap_or_default(),
            data: sms.data().await.unwrap_or_default(),
            timestamp: sms.timestamp().await.ok().filter(|s| !s.is_empty()),
            smsc: sms.smsc().await.ok().filter(|s| !s.is_empty()),
            class: sms.class().await.unwrap_or(-1),
        })
    }

    /// Create and send a text SMS.  ModemManager takes care of the
    /// segmentation and of picking GSM7/UCS2.
    pub async fn send_sms(
        &self,
        number: &str,
        text: &str,
        delivery_report: bool,
    ) -> Result<OwnedObjectPath> {
        let mut props: HashMap<&str, Value<'_>> = HashMap::new();
        props.insert("number", Value::from(number));
        props.insert("text", Value::from(text));
        if delivery_report {
            props.insert("delivery-report-request", Value::from(true));
        }
        let path = self.messaging.create(props).await.context("Messaging.Create")?;
        let sms = self.sms_proxy(&path).await?;
        match sms.send().await {
            Ok(()) => Ok(path),
            Err(e) => {
                let _ = self.messaging.delete(&path).await;
                Err(anyhow::anyhow!("Sms.Send failed: {e}"))
            }
        }
    }

    pub async fn delete_sms(&self, path: &OwnedObjectPath) -> Result<()> {
        self.messaging.delete(path).await.context("Messaging.Delete")?;
        Ok(())
    }

    pub async fn list_sms(&self) -> Result<Vec<OwnedObjectPath>> {
        Ok(self.messaging.list().await?)
    }

    pub async fn list_calls(&self) -> Result<Vec<OwnedObjectPath>> {
        Ok(self.voice.list_calls().await?)
    }

    /// The connected data bearer's network interface and IPv4 address.
    ///
    /// MMS has to leave through the modem, and the address changes on every
    /// attach, so it is looked up when it is needed rather than configured.
    pub async fn data_bearer(&self) -> Option<BearerNet> {
        let bearers = self.modem.list_bearers().await.ok()?;
        for path in bearers {
            let Ok(bearer) = proxies::BearerProxy::builder(&self.conn)
                .path(path.clone())
                .ok()?
                .build()
                .await
            else {
                continue;
            };
            if !bearer.connected().await.unwrap_or(false) {
                continue;
            }
            let interface = bearer.interface().await.unwrap_or_default();
            if interface.is_empty() {
                continue;
            }
            let cfg = bearer.ip4_config().await.unwrap_or_default();
            let string_of = |key: &str| -> Option<String> {
                cfg.get(key).and_then(|v| String::try_from(v.try_clone().ok()?).ok())
            };
            let address = string_of("address").and_then(|s| s.parse().ok());
            let dns: Vec<std::net::IpAddr> = ["dns1", "dns2", "dns3"]
                .iter()
                .filter_map(|k| string_of(k))
                .filter_map(|s| s.parse().ok())
                .collect();
            debug!(interface, ?address, ?dns, "data bearer found for MMS");
            return Some(BearerNet { interface, address, dns });
        }
        None
    }
}

async fn read_info(
    conn: &Connection,
    path: &OwnedObjectPath,
    modem: &ModemProxy<'static>,
) -> ModemInfo {
    let mut info = ModemInfo {
        path: path.to_string(),
        device: modem.device().await.unwrap_or_default(),
        device_id: modem.device_identifier().await.unwrap_or_default(),
        equipment_id: modem.equipment_identifier().await.unwrap_or_default(),
        manufacturer: modem.manufacturer().await.unwrap_or_default(),
        model: modem.model().await.unwrap_or_default(),
        revision: modem.revision().await.unwrap_or_default(),
        primary_port: modem.primary_port().await.unwrap_or_default(),
        own_number: modem.own_numbers().await.ok().and_then(|v| v.into_iter().next()),
        ..Default::default()
    };

    // MMModemPortType: 3 = AT, 8 = audio.
    if let Ok(ports) = modem.ports().await {
        for (name, kind) in ports {
            match kind {
                3 => info.at_ports.push(format!("/dev/{name}")),
                8 => info.audio_ports.push(name),
                _ => {}
            }
        }
    }

    if let Ok(sim_path) = modem.sim().await {
        if sim_path.as_str() != "/" {
            if let Ok(sim) = build_sim(conn, sim_path).await {
                info.sim_id = sim.sim_identifier().await.ok().filter(|s| !s.is_empty());
                info.imsi = sim.imsi().await.ok().filter(|s| !s.is_empty());
                info.operator = sim.operator_name().await.ok().filter(|s| !s.is_empty());
            }
        }
    }
    if let Ok(m3gpp) = build_3gpp(conn, path.clone()).await {
        if info.equipment_id.is_empty() {
            info.equipment_id = m3gpp.imei().await.unwrap_or_default();
        }
        if let Ok(op) = m3gpp.operator_name().await {
            if !op.is_empty() {
                info.operator = Some(op);
            }
        }
    }
    info
}

async fn build_sim(conn: &Connection, path: OwnedObjectPath) -> Result<SimProxy<'static>> {
    Ok(SimProxy::builder(conn).path(path)?.build().await?)
}

async fn build_3gpp(conn: &Connection, path: OwnedObjectPath) -> Result<Modem3gppProxy<'static>> {
    Ok(Modem3gppProxy::builder(conn).path(path)?.build().await?)
}
