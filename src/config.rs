//! Configuration file handling (TOML).
//!
//! One process serves exactly one modem; the modem is selected by the
//! `[modem]` matcher below.  Everything else has a usable default.

use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub general: General,
    pub modem: ModemMatch,
    pub audio: Audio,
    pub sip: Sip,
    pub rtp: Rtp,
    pub storage: Storage,
    pub sms: Sms,
    pub mms: Mms,
    pub http: Http,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct General {
    /// Tracing filter, e.g. "info", "modem2sip=debug,zbus=warn".
    pub log: String,
    /// Friendly name used in logs and in the HTTP API.
    pub name: String,
}

impl Default for General {
    fn default() -> Self {
        Self { log: "info".into(), name: "modem2sip".into() }
    }
}

/// How to pick *the* modem this process owns.
///
/// Every field that is set must match; unset fields are ignored.  With no
/// field set at all the first modem reported by ModemManager is used, which
/// is only sensible on single-modem systems.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ModemMatch {
    /// EquipmentIdentifier (IMEI/ESN).  Most stable identifier.
    pub imei: Option<String>,
    /// ModemManager `Device` property: the sysfs path of the physical device.
    /// Matched as a prefix, so `/sys/devices/.../usb1/1-1` works.
    pub device: Option<String>,
    /// ModemManager `DeviceIdentifier` property.
    pub device_id: Option<String>,
    /// Primary control port, e.g. `cdc-wdm0` or `ttyUSB2`.
    pub primary_port: Option<String>,
    /// SIM ICCID.
    pub sim_id: Option<String>,
    /// SIM IMSI.
    pub imsi: Option<String>,
    /// D-Bus object index (`/org/freedesktop/ModemManager1/Modem/<n>`).
    /// Unstable across replugs - prefer `imei` or `device`.
    pub index: Option<u32>,
    /// Enable the modem if ModemManager reports it as disabled.
    pub enable: bool,
    /// Own MSISDN. Used as the SIP user part for outbound notifications when
    /// the SIM does not report one.
    pub own_number: Option<String>,
    /// Shell command run every time the modem becomes ready (also after a
    /// replug).  The escape hatch for vendor knobs ModemManager does not
    /// expose.  The Quectel USB voice path is handled natively instead - see
    /// [`Audio::vendor_audio_setup`] and [`crate::vendor`].
    ///
    /// Exported: M2S_MODEM_PATH, M2S_DEVICE, M2S_IMEI, M2S_PRIMARY_PORT,
    /// M2S_AT_PORT, M2S_AT_PORTS, M2S_AUDIO_PORTS, M2S_ALSA_DEVICE.
    pub ready_command: Option<String>,
}

impl Default for ModemMatch {
    fn default() -> Self {
        Self {
            imei: None,
            device: None,
            device_id: None,
            primary_port: None,
            sim_id: None,
            imsi: None,
            index: None,
            // Enabling a disabled modem is what an unattended gateway wants.
            enable: true,
            own_number: None,
            ready_command: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Audio {
    /// Auto-detect the ALSA card that belongs to *this* modem by walking
    /// sysfs from the modem's `Device` path.  Strongly recommended when
    /// several modems of the same model are installed.
    pub auto: bool,
    /// Explicit ALSA device for both directions, e.g. "hw:2,0" or "plughw:2,0".
    pub device: Option<String>,
    /// Per-direction overrides (win over `device`).
    pub capture_device: Option<String>,
    pub playback_device: Option<String>,
    /// Substring matched against the ALSA card id/name when auto-detection
    /// finds several candidates (e.g. "Quectel", "LTE Module").
    pub card_hint: Option<String>,
    /// Sample rate of the modem card.  Quectel UAC cards are 8000 or 16000.
    /// The negotiated RTP rate is always 8000; resampling is automatic.
    pub rate: u32,
    /// Packetisation / ALSA period in milliseconds.
    pub period_ms: u32,
    /// ALSA ring buffer size in periods.
    pub periods: u32,
    /// Linear gain applied modem->SIP and SIP->modem.
    pub tx_gain: f32,
    pub rx_gain: f32,
    /// Honour the `AudioPort`/`AudioFormat` properties of the MM call when
    /// present (some drivers hand out the ALSA device that way).
    pub use_mm_audio_port: bool,
    /// Switch the modem's USB voice path on by itself.  Quectel modems need
    /// `AT+QPCMV=1,2` or every call is silent, and ModemManager has no API
    /// for it - see [`crate::vendor`].
    /// "auto" (Quectel only), "always", or "never".
    pub vendor_audio_setup: crate::vendor::VendorAudioSetup,
}

impl Default for Audio {
    fn default() -> Self {
        Self {
            auto: true,
            device: None,
            capture_device: None,
            playback_device: None,
            card_hint: None,
            rate: 8000,
            period_ms: 20,
            periods: 4,
            tx_gain: 1.0,
            rx_gain: 1.0,
            use_mm_audio_port: true,
            vendor_audio_setup: crate::vendor::VendorAudioSetup::Auto,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Sip {
    /// UDP socket the built-in SIP server listens on.
    pub bind: String,
    /// Address advertised in Contact/Via/SDP.  Defaults to the bind address,
    /// or to the local address of the peer's route when that is 0.0.0.0.
    pub public_ip: Option<String>,
    /// Realm used for digest authentication and as the default URI host.
    pub domain: Option<String>,
    pub user_agent: String,
    /// When set, inbound INVITE/MESSAGE must authenticate with these
    /// credentials (digest, MD5).
    pub auth: Option<SipAuth>,
    /// Where inbound (modem -> SIP) calls are sent.  If unset the most
    /// recently registered contact is used.
    pub call_target: Option<String>,
    /// Where SMS/MMS notifications are sent.  Falls back to `call_target`
    /// and then to the registered contact.
    pub sms_target: Option<String>,
    /// Optional source-IP allow list (plain addresses or CIDR).  Empty = any.
    pub allow: Vec<String>,
    /// Optional outbound registration to an upstream registrar (Asterisk,
    /// Kamailio, ...).  The built-in server keeps listening either way.
    pub register: Option<Upstream>,
    /// Answer alerting with `183 Session Progress` and SDP instead of a bare
    /// `180 Ringing`, and open the audio path straight away, so the caller
    /// hears what the network is actually playing: its ringback tone, the
    /// operator's announcements ("the number you have dialled is not in
    /// service"), and IVRs that answer with early media.
    ///
    /// Turn it off to have the caller's own phone generate local ringback.
    pub early_media: bool,
    /// How long an outbound INVITE may ring before it is abandoned.
    pub ring_timeout_secs: u64,
    /// User part used in the From header of gateway-originated requests.
    /// Defaults to the modem's own number.
    pub from_user: Option<String>,
    /// Reply 503 (instead of 200) to OPTIONS while the modem is unusable.
    /// Handy as a health probe for the upstream proxy.
    pub options_reflect_modem: bool,
    /// Retry-After value sent with 503 responses.
    pub retry_after_secs: u32,
}

impl Default for Sip {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:5060".into(),
            public_ip: None,
            domain: None,
            user_agent: concat!("modem2sip/", env!("CARGO_PKG_VERSION")).into(),
            auth: None,
            call_target: None,
            sms_target: None,
            allow: Vec::new(),
            register: None,
            early_media: true,
            ring_timeout_secs: 60,
            from_user: None,
            options_reflect_modem: true,
            retry_after_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SipAuth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Upstream {
    /// e.g. "sip:pbx.example.net" or "sip:pbx.example.net:5060".
    pub registrar: String,
    pub username: String,
    pub password: String,
    pub expires: u32,
    /// Defaults to `username`.
    pub from_user: Option<String>,
    pub contact_user: Option<String>,
}

impl Default for Upstream {
    fn default() -> Self {
        Self {
            registrar: String::new(),
            username: String::new(),
            password: String::new(),
            expires: 300,
            from_user: None,
            contact_user: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Rtp {
    /// Address RTP sockets bind to.  Defaults to the SIP bind address.
    pub bind: Option<String>,
    pub port_min: u16,
    pub port_max: u16,
    /// Target jitter buffer depth in milliseconds.
    pub jitter_ms: u32,
    /// Payload type offered for RFC2833 telephone-event.
    pub dtmf_payload_type: u8,
    /// Latch the remote RTP address from the first received packet
    /// (symmetric RTP / NAT traversal).
    pub symmetric: bool,
    /// How digits received from SIP reach the far end.
    ///
    /// * `auto`   - ask ModemManager first, fall back to in-band tones when
    ///   it fails (VoLTE calls usually need the fallback)
    /// * `modem`   - `Call.SendDtmf` only
    /// * `inband`  - generate the tones in the uplink audio only
    /// * `none`    - drop them
    pub dtmf_method: DtmfMethod,
    /// Length of a generated in-band digit and the silence after it.
    pub dtmf_tone_ms: u32,
    pub dtmf_gap_ms: u32,
    /// Listen for DTMF tones in the audio coming from the mobile network and
    /// relay them to SIP as INFO.  `Call.DtmfReceived` is unreliable on the
    /// modems that need in-band DTMF in the first place.
    pub detect_inband_dtmf: bool,
    /// End a call whose SIP peer has sent no RTP for this many seconds.
    ///
    /// A peer that dies without a BYE - a PBX restarted, a network that went
    /// away - leaves the mobile leg connected and billed with nothing on
    /// either side to notice.  0 disables the check.
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DtmfMethod {
    #[default]
    Auto,
    Modem,
    Inband,
    None,
}

impl Default for Rtp {
    fn default() -> Self {
        Self {
            bind: None,
            port_min: 16384,
            port_max: 16584,
            jitter_ms: 60,
            dtmf_payload_type: 101,
            symmetric: true,
            dtmf_method: DtmfMethod::Auto,
            dtmf_tone_ms: 180,
            dtmf_gap_ms: 80,
            detect_inband_dtmf: true,
            timeout_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Storage {
    /// Base directory for the SQLite database and MMS attachments.
    pub dir: PathBuf,
    /// Database file name (relative to `dir` unless absolute).
    pub db: PathBuf,
}

impl Default for Storage {
    fn default() -> Self {
        Self { dir: PathBuf::from("./data"), db: PathBuf::from("modem2sip.db") }
    }
}

impl Storage {
    pub fn db_path(&self) -> PathBuf {
        if self.db.is_absolute() {
            self.db.clone()
        } else {
            self.dir.join(&self.db)
        }
    }
    pub fn attachments_dir(&self) -> PathBuf {
        self.dir.join("attachments")
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Sms {
    /// Delete the message from modem/SIM storage once it is safely in SQLite.
    pub delete_from_modem: bool,
    /// Forward received messages to SIP as a MESSAGE request.
    pub notify_sip: bool,
    /// Request a delivery report for outgoing messages.
    pub delivery_report: bool,
}

impl Default for Sms {
    fn default() -> Self {
        Self { delete_from_modem: true, notify_sip: true, delivery_report: false }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Mms {
    pub enabled: bool,
    /// MMSC URL, e.g. "http://mmsc.example.net:8002/mms" (carrier specific).
    pub mmsc: Option<String>,
    /// WAP gateway / HTTP proxy, "host:port".
    pub proxy: Option<String>,
    /// Bind MMS traffic to this network interface (SO_BINDTODEVICE, needs
    /// CAP_NET_RAW or root).  Usually the modem's WWAN interface.
    pub interface: Option<String>,
    /// Bind MMS traffic to this source address instead / additionally.
    pub local_ip: Option<IpAddr>,
    /// DNS servers used for MMSC host names, queried over the modem.  Empty
    /// means "whatever the data bearer reports", which is almost always what
    /// you want - MMSC names often do not exist in public DNS.
    pub dns: Vec<IpAddr>,
    /// Automatically fetch the message body when a WAP-push notification
    /// arrives.  When false only the notification is stored.
    pub auto_retrieve: bool,
    /// Refuse to download anything larger than this (bytes).
    pub max_size: usize,
    pub user_agent: String,
    /// UAProf URL sent as x-wap-profile.
    pub ua_profile: Option<String>,
    /// Optional shell command executed before MMS traffic (route/APN setup).
    pub setup_command: Option<String>,
    pub timeout_secs: u64,
}

impl Default for Mms {
    fn default() -> Self {
        Self {
            enabled: false,
            mmsc: None,
            proxy: None,
            interface: None,
            local_ip: None,
            dns: Vec::new(),
            auto_retrieve: true,
            max_size: 2 * 1024 * 1024,
            user_agent: concat!("modem2sip/", env!("CARGO_PKG_VERSION")).into(),
            ua_profile: None,
            setup_command: None,
            timeout_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Http {
    /// Local control/attachment API.  Also serves the URLs referenced by the
    /// simplified MMS notifications sent over SIP.
    pub enabled: bool,
    pub bind: String,
    /// Externally reachable base URL used when building attachment links.
    pub base_url: Option<String>,
    /// Optional bearer token required for every request.
    pub token: Option<String>,
}

impl Default for Http {
    fn default() -> Self {
        Self { enabled: true, bind: "127.0.0.1:8088".into(), base_url: None, token: None }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config = toml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        self.sip
            .bind
            .parse::<std::net::SocketAddr>()
            .with_context(|| format!("sip.bind is not a socket address: {}", self.sip.bind))?;
        if self.http.enabled {
            // Checked here rather than in the HTTP task, where a typo used to
            // leave the process running with no API while SIP kept handing
            // out attachment URLs that pointed at it.
            self.http.bind.parse::<std::net::SocketAddr>().with_context(|| {
                format!("http.bind is not a socket address: {}", self.http.bind)
            })?;
            // An empty token would compare equal to the empty string a
            // request with no Authorization header presents, i.e. it would
            // read as "authentication configured" while allowing everything.
            if let Some(token) = &self.http.token {
                anyhow::ensure!(!token.is_empty(), "http.token must not be empty");
            }
        }
        anyhow::ensure!(self.rtp.port_min < self.rtp.port_max, "rtp.port_min must be < rtp.port_max");
        anyhow::ensure!(self.rtp.port_min >= 1024, "rtp.port_min must be >= 1024");
        anyhow::ensure!(
            (96..=127).contains(&self.rtp.dtmf_payload_type),
            "rtp.dtmf_payload_type must be a dynamic type (96..127)"
        );
        anyhow::ensure!(self.sip.ring_timeout_secs > 0, "sip.ring_timeout_secs must be > 0");
        // The upper bounds are what keeps the ALSA period and buffer sizes
        // inside a C long, which is 32 bits wide on the OpenWrt targets.
        anyhow::ensure!(
            (1..=200).contains(&self.audio.period_ms),
            "audio.period_ms must be between 1 and 200"
        );
        anyhow::ensure!(
            (8000..=192_000).contains(&self.audio.rate),
            "audio.rate must be between 8000 and 192000"
        );
        anyhow::ensure!(
            (2..=32).contains(&self.audio.periods),
            "audio.periods must be between 2 and 32"
        );
        for (what, gain) in [("tx_gain", self.audio.tx_gain), ("rx_gain", self.audio.rx_gain)] {
            anyhow::ensure!(
                gain.is_finite() && (0.0..=16.0).contains(&gain),
                "audio.{what} must be between 0 and 16"
            );
        }
        if self.mms.enabled {
            anyhow::ensure!(self.mms.mmsc.is_some(), "mms.enabled requires mms.mmsc");
            anyhow::ensure!(self.mms.max_size > 0, "mms.max_size must be > 0");
        }
        if let Some(up) = &self.sip.register {
            anyhow::ensure!(!up.registrar.is_empty(), "sip.register.registrar is required");
        }
        Ok(())
    }

    /// The address the SIP server binds to.
    pub fn sip_bind(&self) -> std::net::SocketAddr {
        // `validate` runs on every loaded config; a default-constructed one
        // (tests, future --check mode) must still not bring the process down.
        self.sip.bind.parse().unwrap_or_else(|_| {
            std::net::SocketAddr::from(([0, 0, 0, 0], 5060))
        })
    }
}
