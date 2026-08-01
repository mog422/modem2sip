//! Typed D-Bus proxies for the parts of ModemManager we use.
//!
//! Everything the gateway does to the modem goes through these interfaces -
//! there is no AT command handling anywhere in this crate.

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;

use zbus::proxy;
use zvariant::{OwnedObjectPath, OwnedValue, Value};

pub const MM_SERVICE: &str = "org.freedesktop.ModemManager1";
pub const MM_PATH: &str = "/org/freedesktop/ModemManager1";

// ---------------------------------------------------------------- constants

// The numbering below is ModemManager's own, transcribed whole rather
// than trimmed to what is used today: a half-copied enum is worse than
// no copy when the next state has to be handled.

#[allow(dead_code)]
pub mod state {
    pub const MODEM_FAILED: i32 = -1;
    pub const MODEM_UNKNOWN: i32 = 0;
    pub const MODEM_INITIALIZING: i32 = 1;
    pub const MODEM_LOCKED: i32 = 2;
    pub const MODEM_DISABLED: i32 = 3;
    pub const MODEM_DISABLING: i32 = 4;
    pub const MODEM_ENABLING: i32 = 5;
    pub const MODEM_ENABLED: i32 = 6;
    pub const MODEM_SEARCHING: i32 = 7;
    pub const MODEM_REGISTERED: i32 = 8;
    pub const MODEM_DISCONNECTING: i32 = 9;
    pub const MODEM_CONNECTING: i32 = 10;
    pub const MODEM_CONNECTED: i32 = 11;

    pub fn modem_state_name(s: i32) -> &'static str {
        match s {
            -1 => "failed",
            0 => "unknown",
            1 => "initializing",
            2 => "locked",
            3 => "disabled",
            4 => "disabling",
            5 => "enabling",
            6 => "enabled",
            7 => "searching",
            8 => "registered",
            9 => "disconnecting",
            10 => "connecting",
            11 => "connected",
            _ => "?",
        }
    }
}

#[allow(dead_code)]
pub mod call {
    pub const UNKNOWN: i32 = 0;
    pub const DIALING: i32 = 1;
    pub const RINGING_OUT: i32 = 2;
    pub const RINGING_IN: i32 = 3;
    pub const ACTIVE: i32 = 4;
    pub const HELD: i32 = 5;
    pub const WAITING: i32 = 6;
    pub const TERMINATED: i32 = 7;

    pub const DIR_UNKNOWN: i32 = 0;
    pub const DIR_INCOMING: i32 = 1;
    pub const DIR_OUTGOING: i32 = 2;

    pub fn state_name(s: i32) -> &'static str {
        match s {
            1 => "dialing",
            2 => "ringing-out",
            3 => "ringing-in",
            4 => "active",
            5 => "held",
            6 => "waiting",
            7 => "terminated",
            _ => "unknown",
        }
    }

    /// MMCallStateReason - only the ones we map to SIP status codes.
    pub const REASON_UNKNOWN: u32 = 0;
    pub const REASON_OUTGOING_STARTED: u32 = 1;
    pub const REASON_INCOMING_NEW: u32 = 2;
    pub const REASON_ACCEPTED: u32 = 3;
    pub const REASON_TERMINATED: u32 = 4;
    pub const REASON_REFUSED_OR_BUSY: u32 = 5;
    pub const REASON_ERROR: u32 = 6;
    pub const REASON_AUDIO_SETUP_FAILED: u32 = 7;
    pub const REASON_TRANSFERRED: u32 = 8;
    pub const REASON_DEFLECTED: u32 = 9;
}

#[allow(dead_code)]
pub mod sms {
    pub const STATE_UNKNOWN: u32 = 0;
    pub const STATE_STORED: u32 = 1;
    pub const STATE_RECEIVING: u32 = 2;
    pub const STATE_RECEIVED: u32 = 3;
    pub const STATE_SENDING: u32 = 4;
    pub const STATE_SENT: u32 = 5;

    pub const PDU_UNKNOWN: u32 = 0;
    pub const PDU_DELIVER: u32 = 1;
    pub const PDU_SUBMIT: u32 = 2;
    pub const PDU_STATUS_REPORT: u32 = 3;
}

// ------------------------------------------------------------------ proxies

#[proxy(
    interface = "org.freedesktop.ModemManager1.Modem",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Modem {
    fn enable(&self, enable: bool) -> zbus::Result<()>;
    fn list_bearers(&self) -> zbus::Result<Vec<OwnedObjectPath>>;
    fn create_bearer(&self, properties: HashMap<&str, Value<'_>>) -> zbus::Result<OwnedObjectPath>;
    fn delete_bearer(&self, bearer: &OwnedObjectPath) -> zbus::Result<()>;
    fn set_power_state(&self, state: u32) -> zbus::Result<()>;
    /// Send an AT command through ModemManager's own channel, so it is
    /// serialised with whatever else MM is doing on that port.  Refused with
    /// `Unauthorized` unless ModemManager was started with `--debug`.
    fn command(&self, cmd: &str, timeout: u32) -> zbus::Result<String>;

    /// sysfs path of the physical device, e.g.
    /// `/sys/devices/pci0000:00/0000:00:14.0/usb1/1-3`.
    #[zbus(property)]
    fn device(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn device_identifier(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn drivers(&self) -> zbus::Result<Vec<String>>;
    #[zbus(property)]
    fn equipment_identifier(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn manufacturer(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn model(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn revision(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn own_numbers(&self) -> zbus::Result<Vec<String>>;
    #[zbus(property)]
    fn primary_port(&self) -> zbus::Result<String>;
    /// (port name, MMModemPortType)
    #[zbus(property)]
    fn ports(&self) -> zbus::Result<Vec<(String, u32)>>;
    #[zbus(property(emits_changed_signal = "false"))]
    fn state(&self) -> zbus::Result<i32>;
    #[zbus(property)]
    fn state_failed_reason(&self) -> zbus::Result<u32>;
    #[zbus(property(emits_changed_signal = "false"))]
    fn signal_quality(&self) -> zbus::Result<(u32, bool)>;
    #[zbus(property)]
    fn sim(&self) -> zbus::Result<OwnedObjectPath>;
    #[zbus(property)]
    fn unlock_required(&self) -> zbus::Result<u32>;
    #[zbus(property)]
    fn supported_capabilities(&self) -> zbus::Result<Vec<u32>>;

    /// Named `modem_state_changed` because the generated stream for the
    /// `State` property would otherwise collide with it.  The argument names
    /// are ours (D-Bus signal arguments are positional).
    #[zbus(signal, name = "StateChanged")]
    fn modem_state_changed(
        &self,
        old_state: i32,
        new_state: i32,
        reason: u32,
    ) -> zbus::Result<()>;
}

#[proxy(
    interface = "org.freedesktop.ModemManager1.Modem.Modem3gpp",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Modem3gpp {
    #[zbus(property)]
    fn imei(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn operator_name(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn operator_code(&self) -> zbus::Result<String>;
    #[zbus(property(emits_changed_signal = "false"))]
    fn registration_state(&self) -> zbus::Result<u32>;
}

#[proxy(
    interface = "org.freedesktop.ModemManager1.Sim",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Sim {
    #[zbus(property)]
    fn sim_identifier(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn imsi(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn operator_name(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn operator_identifier(&self) -> zbus::Result<String>;
}

#[proxy(
    interface = "org.freedesktop.ModemManager1.Modem.Voice",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Voice {
    fn list_calls(&self) -> zbus::Result<Vec<OwnedObjectPath>>;
    fn create_call(&self, properties: HashMap<&str, Value<'_>>) -> zbus::Result<OwnedObjectPath>;
    fn delete_call(&self, path: &OwnedObjectPath) -> zbus::Result<()>;
    fn hangup_all(&self) -> zbus::Result<()>;

    #[zbus(property)]
    fn calls(&self) -> zbus::Result<Vec<OwnedObjectPath>>;
    #[zbus(property)]
    fn emergency_only(&self) -> zbus::Result<bool>;

    #[zbus(signal)]
    fn call_added(&self, path: OwnedObjectPath) -> zbus::Result<()>;
    #[zbus(signal)]
    fn call_deleted(&self, path: OwnedObjectPath) -> zbus::Result<()>;
}

/// The `Call` interface lives in its own module: the proxy macro names the
/// types it generates for a signal after the *D-Bus* signal name, and both
/// `Modem` and `Call` have a `StateChanged` signal.  Two `StateChangedArgs`
/// in one module do not compile.
mod call_iface {
    use super::*;

#[proxy(
    interface = "org.freedesktop.ModemManager1.Call",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Call {
    /// Place an outgoing call (only valid right after CreateCall).
    fn start(&self) -> zbus::Result<()>;
    /// Answer an incoming call.
    fn accept(&self) -> zbus::Result<()>;
    fn hangup(&self) -> zbus::Result<()>;
    fn send_dtmf(&self, dtmf: &str) -> zbus::Result<()>;
    fn deflect(&self, number: &str) -> zbus::Result<()>;

    #[zbus(property(emits_changed_signal = "false"))]
    fn state(&self) -> zbus::Result<i32>;
    #[zbus(property(emits_changed_signal = "false"))]
    fn state_reason(&self) -> zbus::Result<i32>;
    #[zbus(property)]
    fn direction(&self) -> zbus::Result<i32>;
    #[zbus(property)]
    fn number(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn multiparty(&self) -> zbus::Result<bool>;
    /// Some drivers hand out the audio device here (e.g. an ALSA device or a
    /// serial port carrying PCM).  Empty on modems with a real sound card.
    #[zbus(property(emits_changed_signal = "false"))]
    fn audio_port(&self) -> zbus::Result<String>;
    #[zbus(property(emits_changed_signal = "false"))]
    fn audio_format(&self) -> zbus::Result<HashMap<String, OwnedValue>>;

    /// See the note on `Modem.modem_state_changed`.
    #[zbus(signal, name = "StateChanged")]
    fn call_state_changed(
        &self,
        old_state: i32,
        new_state: i32,
        reason: u32,
    ) -> zbus::Result<()>;
    #[zbus(signal)]
    fn dtmf_received(&self, dtmf: String) -> zbus::Result<()>;
}

} // mod call_iface

pub use call_iface::CallProxy;

#[proxy(
    interface = "org.freedesktop.ModemManager1.Modem.Messaging",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Messaging {
    fn list(&self) -> zbus::Result<Vec<OwnedObjectPath>>;
    fn create(&self, properties: HashMap<&str, Value<'_>>) -> zbus::Result<OwnedObjectPath>;
    fn delete(&self, path: &OwnedObjectPath) -> zbus::Result<()>;

    #[zbus(property)]
    fn messages(&self) -> zbus::Result<Vec<OwnedObjectPath>>;
    #[zbus(property)]
    fn default_storage(&self) -> zbus::Result<u32>;

    #[zbus(signal)]
    fn added(&self, path: OwnedObjectPath, received: bool) -> zbus::Result<()>;
    #[zbus(signal)]
    fn deleted(&self, path: OwnedObjectPath) -> zbus::Result<()>;
}

#[proxy(
    interface = "org.freedesktop.ModemManager1.Sms",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Sms {
    fn send(&self) -> zbus::Result<()>;
    fn store(&self, storage: u32) -> zbus::Result<()>;

    #[zbus(property(emits_changed_signal = "false"))]
    fn state(&self) -> zbus::Result<u32>;
    #[zbus(property)]
    fn pdu_type(&self) -> zbus::Result<u32>;
    #[zbus(property)]
    fn number(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn text(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn data(&self) -> zbus::Result<Vec<u8>>;
    #[zbus(property)]
    fn smsc(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn timestamp(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn discharge_timestamp(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn class(&self) -> zbus::Result<i32>;
    #[zbus(property)]
    fn message_reference(&self) -> zbus::Result<u32>;
    #[zbus(property)]
    fn delivery_report_request(&self) -> zbus::Result<bool>;
    #[zbus(property)]
    fn storage(&self) -> zbus::Result<u32>;
}

#[proxy(
    interface = "org.freedesktop.ModemManager1.Bearer",
    default_service = "org.freedesktop.ModemManager1"
)]
pub trait Bearer {
    fn connect(&self) -> zbus::Result<()>;
    fn disconnect(&self) -> zbus::Result<()>;

    #[zbus(property(emits_changed_signal = "false"))]
    fn connected(&self) -> zbus::Result<bool>;
    #[zbus(property)]
    fn interface(&self) -> zbus::Result<String>;
    #[zbus(property(emits_changed_signal = "false"))]
    fn ip4_config(&self) -> zbus::Result<HashMap<String, OwnedValue>>;
    #[zbus(property)]
    fn properties(&self) -> zbus::Result<HashMap<String, OwnedValue>>;
}
