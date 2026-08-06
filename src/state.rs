//! State shared between the gateway, the SIP core and the HTTP API.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::RwLock;

use crate::config::Config;
use crate::db::Db;
use crate::mm::ModemHandle;
use crate::mms::MmsManager;

pub struct Shared {
    pub cfg: Arc<Config>,
    pub db: Db,
    pub mms: Arc<MmsManager>,
    /// The SIP element, once it exists.  Lets anything holding [`Shared`]
    /// tell a peer about something - the HTTP API announcing an MMS it has
    /// just fetched, for instance.
    sip: RwLock<Option<Arc<crate::sip::SipCore>>>,
    /// `Some` while the configured modem is present and usable.
    modem: RwLock<Option<Arc<ModemHandle>>>,
    /// Cheap, lock-free view of the above for the SIP fast path (503s).
    pub modem_ready: Arc<AtomicBool>,
}

impl Shared {
    pub fn new(cfg: Arc<Config>, db: Db, mms: Arc<MmsManager>) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            db,
            mms,
            sip: RwLock::new(None),
            modem: RwLock::new(None),
            modem_ready: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn set_sip(&self, core: Arc<crate::sip::SipCore>) {
        *self.sip.write().await = Some(core);
    }

    pub async fn sip(&self) -> Option<Arc<crate::sip::SipCore>> {
        self.sip.read().await.clone()
    }

    pub async fn set_modem(&self, handle: Option<Arc<ModemHandle>>) {
        let up = handle.is_some();
        // Going down, the flag has to lead so nothing new is accepted while
        // the handle is being dropped; coming up it has to trail, or the SIP
        // fast path lets a request through to a modem that is not stored yet.
        if !up {
            self.modem_ready.store(false, Ordering::Relaxed);
        }
        // MMS binds its HTTP traffic to the modem's data bearer, so it needs
        // the same handle.
        self.mms.set_modem(handle.clone()).await;
        *self.modem.write().await = handle;
        if up {
            self.modem_ready.store(true, Ordering::Relaxed);
        }
    }

    pub async fn modem(&self) -> Option<Arc<ModemHandle>> {
        self.modem.read().await.clone()
    }

    /// The number this line answers on, as reported by the SIM and otherwise
    /// as configured.  Used to address what arrives from the mobile side, so
    /// a SIP client can tell which line a call or message came in on.
    pub async fn own_number(&self) -> Option<String> {
        // An explicit setting is an override; the SIM is the fallback.
        let configured =
            self.cfg.modem.own_number.clone().filter(|n| !n.trim().is_empty());
        let number = match configured {
            Some(n) => Some(n),
            None => self
                .modem()
                .await
                .and_then(|m| m.info.own_number.clone())
                .filter(|n| !n.trim().is_empty()),
        }?;
        Some(self.local_number(&number))
    }

    /// A number as it is written where this line lives.
    ///
    /// Everything arriving from the mobile side goes through here - the
    /// caller of an incoming call, the sender of a message - so a SIP client
    /// sees `01012345678` rather than the international form the network
    /// uses, and can call or reply straight back.
    pub fn local_number(&self, number: &str) -> String {
        local_number(&self.cfg, number)
    }

    pub fn is_ready(&self) -> bool {
        self.modem_ready.load(Ordering::Relaxed)
    }

    /// Base URL used when handing attachment links to SIP peers.
    pub fn http_base_url(&self) -> String {
        self.cfg
            .http
            .base_url
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.cfg.http.bind))
    }
}

/// [`Shared::local_number`] for anything that holds the config but not the
/// shared state.
pub fn local_number(cfg: &Config, number: &str) -> String {
    to_national(number, cfg.modem.country_code.as_deref(), &cfg.modem.national_prefix)
}

/// Render a number the way people in that country write it.
///
/// SIMs report the own number in international format (`821012345678`), which
/// is not what a Korean subscriber recognises as their number, nor what the
/// network puts in the From of an incoming message (`01012345678`).  With the
/// country code known, the international prefix is swapped for the national
/// trunk prefix; without it the number is left exactly as it came.
pub fn to_national(number: &str, country_code: Option<&str>, national_prefix: &str) -> String {
    let number = number.trim();
    // Written either way round in the config - "82" or "+82" - and either way
    // round on the wire, since a network may or may not put the plus there.
    let Some(cc) = country_code
        .map(str::trim)
        .map(|c| c.strip_prefix('+').unwrap_or(c))
        .filter(|c| !c.is_empty())
    else {
        return number.to_string();
    };
    let digits = number.strip_prefix('+').unwrap_or(number);
    match digits.strip_prefix(cc) {
        // "82" alone, or a number that merely starts with those digits and is
        // too short to have a subscriber part, is not an international form.
        Some(rest) if !rest.is_empty() => format!("{national_prefix}{rest}"),
        _ => number.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::to_national;

    #[test]
    fn international_becomes_national() {
        assert_eq!(to_national("821012345678", Some("82"), "0"), "01012345678");
        assert_eq!(to_national("+821012345678", Some("82"), "0"), "01012345678");
        // Already national, or from somewhere else: left alone.
        assert_eq!(to_national("01012345678", Some("82"), "0"), "01012345678");
        assert_eq!(to_national("+4915112345678", Some("82"), "0"), "+4915112345678");
    }

    #[test]
    fn the_country_code_may_carry_a_plus() {
        // Config and wire, in every combination.
        assert_eq!(to_national("821012345678", Some("+82"), "0"), "01012345678");
        assert_eq!(to_national("+821012345678", Some("+82"), "0"), "01012345678");
        assert_eq!(to_national("+821012345678", Some("82"), "0"), "01012345678");
        assert_eq!(to_national("821012345678", Some("82"), "0"), "01012345678");
    }

    #[test]
    fn a_number_already_written_locally_is_left_alone() {
        // Applied twice over - a peer normalised on the way in and again on
        // the way out to SIP - must not shed another prefix.
        let once = to_national("+821012345678", Some("+82"), "0");
        assert_eq!(to_national(&once, Some("+82"), "0"), once);
        // Nor may a short code be mistaken for an international number.
        assert_eq!(to_national("106", Some("82"), "0"), "106");
    }

    #[test]
    fn without_a_country_code_nothing_changes() {
        assert_eq!(to_national("821012345678", None, "0"), "821012345678");
        assert_eq!(to_national("821012345678", Some(""), "0"), "821012345678");
    }

    #[test]
    fn a_plan_without_a_trunk_prefix() {
        // North America: +1 415 555 0100 is dialled as 4155550100.
        assert_eq!(to_national("+14155550100", Some("1"), ""), "4155550100");
    }

    #[test]
    fn the_country_code_alone_is_not_a_number() {
        assert_eq!(to_national("82", Some("82"), "0"), "82");
    }
}
