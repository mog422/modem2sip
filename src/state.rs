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
            modem: RwLock::new(None),
            modem_ready: Arc::new(AtomicBool::new(false)),
        })
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
        let from_sim = self
            .modem()
            .await
            .and_then(|m| m.info.own_number.clone())
            .filter(|n| !n.trim().is_empty());
        from_sim.or_else(|| {
            self.cfg.modem.own_number.clone().filter(|n| !n.trim().is_empty())
        })
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
