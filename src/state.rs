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
        self.modem_ready.store(handle.is_some(), Ordering::Relaxed);
        // MMS binds its HTTP traffic to the modem's data bearer, so it needs
        // the same handle.
        self.mms.set_modem(handle.clone()).await;
        *self.modem.write().await = handle;
    }

    pub async fn modem(&self) -> Option<Arc<ModemHandle>> {
        self.modem.read().await.clone()
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
