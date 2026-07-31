//! A tiny registrar so softphones can bind to the gateway directly.
//!
//! Bindings are kept in memory only; the gateway targets the freshest one
//! when it needs to deliver a call or a message and no static target is
//! configured.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::uri::NameAddr;

#[derive(Debug, Clone)]
pub struct Binding {
    pub contact: NameAddr,
    pub source: SocketAddr,
    pub expires_at: Instant,
    pub registered_at: Instant,
}

#[derive(Default)]
pub struct Registrar {
    bindings: Mutex<HashMap<String, Vec<Binding>>>,
}

impl Registrar {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add/refresh/remove a binding.  `expires == 0` removes it.
    pub fn update(&self, aor: &str, contact: NameAddr, source: SocketAddr, expires: u32) {
        let mut map = self.bindings.lock().unwrap();
        let list = map.entry(aor.to_string()).or_default();
        let key = contact.uri.bare().to_string();
        list.retain(|b| b.contact.uri.bare().to_string() != key);
        if expires > 0 {
            list.push(Binding {
                contact,
                source,
                expires_at: Instant::now() + Duration::from_secs(expires as u64),
                registered_at: Instant::now(),
            });
        }
        if list.is_empty() {
            map.remove(aor);
        }
    }

    /// Wildcard de-registration (`Contact: *` with `Expires: 0`).
    pub fn remove_all(&self, aor: &str) {
        self.bindings.lock().unwrap().remove(aor);
    }

    pub fn contacts(&self, aor: &str) -> Vec<Binding> {
        let mut map = self.bindings.lock().unwrap();
        Self::expire(&mut map);
        map.get(aor).cloned().unwrap_or_default()
    }

    /// The most recently refreshed binding across every AOR.
    pub fn newest(&self) -> Option<Binding> {
        let mut map = self.bindings.lock().unwrap();
        Self::expire(&mut map);
        map.values()
            .flatten()
            .max_by_key(|b| b.registered_at)
            .cloned()
    }

    fn expire(map: &mut HashMap<String, Vec<Binding>>) {
        let now = Instant::now();
        map.retain(|_, list| {
            list.retain(|b| b.expires_at > now);
            !list.is_empty()
        });
    }
}
