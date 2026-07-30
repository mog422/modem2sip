//! Optional outbound registration to an upstream registrar (PBX).
//!
//! Keeps retrying forever; a registrar that is down must never take the
//! gateway down with it.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tracing::{info, warn};

use super::auth;
use super::core::SipCore;
use super::message::Method;
use super::transport;
use super::uri::{NameAddr, Uri};

pub async fn run(core: Arc<SipCore>) {
    let Some(up) = core.cfg.sip.register.clone() else { return };
    let mut backoff = Duration::from_secs(2);

    loop {
        match register_once(&core, &up).await {
            Ok(expires) => {
                backoff = Duration::from_secs(2);
                // Refresh at ~80% of the granted lifetime.
                let refresh = (expires as u64).saturating_mul(4) / 5;
                let refresh = refresh.clamp(30, 3600);
                info!(registrar = %up.registrar, expires, "registered upstream");
                tokio::time::sleep(Duration::from_secs(refresh)).await;
            }
            Err(e) => {
                warn!(registrar = %up.registrar, error = %e, "upstream registration failed");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(300));
            }
        }
    }
}

async fn register_once(core: &Arc<SipCore>, up: &crate::config::Upstream) -> Result<u32> {
    let registrar_uri =
        Uri::parse(&up.registrar).ok_or_else(|| anyhow!("invalid registrar URI"))?;
    let dest = transport::resolve_uri(&registrar_uri).await?;

    let aor_user = up.from_user.clone().unwrap_or_else(|| up.username.clone());
    let aor = Uri::new(Some(&aor_user), &registrar_uri.host, registrar_uri.port);
    let from = NameAddr::new(aor.clone()).with_tag(&auth::random_hex(6));
    let to = NameAddr::new(aor);
    let call_id = format!("{}@modem2sip", auth::random_hex(10));

    let mut req = core.build_request(
        Method::Register,
        Uri::new(None, &registrar_uri.host, registrar_uri.port),
        from,
        to,
        &call_id,
        core.next_cseq(),
        dest,
        false,
    );

    let ip = core.transport.advertised_ip(dest);
    let contact_user = up.contact_user.clone().unwrap_or(aor_user);
    let mut contact = Uri::new(Some(&contact_user), &ip.to_string(), Some(core.transport.port()));
    contact.set_param("transport", Some("udp"));
    req.headers.set("contact", NameAddr::new(contact).to_string());
    req.headers.set("expires", up.expires.to_string());
    req.headers.set("allow", "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, MESSAGE");

    let resp = core
        .transact(req, dest, Some((&up.username, &up.password)), Duration::from_secs(32))
        .await?;

    if !resp.is_success() {
        return Err(anyhow!("registrar replied {} {}", resp.code, resp.reason));
    }
    let granted = resp
        .headers
        .get("expires")
        .and_then(|v| v.trim().parse::<u32>().ok())
        .or_else(|| {
            resp.headers
                .contact()
                .and_then(|c| c.param("expires").and_then(|v| v.parse().ok()))
        })
        .unwrap_or(up.expires);
    Ok(granted)
}
