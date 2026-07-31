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
    // RFC 3261 §10.2: every registration for an address of record shares one
    // Call-ID with an increasing CSeq.  Registrars that key bindings on the
    // Call-ID accumulate a new binding per refresh otherwise.
    let call_id = format!("{}@modem2sip", auth::random_hex(10));
    let local_tag = auth::random_hex(6);
    // What we ask for, which a registrar with a higher minimum can raise.
    let mut asked_for = up.expires;

    loop {
        match register_once(&core, &up, &call_id, &local_tag, &mut asked_for).await {
            Ok(expires) => {
                backoff = Duration::from_secs(2);
                // Refresh at ~80% of the granted lifetime, but never after it
                // has already lapsed: a registrar that grants 20 s must be
                // refreshed inside 20 s, floor or no floor.  The floor is
                // still there to keep a registrar that grants something
                // absurdly short from turning this into a request flood.
                let granted = expires as u64;
                // The floor comes first and the cap last, or the floor wins
                // over "inside the lifetime" and a short grant lapses between
                // refreshes - the one thing the cap exists to prevent.
                let refresh = (granted * 4 / 5)
                    .clamp(5, 3600)
                    .min(granted.saturating_sub(1).max(1));
                info!(registrar = %up.registrar, expires, refresh, "registered upstream");
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

async fn register_once(
    core: &Arc<SipCore>,
    up: &crate::config::Upstream,
    call_id: &str,
    local_tag: &str,
    expires: &mut u32,
) -> Result<u32> {
    let registrar_uri =
        Uri::parse(&up.registrar).ok_or_else(|| anyhow!("invalid registrar URI"))?;
    let dest = transport::resolve_uri(&registrar_uri).await?;

    let aor_user = up.from_user.clone().unwrap_or_else(|| up.username.clone());
    let aor = Uri::new(Some(&aor_user), &registrar_uri.host, registrar_uri.port);
    let from = NameAddr::new(aor.clone()).with_tag(local_tag);
    let to = NameAddr::new(aor);

    let mut req = core.build_request(
        Method::Register,
        Uri::new(None, &registrar_uri.host, registrar_uri.port),
        from,
        to,
        call_id,
        core.next_cseq(),
        dest,
        false,
    );

    let ip = core.transport.advertised_ip(dest);
    let contact_user = up.contact_user.clone().unwrap_or(aor_user);
    let mut contact = Uri::new(Some(&contact_user), &ip.to_string(), Some(core.transport.port()));
    contact.set_param("transport", Some("udp"));
    let our_contact = contact.bare();
    req.headers.set("contact", NameAddr::new(contact).to_string());
    req.headers.set("expires", expires.to_string());
    req.headers.set("allow", "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, MESSAGE");

    let resp = core
        .transact(req, dest, Some((&up.username, &up.password)), Duration::from_secs(32))
        .await?;

    // RFC 3261 §10.2.8: the registrar states its minimum in Min-Expires and
    // expects the request again with at least that.  Treating it as a plain
    // failure meant a registrar with a floor above `sip.register.expires`
    // could never be registered with at all, only retried against forever.
    if resp.code == 423 {
        let min = resp
            .headers
            .get("min-expires")
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|m| *m > *expires)
            .ok_or_else(|| anyhow!("423 Interval Too Brief without a usable Min-Expires"))?;
        warn!(
            registrar = %up.registrar,
            asked_for = *expires,
            min,
            "the registrar wants a longer registration; using its minimum from now on"
        );
        *expires = min;
        return Err(anyhow!("retrying with the registrar's minimum expiry of {min}s"));
    }
    if !resp.is_success() {
        return Err(anyhow!("registrar replied {} {}", resp.code, resp.reason));
    }
    // A 200 OK lists every binding for the address of record, so the expiry
    // has to be read from *our* contact - a co-registered handset's longer
    // lifetime would otherwise let ours lapse.
    let granted = resp
        .headers
        .get_all("contact")
        .into_iter()
        .filter_map(NameAddr::parse)
        .find(|c| c.uri.bare() == our_contact)
        .and_then(|c| c.param("expires").and_then(|v| v.trim().parse::<u32>().ok()))
        .or_else(|| resp.headers.get("expires").and_then(|v| v.trim().parse::<u32>().ok()))
        .unwrap_or(*expires);
    if granted == 0 {
        // A 200 that grants nothing is a refusal dressed as success; backing
        // off is right, re-registering immediately is a flood.
        return Err(anyhow!("registrar granted a zero lifetime"));
    }
    Ok(granted)
}
