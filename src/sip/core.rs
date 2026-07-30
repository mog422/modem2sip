//! The SIP element: UDP server loop, registrar, client transactions.
//!
//! Deliberately small.  Requests that belong to the gateway (INVITE, ACK,
//! BYE, CANCEL, INFO, MESSAGE) are handed to [`crate::gateway`] through a
//! channel; REGISTER and OPTIONS are answered here.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

use crate::config::Config;

use super::auth::{self, Challenge, Credentials, NonceFactory};
use super::message::{reason_phrase, Headers, Message, Method, Request, Response, Via};
use super::registrar::Registrar;
use super::transport::{self, Transport, MAX_DATAGRAM};
use super::uri::{NameAddr, Uri};

/// Requests the gateway state machine has to deal with.
#[derive(Debug)]
pub enum SipEvent {
    Request { req: Request, src: SocketAddr },
}

struct ClientTxnEntry {
    tx: mpsc::UnboundedSender<Response>,
    stop_retransmit: Arc<AtomicBool>,
}

/// Handle for an outstanding client transaction.
pub struct ClientTxn {
    pub branch: String,
    pub rx: mpsc::UnboundedReceiver<Response>,
    core: Arc<SipCore>,
    stop: Arc<AtomicBool>,
}

impl ClientTxn {
    /// Wait for the next response, giving up after `timeout`.
    pub async fn next_response(&mut self, timeout: Duration) -> Option<Response> {
        tokio::time::timeout(timeout, self.rx.recv()).await.ok().flatten()
    }

    /// Wait for the final response of a non-INVITE transaction.
    pub async fn final_response(&mut self, timeout: Duration) -> Option<Response> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match self.next_response(remaining).await {
                Some(r) if r.is_provisional() => continue,
                other => return other,
            }
        }
    }
}

impl Drop for ClientTxn {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.core.client_txns.lock().unwrap().remove(&self.branch);
    }
}

pub struct SipCore {
    pub cfg: Arc<Config>,
    pub transport: Transport,
    pub registrar: Registrar,
    nonce: NonceFactory,
    client_txns: Mutex<HashMap<String, ClientTxnEntry>>,
    /// Cached final responses for retransmission detection (branch+method).
    server_cache: Mutex<HashMap<String, (Vec<u8>, Instant)>>,
    events: mpsc::Sender<SipEvent>,
    /// Whether the modem is usable right now; drives 503 responses.
    modem_ready: Arc<AtomicBool>,
    cseq: AtomicU32,
}

impl SipCore {
    pub async fn new(
        cfg: Arc<Config>,
        events: mpsc::Sender<SipEvent>,
        modem_ready: Arc<AtomicBool>,
    ) -> Result<Arc<Self>> {
        let public_ip = cfg
            .sip
            .public_ip
            .as_deref()
            .map(|s| s.parse::<IpAddr>())
            .transpose()
            .context("sip.public_ip is not an IP address")?;
        let transport = Transport::bind(cfg.sip_bind(), public_ip).await?;
        info!(addr = %transport.bound(), "SIP listening (UDP)");
        Ok(Arc::new(Self {
            cfg,
            transport,
            registrar: Registrar::new(),
            nonce: NonceFactory::new(),
            client_txns: Mutex::new(HashMap::new()),
            server_cache: Mutex::new(HashMap::new()),
            events,
            modem_ready,
            cseq: AtomicU32::new(1),
        }))
    }

    pub fn modem_ready(&self) -> bool {
        self.modem_ready.load(Ordering::Relaxed)
    }

    pub fn next_cseq(&self) -> u32 {
        self.cseq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn domain(&self) -> String {
        self.cfg
            .sip
            .domain
            .clone()
            .unwrap_or_else(|| self.transport.bound().ip().to_string())
    }

    /// Receive loop.  Never returns unless the socket dies.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            let (len, src) = match self.transport.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "SIP socket read failed");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
            };
            let data = &buf[..len];
            // Keep-alive pings (CRLFCRLF) and other noise.
            if data.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            trace!(%src, "<<<\n{}", String::from_utf8_lossy(data));
            let Some(msg) = Message::parse(data) else {
                debug!(%src, "unparseable SIP datagram, dropped");
                continue;
            };
            match msg {
                Message::Response(resp) => self.on_response(resp),
                Message::Request(req) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = this.on_request(req, src).await {
                            warn!(error = %e, "handling SIP request failed");
                        }
                    });
                }
            }
            self.gc();
        }
    }

    fn gc(&self) {
        let mut cache = self.server_cache.lock().unwrap();
        if cache.len() > 256 {
            let cutoff = Instant::now() - Duration::from_secs(64);
            cache.retain(|_, (_, t)| *t > cutoff);
        }
    }

    fn on_response(&self, resp: Response) {
        let Some(branch) = resp.headers.top_via().and_then(|v| v.branch().map(str::to_string))
        else {
            debug!("response without Via branch, dropped");
            return;
        };
        let map = self.client_txns.lock().unwrap();
        match map.get(&branch) {
            Some(entry) => {
                entry.stop_retransmit.store(true, Ordering::Relaxed);
                let _ = entry.tx.send(resp);
            }
            None => debug!(%branch, code = resp.code, "response for unknown transaction"),
        }
    }

    async fn on_request(self: &Arc<Self>, req: Request, src: SocketAddr) -> Result<()> {
        if !self.source_allowed(src.ip()) {
            warn!(%src, "request from disallowed source, 403");
            self.respond(&req, src, self.make_response(&req, 403, None)).await?;
            return Ok(());
        }

        // Retransmission of a request we already answered?
        if req.method != Method::Ack {
            if let Some(key) = txn_key(&req) {
                let cached = self.server_cache.lock().unwrap().get(&key).map(|(b, _)| b.clone());
                if let Some(bytes) = cached {
                    debug!(%key, "retransmitted request, replaying cached response");
                    self.transport.send(&bytes, src).await?;
                    return Ok(());
                }
            }
        }

        match req.method {
            Method::Register => self.handle_register(req, src).await,
            Method::Options => self.handle_options(req, src).await,
            Method::Invite
            | Method::Ack
            | Method::Bye
            | Method::Cancel
            | Method::Info
            | Method::Message => {
                if req.method != Method::Ack && !self.check_auth(&req, src).await? {
                    return Ok(());
                }
                self.events
                    .send(SipEvent::Request { req, src })
                    .await
                    .map_err(|_| anyhow!("gateway channel closed"))
            }
            _ => {
                let mut resp = self.make_response(&req, 405, None);
                resp.headers
                    .set("allow", "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, MESSAGE, REGISTER");
                self.respond(&req, src, resp).await
            }
        }
    }

    fn source_allowed(&self, ip: IpAddr) -> bool {
        if self.cfg.sip.allow.is_empty() {
            return true;
        }
        self.cfg.sip.allow.iter().any(|rule| ip_matches(rule, ip))
    }

    /// Returns Ok(true) when the request may proceed.  Sends 401 otherwise.
    async fn check_auth(self: &Arc<Self>, req: &Request, src: SocketAddr) -> Result<bool> {
        let Some(cred_cfg) = &self.cfg.sip.auth else { return Ok(true) };
        // In-dialog requests ride on the credentials of the initial one.
        if req.headers.to().and_then(|t| t.tag().map(str::to_string)).is_some()
            && matches!(req.method, Method::Bye | Method::Info | Method::Cancel)
        {
            return Ok(true);
        }
        let provided = req.headers.get("authorization").and_then(Credentials::parse);
        let ok = match &provided {
            Some(c) => {
                let user_ok = c.username == cred_cfg.username;
                let nonce_ok = self.nonce.is_valid(&c.nonce, 300);
                let digest_ok =
                    auth::verify(c, &cred_cfg.password, req.method.as_str(), &req.body);
                if !(user_ok && nonce_ok && digest_ok) {
                    debug!(
                        user_ok,
                        nonce_ok,
                        digest_ok,
                        username = %c.username,
                        uri = %c.uri,
                        qop = ?c.qop,
                        algorithm = ?c.algorithm,
                        "rejecting credentials"
                    );
                }
                user_ok && nonce_ok && digest_ok
            }
            None => false,
        };
        if ok {
            return Ok(true);
        }
        let stale = provided.map(|c| !self.nonce.is_valid(&c.nonce, 300)).unwrap_or(false);
        let mut resp = self.make_response(req, 401, None);
        resp.headers.set(
            "www-authenticate",
            format!(
                "Digest realm=\"{}\", nonce=\"{}\", algorithm=MD5, qop=\"auth\"{}",
                self.domain(),
                self.nonce.issue(),
                if stale { ", stale=true" } else { "" }
            ),
        );
        self.respond(req, src, resp).await?;
        Ok(false)
    }

    async fn handle_register(self: &Arc<Self>, req: Request, src: SocketAddr) -> Result<()> {
        if !self.check_auth(&req, src).await? {
            return Ok(());
        }
        let Some(to) = req.headers.to() else {
            return self.respond(&req, src, self.make_response(&req, 400, None)).await;
        };
        let aor = to.uri.bare().to_string();
        let default_expires: u32 =
            req.headers.get("expires").and_then(|v| v.trim().parse().ok()).unwrap_or(3600);

        let contacts = req.headers.get_all("contact");
        if contacts.iter().any(|c| c.trim() == "*") {
            self.registrar.remove_all(&aor);
        } else {
            for raw in contacts {
                let Some(contact) = NameAddr::parse(raw) else { continue };
                let expires = contact
                    .param("expires")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(default_expires);
                self.registrar.update(
                    &aor,
                    contact,
                    src,
                    expires,
                    req.headers.call_id().unwrap_or_default(),
                );
            }
        }

        let bindings = self.registrar.contacts(&aor);
        info!(%aor, bindings = bindings.len(), %src, "REGISTER");
        let mut resp = self.make_response(&req, 200, None);
        for b in &bindings {
            let secs = b.expires_at.saturating_duration_since(Instant::now()).as_secs();
            resp.headers.push("contact", format!("{};expires={}", b.contact, secs));
        }
        resp.headers.set("expires", default_expires.to_string());
        self.respond(&req, src, resp).await
    }

    async fn handle_options(self: &Arc<Self>, req: Request, src: SocketAddr) -> Result<()> {
        let down = self.cfg.sip.options_reflect_modem && !self.modem_ready();
        let mut resp = if down {
            let mut r = self.make_response(&req, 503, None);
            r.headers.set("retry-after", self.cfg.sip.retry_after_secs.to_string());
            r.headers.set("warning", "399 modem2sip \"modem not available\"");
            r
        } else {
            self.make_response(&req, 200, None)
        };
        resp.headers.set("allow", "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, MESSAGE, REGISTER");
        resp.headers.set("accept", "application/sdp, text/plain, application/json");
        self.respond(&req, src, resp).await
    }

    // ---------------------------------------------------------------- output

    pub fn make_response(&self, req: &Request, code: u16, reason: Option<&str>) -> Response {
        let mut resp = req.reply(code, reason.unwrap_or_else(|| reason_phrase(code)));
        resp.headers.set("user-agent", self.cfg.sip.user_agent.clone());
        resp
    }

    /// Send a response, applying rport/received and caching finals so that
    /// request retransmissions can be answered without re-running logic.
    pub async fn respond(&self, req: &Request, src: SocketAddr, mut resp: Response) -> Result<()> {
        // RFC 3581: reflect the source in the topmost Via.
        if let Some(raw) = resp.headers.get("via").map(str::to_string) {
            if let Some(mut via) = Via::parse(&raw) {
                via.set_param("received", Some(&src.ip().to_string()));
                if via.has_param("rport") {
                    via.set_param("rport", Some(&src.port().to_string()));
                }
                resp.headers.replace_first("via", via.to_string());
            }
        }
        if resp.headers.get("contact").is_none() && resp.code < 300 && resp.code != 100 {
            let ip = self.transport.advertised_ip(src);
            let user = req.uri.user.clone();
            let mut c = Uri::new(user.as_deref(), &ip.to_string(), Some(self.transport.port()));
            c.set_param("transport", Some("udp"));
            resp.headers.set("contact", NameAddr::new(c).to_string());
        }
        let bytes = resp.encode();
        trace!(%src, ">>>\n{}", String::from_utf8_lossy(&bytes));

        if resp.code >= 200 {
            if let Some(key) = txn_key(req) {
                self.server_cache.lock().unwrap().insert(key, (bytes.clone(), Instant::now()));
            }
        }
        self.transport.send(&bytes, src).await
    }

    /// Send a request and return a handle to its transaction.
    pub async fn send_request(self: &Arc<Self>, req: Request, dest: SocketAddr) -> Result<ClientTxn> {
        let branch = req
            .headers
            .top_via()
            .and_then(|v| v.branch().map(str::to_string))
            .ok_or_else(|| anyhow!("outgoing request has no Via branch"))?;
        let bytes = req.encode();
        trace!(%dest, ">>>\n{}", String::from_utf8_lossy(&bytes));

        let (tx, rx) = mpsc::unbounded_channel();
        let stop = Arc::new(AtomicBool::new(false));
        self.client_txns
            .lock()
            .unwrap()
            .insert(branch.clone(), ClientTxnEntry { tx, stop_retransmit: stop.clone() });

        self.transport.send(&bytes, dest).await?;

        // UDP retransmission: T1 with exponential back-off, capped at 32s.
        let core = self.clone();
        let stop_task = stop.clone();
        tokio::spawn(async move {
            let mut delay = Duration::from_millis(500);
            let mut elapsed = Duration::ZERO;
            while elapsed < Duration::from_secs(32) {
                tokio::time::sleep(delay).await;
                if stop_task.load(Ordering::Relaxed) {
                    return;
                }
                if core.transport.send(&bytes, dest).await.is_err() {
                    return;
                }
                elapsed += delay;
                delay = (delay * 2).min(Duration::from_secs(4));
            }
        });

        Ok(ClientTxn { branch, rx, core: self.clone(), stop })
    }

    /// Fire-and-forget (ACK, and requests we do not track).
    pub async fn send_raw(&self, req: &Request, dest: SocketAddr) -> Result<()> {
        let bytes = req.encode();
        trace!(%dest, ">>>\n{}", String::from_utf8_lossy(&bytes));
        self.transport.send(&bytes, dest).await
    }

    /// Build an out-of-dialog request with all mandatory headers filled in.
    #[allow(clippy::too_many_arguments)]
    pub fn build_request(
        &self,
        method: Method,
        request_uri: Uri,
        from: NameAddr,
        to: NameAddr,
        call_id: &str,
        cseq: u32,
        dest: SocketAddr,
        with_contact: bool,
    ) -> Request {
        let mut req = Request::new(method, request_uri);
        let ip = self.transport.advertised_ip(dest);
        let branch = format!("z9hG4bK{}", auth::random_hex(8));
        req.headers.push(
            "via",
            format!("SIP/2.0/UDP {}:{};rport;branch={}", ip, self.transport.port(), branch),
        );
        req.headers.set("max-forwards", "70");
        req.headers.set("from", from.to_string());
        req.headers.set("to", to.to_string());
        req.headers.set("call-id", call_id.to_string());
        req.headers.set("cseq", format!("{} {}", cseq, method));
        if with_contact {
            let user = from.uri.user.clone();
            let mut c = Uri::new(user.as_deref(), &ip.to_string(), Some(self.transport.port()));
            c.set_param("transport", Some("udp"));
            req.headers.set("contact", NameAddr::new(c).to_string());
        }
        req.headers.set("user-agent", self.cfg.sip.user_agent.clone());
        req
    }

    /// Run a non-INVITE transaction to completion, answering one digest
    /// challenge if credentials are available.
    pub async fn transact(
        self: &Arc<Self>,
        mut req: Request,
        dest: SocketAddr,
        creds: Option<(&str, &str)>,
        timeout: Duration,
    ) -> Result<Response> {
        let mut txn = self.send_request(req.clone(), dest).await?;
        let resp = txn
            .final_response(timeout)
            .await
            .ok_or_else(|| anyhow!("no final response to {}", req.method))?;
        drop(txn);

        if !matches!(resp.code, 401 | 407) {
            return Ok(resp);
        }
        let Some((user, pass)) = creds else { return Ok(resp) };

        let (hdr, out_hdr) = if resp.code == 401 {
            ("www-authenticate", "authorization")
        } else {
            ("proxy-authenticate", "proxy-authorization")
        };
        let challenge = resp
            .headers
            .get(hdr)
            .and_then(Challenge::parse)
            .ok_or_else(|| anyhow!("{} without a parsable challenge", resp.code))?;

        let uri_str = req.uri.to_string();
        let creds = auth::answer(
            &challenge,
            user,
            pass,
            req.method.as_str(),
            &uri_str,
            &req.body,
            1,
        );
        // New branch, next CSeq.
        let cseq = self.next_cseq();
        req.headers.set("cseq", format!("{} {}", cseq, req.method));
        refresh_via_branch(&mut req.headers);
        req.headers.set(out_hdr, creds.to_header());

        let mut txn = self.send_request(req, dest).await?;
        txn.final_response(timeout)
            .await
            .ok_or_else(|| anyhow!("no final response after authentication"))
    }
}

pub fn refresh_via_branch(headers: &mut Headers) {
    if let Some(raw) = headers.get("via").map(str::to_string) {
        if let Some(mut via) = Via::parse(&raw) {
            via.set_param("branch", Some(&format!("z9hG4bK{}", auth::random_hex(8))));
            headers.set("via", via.to_string());
        }
    }
}

/// Transaction identity for server-side retransmission detection.
fn txn_key(req: &Request) -> Option<String> {
    let branch = req.headers.top_via().and_then(|v| v.branch().map(str::to_string))?;
    let (cseq, _) = req.headers.cseq()?;
    // CANCEL shares the branch of the INVITE it cancels, so keep the method.
    Some(format!("{branch}|{cseq}|{}", req.method))
}

fn ip_matches(rule: &str, ip: IpAddr) -> bool {
    let rule = rule.trim();
    if let Some((net, prefix)) = rule.split_once('/') {
        let Ok(net): std::result::Result<IpAddr, _> = net.trim().parse() else { return false };
        let Ok(prefix): std::result::Result<u32, _> = prefix.trim().parse() else { return false };
        return match (net, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) if prefix <= 32 => {
                let mask = if prefix == 0 { 0 } else { u32::MAX << (32 - prefix) };
                u32::from(n) & mask == u32::from(a) & mask
            }
            (IpAddr::V6(n), IpAddr::V6(a)) if prefix <= 128 => {
                let n = u128::from(n);
                let a = u128::from(a);
                let mask = if prefix == 0 { 0 } else { u128::MAX << (128 - prefix) };
                n & mask == a & mask
            }
            _ => false,
        };
    }
    rule.parse::<IpAddr>().map(|r| r == ip).unwrap_or(false)
}

/// Resolve where gateway-originated requests should go: the configured
/// target if any, otherwise the freshest registration.
pub async fn resolve_target(core: &SipCore, configured: Option<&str>) -> Result<(Uri, SocketAddr)> {
    if let Some(t) = configured {
        let uri = Uri::parse(t).ok_or_else(|| anyhow!("invalid SIP target: {t}"))?;
        let addr = transport::resolve_uri(&uri).await?;
        return Ok((uri, addr));
    }
    let binding = core
        .registrar
        .newest()
        .ok_or_else(|| anyhow!("no SIP target configured and nobody is registered"))?;
    let uri = binding.contact.uri.clone();
    // Prefer the source address the UA registered from (NAT).
    Ok((uri, binding.source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_matching() {
        assert!(ip_matches("192.168.1.0/24", "192.168.1.42".parse().unwrap()));
        assert!(!ip_matches("192.168.1.0/24", "192.168.2.42".parse().unwrap()));
        assert!(ip_matches("10.0.0.1", "10.0.0.1".parse().unwrap()));
        assert!(ip_matches("0.0.0.0/0", "8.8.8.8".parse().unwrap()));
    }
}
