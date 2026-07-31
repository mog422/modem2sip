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
    /// The method this transaction sent.  A CANCEL reuses the branch of the
    /// INVITE it cancels, so the branch alone does not identify a response.
    method: Method,
    tx: mpsc::UnboundedSender<Response>,
    stop_retransmit: Arc<AtomicBool>,
}

/// Server-side transaction state, kept so that a retransmitted request is
/// never executed twice.
enum ServerTxn {
    /// Handed to a handler; the final response has not been produced yet.
    /// The most recent provisional, if any, is kept so a retransmission can
    /// be answered with it.
    InProgress { since: Instant, provisional: Option<Vec<u8>> },
    /// Answered.  Retransmissions get this response replayed verbatim.
    Completed(Vec<u8>, Instant),
}

impl ServerTxn {
    fn started(&self) -> Instant {
        match self {
            ServerTxn::InProgress { since, .. } => *since,
            ServerTxn::Completed(_, t) => *t,
        }
    }
}

/// How long a server transaction is remembered (RFC 3261 Timer H / 64*T1).
const TXN_LIFETIME: Duration = Duration::from_secs(32);

/// Longest registration this registrar hands out, whatever the UA asks for.
const MAX_REGISTER_EXPIRES: u32 = 3600;

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
    /// Server transactions, keyed by branch+cseq+method.
    server_cache: Mutex<HashMap<String, ServerTxn>>,
    /// Highest nonce-count seen per issued nonce, so a captured Authorization
    /// header cannot simply be replayed.
    nonce_seen: Mutex<HashMap<String, (u32, Instant)>>,
    /// 2xx answers to an INVITE that are still waiting for their ACK, keyed
    /// by Call-ID and the INVITE's sequence number.  Setting the flag stops
    /// the retransmission timer.  Shared with those timers, which drop their
    /// own entry when they give up.
    pending_ack: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
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
            nonce_seen: Mutex::new(HashMap::new()),
            pending_ack: Arc::new(Mutex::new(HashMap::new())),
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
        let now = Instant::now();
        let mut cache = self.server_cache.lock().unwrap();
        if cache.len() > 64 {
            cache.retain(|_, txn| now.duration_since(txn.started()) < TXN_LIFETIME);
        }
        let mut nonces = self.nonce_seen.lock().unwrap();
        if nonces.len() > 64 {
            nonces.retain(|_, (_, t)| now.duration_since(*t) < Duration::from_secs(600));
        }
    }

    fn on_response(&self, resp: Response) {
        let Some(branch) = resp.headers.top_via().and_then(|v| v.branch().map(str::to_string))
        else {
            debug!("response without Via branch, dropped");
            return;
        };
        let Some((_, method)) = resp.headers.cseq() else {
            debug!(%branch, "response without a usable CSeq, dropped");
            return;
        };
        let map = self.client_txns.lock().unwrap();
        match map.get(&branch) {
            // A CANCEL carries the branch of the INVITE it cancels, so its
            // 200 OK would otherwise be delivered as if the INVITE had been
            // answered.
            Some(entry) if entry.method != method => debug!(
                %branch,
                code = resp.code,
                %method,
                "response method does not match the transaction, dropped"
            ),
            Some(entry) => {
                // RFC 3261 §17.1.2.2: a non-INVITE transaction keeps
                // retransmitting after a provisional, because the server side
                // only re-sends its final answer when it sees the request
                // again.  Stopping on a 100 Trying from a proxy meant a lost
                // final response was never recovered - the SMS or the
                // registration simply timed out.  For an INVITE the
                // provisional is the signal to stop.
                if resp.code >= 200 || entry.method == Method::Invite {
                    entry.stop_retransmit.store(true, Ordering::Relaxed);
                }
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

        // The ACK is what tells us the 2xx arrived; until it does, that
        // response is retransmitted.  It carries the INVITE's sequence
        // number, so it identifies the answer it acknowledges even though it
        // is a transaction of its own.
        if req.method == Method::Ack {
            if let Some(key) = dialog_key(&req) {
                if let Some(stop) = self.pending_ack.lock().unwrap().remove(&key) {
                    debug!(%key, "ACK received; the 2xx is confirmed");
                    stop.store(true, Ordering::Relaxed);
                }
            }
        }

        // Retransmission handling.  Answering a request twice is not just
        // noise: a repeated MESSAGE sends the SMS again, and a repeated
        // INVITE looks like a re-INVITE for a call that is still dialling.
        // Requests are therefore claimed before any work starts, and only
        // released once the final response has been produced.
        if req.method != Method::Ack {
            if let Some(key) = txn_key(&req) {
                let claim = claim_txn(
                    &mut self.server_cache.lock().unwrap(),
                    &key,
                    Instant::now(),
                );
                match claim {
                    Claim::Answered(bytes) => {
                        debug!(%key, "retransmitted request, replaying the cached response");
                        self.transport.send(&bytes, src).await?;
                        return Ok(());
                    }
                    Claim::InFlight(provisional) => {
                        debug!(%key, "retransmission while the request is still being handled");
                        if let Some(bytes) = provisional {
                            self.transport.send(&bytes, src).await?;
                        }
                        return Ok(());
                    }
                    Claim::Fresh => {}
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
        // RFC 3261 §22.1: a UAS must not challenge a CANCEL - it has to be
        // accepted on the strength of the INVITE transaction it matches, and
        // UAs that do not copy the Authorization across would otherwise never
        // be able to hang up a call that is still ringing.
        if req.method == Method::Cancel {
            return Ok(true);
        }
        // In-dialog requests ride on the credentials of the initial one.  The
        // gateway re-checks that the tags really are the ones it issued
        // before acting on them.
        if req.headers.to().and_then(|t| t.tag().map(str::to_string)).is_some()
            && matches!(req.method, Method::Bye | Method::Info)
        {
            return Ok(true);
        }
        let provided = req.headers.get("authorization").and_then(Credentials::parse);
        // `stale` means "the credentials were right, only the nonce was not",
        // which tells a UA to retry silently instead of asking the user for a
        // password it already has.
        let mut stale = false;
        let ok = match &provided {
            Some(c) => {
                let user_ok = c.username == cred_cfg.username;
                // The digest covers the realm and the URI the client claims
                // to be addressing, so both have to be the ones we asked for
                // - otherwise a response computed for one request authorises
                // any other.
                let realm_ok = c.realm == self.domain();
                // Compared as parsed URIs, not as strings: UAs differ on
                // scheme case, on which parameters they carry, and some put
                // the To-URI here rather than the Request-URI.
                let uri_ok = Uri::parse(&c.uri)
                    .map(|u| {
                        let bare = u.bare();
                        bare == req.uri.bare()
                            || req.headers.to().map(|t| bare == t.uri.bare()).unwrap_or(false)
                    })
                    .unwrap_or(false);
                let nonce_ok = self.nonce.is_valid(&c.nonce, 300);
                let digest_ok =
                    auth::verify(c, &cred_cfg.password, req.method.as_str(), &req.body);
                let credentials_ok = user_ok && realm_ok && uri_ok && digest_ok;
                // Claiming the nonce is what consumes it, so it must not
                // happen for credentials that were going to be rejected
                // anyway - a wrong password would otherwise burn the nonce
                // the legitimate client is about to use.
                let fresh = credentials_ok && nonce_ok && self.claim_nonce(c);
                stale = credentials_ok && !fresh;
                if !(credentials_ok && fresh) {
                    debug!(
                        user_ok,
                        realm_ok,
                        uri_ok,
                        nonce_ok,
                        fresh,
                        digest_ok,
                        username = %c.username,
                        uri = %c.uri,
                        qop = ?c.qop,
                        algorithm = ?c.algorithm,
                        "rejecting credentials"
                    );
                }
                credentials_ok && fresh
            }
            None => false,
        };
        if ok {
            return Ok(true);
        }
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

    /// Consume one use of the nonce in `creds`.
    ///
    /// Digest over UDP is otherwise trivially replayable: the whole
    /// `Authorization` header can be lifted off the wire and pasted onto a
    /// different request until the nonce ages out.  With `qop=auth` the
    /// client supplies a counter, so each value is accepted once and never
    /// again; without one the nonce itself becomes single-use.
    fn claim_nonce(&self, creds: &Credentials) -> bool {
        let nc = creds
            .nc
            .as_deref()
            .and_then(|v| u32::from_str_radix(v.trim(), 16).ok())
            .unwrap_or(0);
        let mut seen = self.nonce_seen.lock().unwrap();
        match seen.get_mut(&creds.nonce) {
            Some((highest, _)) if nc > *highest => {
                *highest = nc;
                true
            }
            Some(_) => false,
            None => {
                seen.insert(creds.nonce.clone(), (nc, Instant::now()));
                true
            }
        }
    }

    async fn handle_register(self: &Arc<Self>, req: Request, src: SocketAddr) -> Result<()> {
        if !self.check_auth(&req, src).await? {
            return Ok(());
        }
        let Some(to) = req.headers.to() else {
            return self.respond(&req, src, self.make_response(&req, 400, None)).await;
        };
        let aor = to.uri.bare().to_string();
        // An unbounded Expires would create a binding that never lapses, so
        // the registrar decides the lifetime and tells the UA what it got.
        let default_expires: u32 = req
            .headers
            .get("expires")
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(MAX_REGISTER_EXPIRES)
            .min(MAX_REGISTER_EXPIRES);

        let contacts = req.headers.get_all("contact");
        if contacts.iter().any(|c| c.trim() == "*") {
            self.registrar.remove_all(&aor);
        } else {
            // A Contact field may hold a comma separated list, and each entry
            // carries its own expiry - including the 0 that de-registers it.
            for raw in contacts.iter().flat_map(|c| split_contacts(c)) {
                let Some(contact) = NameAddr::parse(&raw) else { continue };
                let expires = contact
                    .param("expires")
                    .and_then(|v| v.trim().parse::<u32>().ok())
                    .unwrap_or(default_expires)
                    .min(MAX_REGISTER_EXPIRES);
                self.registrar.update(&aor, contact, src, expires);
            }
        }

        let bindings = self.registrar.contacts(&aor);
        info!(%aor, bindings = bindings.len(), %src, "REGISTER");
        let mut resp = self.make_response(&req, 200, None);
        for b in &bindings {
            let secs = b.expires_at.saturating_duration_since(Instant::now()).as_secs();
            // The stored contact still carries the client's own expires
            // parameter; ours is the authoritative one, so replace it.
            let mut contact = b.contact.clone();
            contact.set_param("expires", Some(&secs.to_string()));
            resp.headers.push("contact", contact.to_string());
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
        // RFC 3261 §8.2.6.2: every response except a 100 Trying carries a To
        // tag - it is half of what identifies the dialog, and peers that take
        // that seriously reject a final response without one.  A call the
        // gateway tracks overwrites this with the tag it stored for the
        // dialog; everything else needs one generated here.
        if code != 100 {
            if let Some(to) = resp.headers.to().filter(|t| t.tag().is_none()) {
                resp.headers.set("to", to.with_tag(&auth::random_hex(6)).to_string());
            }
        }
        resp
    }

    /// Send a response, applying rport/received and caching finals so that
    /// request retransmissions can be answered without re-running logic.
    pub async fn respond(&self, req: &Request, src: SocketAddr, mut resp: Response) -> Result<()> {
        // RFC 3581: reflect the source in the topmost Via.
        if let Some(raw) = resp.headers.get("via").map(str::to_string) {
            // One Via header field may hold several comma separated values
            // (RFC 3261 §7.3.1), and only the first of them is ours to
            // annotate.  Replacing the whole field with just that one threw
            // the others away, and a proxy that folds its Vias then had
            // nothing left to route the response back through.
            let (first, rest) = match raw.split_once(',') {
                Some((f, r)) => (f, Some(r)),
                None => (raw.as_str(), None),
            };
            if let Some(mut via) = Via::parse(first) {
                via.set_param("received", Some(&src.ip().to_string()));
                if via.has_param("rport") {
                    via.set_param("rport", Some(&src.port().to_string()));
                }
                let value = match rest {
                    Some(rest) => format!("{via},{rest}"),
                    None => via.to_string(),
                };
                resp.headers.replace_first("via", value);
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

        if let Some(key) = txn_key(req) {
            let mut cache = self.server_cache.lock().unwrap();
            if resp.code >= 200 {
                cache.insert(key, ServerTxn::Completed(bytes.clone(), Instant::now()));
            } else if let Some(ServerTxn::InProgress { provisional, .. }) = cache.get_mut(&key) {
                // Keep the latest provisional so a retransmission of the
                // request can be answered with it instead of being swallowed.
                // Receiving it is what stops the caller retransmitting.
                *provisional = Some(bytes.clone());
            }
        }

        // RFC 3261 §13.3.1.4: the 2xx that answers an INVITE is the one
        // response nothing else will recover.  The caller stopped
        // retransmitting when it got our 100, and a 2xx ends the transaction,
        // so if this datagram is lost the caller waits in "ringing" while the
        // mobile leg is connected and billed.  Repeat it until the ACK comes.
        if req.method == Method::Invite && (200..300).contains(&resp.code) {
            self.retransmit_until_acked(req, bytes.clone(), src);
        }

        self.transport.send(&bytes, src).await
    }

    /// Keep re-sending a 2xx answer to an INVITE until it is acknowledged.
    fn retransmit_until_acked(&self, req: &Request, bytes: Vec<u8>, dest: SocketAddr) {
        let Some(key) = dialog_key(req) else { return };
        let stop = Arc::new(AtomicBool::new(false));
        // A re-INVITE answered while the first 2xx is still unacknowledged
        // replaces it: only the newest answer is worth repeating.
        if let Some(previous) = self.pending_ack.lock().unwrap().insert(key.clone(), stop.clone()) {
            previous.store(true, Ordering::Relaxed);
        }

        let sock = self.transport.socket();
        let pending = self.pending_ack.clone();
        tokio::spawn(async move {
            let mut delay = Duration::from_millis(500);
            let mut elapsed = Duration::ZERO;
            while elapsed < Duration::from_secs(32) {
                tokio::time::sleep(delay).await;
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                if sock.send_to(&bytes, dest).await.is_err() {
                    break;
                }
                elapsed += delay;
                delay = (delay * 2).min(Duration::from_secs(4));
            }
            if !stop.load(Ordering::Relaxed) {
                warn!(%key, "no ACK for the 2xx after 32s; giving up on retransmitting it");
            }
            // Only our own entry: a re-INVITE may have replaced it, and that
            // newer answer is still waiting for an ACK of its own.
            let mut map = pending.lock().unwrap();
            if map.get(&key).map(|s| Arc::ptr_eq(s, &stop)).unwrap_or(false) {
                map.remove(&key);
            }
        });
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
        self.client_txns.lock().unwrap().insert(
            branch.clone(),
            ClientTxnEntry { method: req.method, tx, stop_retransmit: stop.clone() },
        );

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
            format!(
                "SIP/2.0/UDP {}:{};rport;branch={}",
                super::uri::host_for_wire(&ip.to_string()),
                self.transport.port(),
                branch
            ),
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
        // RFC 8760: a peer may offer several challenges, strongest first, and
        // only some of them are ones we can answer.  Taking the first one
        // blindly meant a SHA-256 challenge was answered with an MD5 digest
        // labelled SHA-256 - rejected every time, with nothing in the log to
        // say why.
        let challenge = resp
            .headers
            .get_all(hdr)
            .into_iter()
            .filter_map(Challenge::parse)
            .find(Challenge::is_supported)
            .ok_or_else(|| {
                anyhow!("{} with no digest challenge this gateway can answer (MD5 only)", resp.code)
            })?;

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

/// What to do with a request whose transaction may already be known.
#[derive(Debug, PartialEq, Eq)]
enum Claim {
    /// Not seen before (or long expired): run the handler.
    Fresh,
    /// Still being handled; this copy is a retransmission.  RFC 3261 §17.2.1
    /// says to re-send the most recent provisional, which is what stops the
    /// caller retransmitting; with nothing to send yet the copy is dropped.
    InFlight(Option<Vec<u8>>),
    /// Already answered; replay these bytes.
    Answered(Vec<u8>),
}

/// Take ownership of a server transaction, or say why we cannot.
///
/// Claiming happens before any work starts, not after the response exists:
/// a MESSAGE that spends seconds at the modem would otherwise be handled once
/// per retransmission and send the SMS several times.
fn claim_txn(cache: &mut HashMap<String, ServerTxn>, key: &str, now: Instant) -> Claim {
    match cache.get(key) {
        Some(txn) if now.duration_since(txn.started()) < TXN_LIFETIME => match txn {
            ServerTxn::Completed(bytes, _) => Claim::Answered(bytes.clone()),
            ServerTxn::InProgress { provisional, .. } => Claim::InFlight(provisional.clone()),
        },
        // Anything older than the transaction lifetime is a stale collision
        // from a UA that restarted its branch counter, not a retransmission.
        _ => {
            cache.insert(key.to_string(), ServerTxn::InProgress { since: now, provisional: None });
            Claim::Fresh
        }
    }
}

/// Split one Contact header field into its comma separated entries.
///
/// Commas inside `<...>` or a quoted display name are part of the value, so a
/// naive `split(',')` would corrupt them.
fn split_contacts(field: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut start = 0;
    let (mut in_angle, mut in_quote) = (false, false);
    for (i, c) in field.char_indices() {
        match c {
            '"' if !in_angle => in_quote = !in_quote,
            '<' if !in_quote => in_angle = true,
            '>' if !in_quote => in_angle = false,
            ',' if !in_angle && !in_quote => {
                out.push(field[start..i].trim().to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(field[start..].trim().to_string());
    out.retain(|s| !s.is_empty());
    out
}

/// Identity shared by an INVITE and the ACK that confirms its 2xx.
///
/// The ACK for a 2xx is a transaction of its own with a branch of its own, so
/// it cannot be matched on the branch; what it does carry is the Call-ID and
/// the INVITE's sequence number.
fn dialog_key(req: &Request) -> Option<String> {
    let call_id = req.headers.call_id()?;
    let (cseq, _) = req.headers.cseq()?;
    Some(format!("{call_id}|{cseq}"))
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

    /// A Contact field may be a comma separated list, and splitting it wrong
    /// turns "de-register this one" into "register it for an hour".
    #[test]
    fn contact_lists_split_on_the_right_commas() {
        assert_eq!(
            split_contacts("<sip:a@h>;expires=0, <sip:b@h>;expires=3600"),
            vec!["<sip:a@h>;expires=0", "<sip:b@h>;expires=3600"]
        );
        assert_eq!(split_contacts("<sip:a@h>"), vec!["<sip:a@h>"]);
        // Commas inside a quoted display name or angle brackets are literal.
        assert_eq!(
            split_contacts("\"Smith, John\" <sip:j@h>, <sip:b@h>"),
            vec!["\"Smith, John\" <sip:j@h>", "<sip:b@h>"]
        );
        assert_eq!(split_contacts("<sip:a@h;x=1,2>"), vec!["<sip:a@h;x=1,2>"]);
        assert!(split_contacts("  ").is_empty());
    }

    /// The ACK for a 2xx is a transaction of its own with a branch of its
    /// own, so the 2xx it confirms can only be found through the Call-ID and
    /// the INVITE's sequence number.
    #[test]
    fn an_ack_is_matched_to_the_invite_it_confirms() {
        let raw = |method: &str, call_id: &str, cseq: u32| {
            let text = format!(
                "{method} sip:x@gw SIP/2.0\r\nVia: SIP/2.0/UDP h;branch=z9hG4bK{cseq}{method}\r\n\
                 From: <sip:a@h>;tag=1\r\nTo: <sip:x@gw>;tag=2\r\nCall-ID: {call_id}\r\n\
                 CSeq: {cseq} {method}\r\n\r\n"
            );
            match Message::parse(text.as_bytes()).unwrap() {
                Message::Request(r) => r,
                _ => panic!("expected a request"),
            }
        };
        // Different branches, same dialog and sequence number: a match.
        assert_eq!(
            dialog_key(&raw("INVITE", "call-1", 7)),
            dialog_key(&raw("ACK", "call-1", 7))
        );
        // An ACK for a different call or a different INVITE is not.
        assert_ne!(
            dialog_key(&raw("INVITE", "call-1", 7)),
            dialog_key(&raw("ACK", "call-2", 7))
        );
        assert_ne!(
            dialog_key(&raw("INVITE", "call-1", 7)),
            dialog_key(&raw("ACK", "call-1", 8))
        );
    }

    /// A CANCEL reuses the INVITE's branch, so the transaction key has to
    /// keep them apart or its 200 OK lands in the INVITE's transaction.
    #[test]
    fn transaction_keys_separate_cancel_from_invite() {
        let raw = |method: &str| {
            let text = format!(
                "{method} sip:x@gw SIP/2.0\r\nVia: SIP/2.0/UDP h;branch=z9hG4bK1\r\n\
                 From: <sip:a@h>;tag=1\r\nTo: <sip:x@gw>\r\nCall-ID: c\r\n\
                 CSeq: 7 {method}\r\n\r\n"
            );
            match Message::parse(text.as_bytes()).unwrap() {
                Message::Request(r) => r,
                _ => panic!("expected a request"),
            }
        };
        assert_ne!(txn_key(&raw("INVITE")), txn_key(&raw("CANCEL")));
        assert_eq!(txn_key(&raw("INVITE")), txn_key(&raw("INVITE")));
    }

    /// A retransmitted request has to be recognised while it is still being
    /// handled, not only once it has been answered - a MESSAGE that takes a
    /// few seconds at the modem used to send the SMS once per retransmission.
    #[test]
    fn a_transaction_is_claimed_before_the_work_starts() {
        let mut cache = HashMap::new();
        let key = "z9hG4bK1|7|MESSAGE";
        let t0 = Instant::now();

        assert_eq!(claim_txn(&mut cache, key, t0), Claim::Fresh, "the first copy is handled");
        // A retransmission arriving while the modem is still sending the SMS
        // must not reach the handler a second time.
        assert_eq!(
            claim_txn(&mut cache, key, t0 + Duration::from_millis(500)),
            Claim::InFlight(None)
        );

        // Once a provisional has gone out, a retransmission gets it again -
        // that is what stops the caller's own retransmission timer.
        cache.insert(
            key.to_string(),
            ServerTxn::InProgress { since: t0, provisional: Some(b"SIP/2.0 100".to_vec()) },
        );
        assert_eq!(
            claim_txn(&mut cache, key, t0 + Duration::from_secs(4)),
            Claim::InFlight(Some(b"SIP/2.0 100".to_vec()))
        );

        cache.insert(key.to_string(), ServerTxn::Completed(b"SIP/2.0 202".to_vec(), t0));
        assert_eq!(
            claim_txn(&mut cache, key, t0 + Duration::from_secs(1)),
            Claim::Answered(b"SIP/2.0 202".to_vec()),
            "once answered, a copy replays the answer"
        );

        // Past the transaction lifetime the entry is a stale collision from a
        // UA that restarted its branch counter, not a retransmission - it must
        // not have a minutes-old response replayed at it.
        assert_eq!(claim_txn(&mut cache, key, t0 + TXN_LIFETIME * 2), Claim::Fresh);
        // A different transaction is never confused with this one.
        assert_eq!(claim_txn(&mut cache, "z9hG4bK1|8|MESSAGE", t0), Claim::Fresh);
    }
}
