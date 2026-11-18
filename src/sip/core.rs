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

/// A client transaction is identified by its branch *and* its method: a
/// CANCEL reuses the branch of the INVITE it cancels, and the two are
/// separate transactions that can both be outstanding at once.
type ClientTxnKey = (String, Method);

struct ClientTxnEntry {
    tx: mpsc::UnboundedSender<Response>,
    stop_retransmit: Arc<AtomicBool>,
}

/// What became of an INVITE we sent, once the gateway stopped waiting on it.
///
/// The transaction is over as far as the gateway is concerned, but not as far
/// as the wire is: UDP loses ACKs, and a peer can answer in the same instant
/// we give up.  Responses that turn up afterwards are dealt with from here.
enum Settled {
    /// Answered with a 2xx and acknowledged.  The peer repeats its 2xx until
    /// the ACK reaches it, so each repeat gets the same ACK again.
    Answered { ack: Vec<u8> },
    /// Given up on: rejected, cancelled, or never answered in time.
    Abandoned { invite: Box<Request> },
}

struct SettledInvite {
    state: Settled,
    dest: SocketAddr,
    since: Instant,
}

impl SettledInvite {
    fn expired(&self, now: Instant) -> bool {
        let lifetime = match self.state {
            // A 2xx is retransmitted for 64*T1 at most.
            Settled::Answered { .. } => Duration::from_secs(64),
            // An INVITE the peer never heard a CANCEL for can still be
            // answered for as long as it keeps ringing (RFC 3261 Timer C).
            Settled::Abandoned { .. } => Duration::from_secs(180),
        };
        now.duration_since(self.since) >= lifetime
    }
}

/// What a response to a settled INVITE calls for.
#[derive(Debug, PartialEq, Eq)]
enum LateReply {
    Ignore,
    /// A retransmission of the 2xx we already acknowledged.
    RepeatAck,
    /// A final rejection of an INVITE we gave up on: acknowledge it, or the
    /// peer keeps retransmitting it for 32 s.
    AckFailure,
    /// The peer answered an INVITE we had already given up on.  It now
    /// believes a call is up; acknowledge the answer and end that call.
    AckAndBye,
}

fn late_reply(answered: bool, code: u16) -> LateReply {
    match (answered, code) {
        (_, 0..=199) => LateReply::Ignore,
        (true, 200..=299) => LateReply::RepeatAck,
        (true, _) => LateReply::Ignore,
        (false, 200..=299) => LateReply::AckAndBye,
        (false, _) => LateReply::AckFailure,
    }
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
    key: ClientTxnKey,
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
        self.core.client_txns.lock().unwrap().remove(&self.key);
    }
}

pub struct SipCore {
    pub cfg: Arc<Config>,
    pub transport: Transport,
    pub registrar: Registrar,
    nonce: NonceFactory,
    client_txns: Mutex<HashMap<ClientTxnKey, ClientTxnEntry>>,
    /// INVITEs we sent whose transaction the gateway has finished with,
    /// keyed by branch, for the responses that still arrive afterwards.
    settled_invites: Mutex<HashMap<String, SettledInvite>>,
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
            settled_invites: Mutex::new(HashMap::new()),
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

    /// The host name this gateway goes by when talking to `peer`: the digest
    /// realm, and the host part of the URIs it writes about itself.
    ///
    /// `sip.domain` when set, otherwise the address it is bound to.  Bound to
    /// the wildcard, that used to come out as a literal `0.0.0.0` - in the
    /// From of every call and message handed to SIP, which a phone cannot
    /// call or reply to.  The address `peer` reaches us on is used instead.
    pub fn domain_for(&self, peer: SocketAddr) -> String {
        if let Some(domain) = &self.cfg.sip.domain {
            return domain.clone();
        }
        let bound = self.transport.bound().ip();
        if !bound.is_unspecified() {
            return bound.to_string();
        }
        self.transport.advertised_ip(peer).to_string()
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
        self.settled_invites.lock().unwrap().retain(|_, s| !s.expired(now));
    }

    fn on_response(self: &Arc<Self>, resp: Response) {
        let Some(branch) = resp.headers.top_via().and_then(|v| v.branch().map(str::to_string))
        else {
            debug!("response without Via branch, dropped");
            return;
        };
        let Some((_, method)) = resp.headers.cseq() else {
            debug!(%branch, "response without a usable CSeq, dropped");
            return;
        };
        // Looked up by branch and method together: a CANCEL carries the
        // branch of the INVITE it cancels, and its 200 OK must not be
        // delivered as if the INVITE had been answered.
        let key = (branch, method);
        {
            let map = self.client_txns.lock().unwrap();
            if let Some(entry) = map.get(&key) {
                // RFC 3261 §17.1.2.2: a non-INVITE transaction keeps
                // retransmitting after a provisional, because the server side
                // only re-sends its final answer when it sees the request
                // again.  Stopping on a 100 Trying from a proxy meant a lost
                // final response was never recovered - the SMS or the
                // registration simply timed out.  For an INVITE the
                // provisional is the signal to stop.
                if resp.code >= 200 || method == Method::Invite {
                    entry.stop_retransmit.store(true, Ordering::Relaxed);
                }
                let _ = entry.tx.send(resp);
                return;
            }
        }
        let (branch, _) = key;
        if method == Method::Invite {
            self.on_late_invite_response(&branch, resp);
        } else {
            debug!(%branch, code = resp.code, %method, "response for unknown transaction");
        }
    }

    /// A response to an INVITE nobody is waiting on any more.
    fn on_late_invite_response(self: &Arc<Self>, branch: &str, resp: Response) {
        let mut map = self.settled_invites.lock().unwrap();
        let Some(entry) = map.get_mut(branch) else {
            debug!(%branch, code = resp.code, "response for unknown transaction");
            return;
        };
        let dest = entry.dest;
        let answered = matches!(entry.state, Settled::Answered { .. });
        match (late_reply(answered, resp.code), &entry.state) {
            (LateReply::RepeatAck, Settled::Answered { ack }) => {
                debug!(%branch, "the 2xx was retransmitted; repeating the ACK");
                self.send_detached(ack.clone(), dest);
            }
            (LateReply::AckFailure, Settled::Abandoned { invite }) => {
                debug!(%branch, code = resp.code, "acknowledging the end of an INVITE we gave up on");
                self.send_detached(ack_for_failure(invite, &resp).encode(), dest);
            }
            (LateReply::AckAndBye, Settled::Abandoned { invite }) => {
                let Some((ack, bye)) = self.ack_and_bye(invite, &resp, dest) else {
                    warn!(%branch, "a 2xx for an abandoned INVITE cannot be answered; ignoring it");
                    return;
                };
                warn!(
                    %branch,
                    "the SIP side answered a call we had already given up on; hanging it up"
                );
                let ack = ack.encode();
                // From here on this is an answered INVITE: repeats of the 2xx
                // get the ACK again, and the BYE goes out only once.
                entry.state = Settled::Answered { ack: ack.clone() };
                entry.since = Instant::now();
                drop(map);
                let core = self.clone();
                tokio::spawn(async move {
                    let _ = core.transport.send(&ack, dest).await;
                    if let Ok(mut txn) = core.send_request(bye, dest).await {
                        let _ = txn.final_response(Duration::from_secs(32)).await;
                    }
                });
            }
            _ => {}
        }
    }

    /// The ACK and the BYE that end a dialog a late 2xx has just created.
    fn ack_and_bye(
        &self,
        invite: &Request,
        resp: &Response,
        dest: SocketAddr,
    ) -> Option<(Request, Request)> {
        let from = invite.headers.from()?;
        let to = resp.headers.to()?;
        let call_id = invite.headers.call_id()?;
        let (cseq, _) = invite.headers.cseq()?;
        let target = resp.headers.contact().map(|c| c.uri).unwrap_or_else(|| invite.uri.clone());
        let mut ack = self.build_request(
            Method::Ack,
            target.clone(),
            from.clone(),
            to.clone(),
            call_id,
            cseq,
            dest,
            false,
        );
        let mut bye =
            self.build_request(Method::Bye, target, from, to, call_id, self.next_cseq(), dest, false);
        for route in route_set_from_response(resp) {
            ack.headers.push("route", route.clone());
            bye.headers.push("route", route);
        }
        Some((ack, bye))
    }

    fn send_detached(self: &Arc<Self>, bytes: Vec<u8>, dest: SocketAddr) {
        let core = self.clone();
        tokio::spawn(async move {
            let _ = core.transport.send(&bytes, dest).await;
        });
    }

    /// The INVITE we sent was answered with a 2xx and `ack` has gone out (or
    /// is about to).
    ///
    /// The peer cannot tell a lost ACK from a caller that has gone away: it
    /// repeats its 2xx, and if nothing ever acknowledges it, ends the call
    /// after 32 s.  So the ACK is kept and sent again for every repeat.
    pub fn invite_answered(&self, invite: &Request, ack: &Request, dest: SocketAddr) {
        self.settle_invite(invite, Settled::Answered { ack: ack.encode() }, dest);
    }

    /// The gateway is done with an INVITE it sent that was not answered:
    /// it was rejected, or it is being given up on.
    ///
    /// Whatever final response still arrives is acknowledged - a rejection
    /// that is not ACKed is retransmitted for 32 s - and a 2xx, from a peer
    /// that picked up in the same moment, is acknowledged and then ended with
    /// a BYE rather than left as a call only one side knows about.
    pub fn invite_abandoned(&self, invite: &Request, dest: SocketAddr) {
        if let Some(branch) = self.settle_invite(
            invite,
            Settled::Abandoned { invite: Box::new(invite.clone()) },
            dest,
        ) {
            // Nobody is listening on the transaction any more; responses go
            // to the record just made instead.
            if let Some(entry) =
                self.client_txns.lock().unwrap().remove(&(branch, Method::Invite))
            {
                entry.stop_retransmit.store(true, Ordering::Relaxed);
            }
        }
    }

    fn settle_invite(&self, invite: &Request, state: Settled, dest: SocketAddr) -> Option<String> {
        let branch = invite.headers.top_via().and_then(|v| v.branch().map(str::to_string))?;
        self.settled_invites
            .lock()
            .unwrap()
            .insert(branch.clone(), SettledInvite { state, dest, since: Instant::now() });
        Some(branch)
    }

    /// Give up on an INVITE we sent that is still ringing.
    ///
    /// The CANCEL is a transaction of its own and is retransmitted like any
    /// other request - sent once, a single lost datagram left the SIP phone
    /// ringing for a call that no longer existed.
    pub fn cancel_invite(self: &Arc<Self>, invite: &Request, dest: SocketAddr) {
        self.invite_abandoned(invite, dest);
        let cancel = cancel_for(invite);
        let core = self.clone();
        tokio::spawn(async move {
            match core.send_request(cancel, dest).await {
                Ok(mut txn) => {
                    let _ = txn.final_response(Duration::from_secs(32)).await;
                }
                Err(e) => debug!(error = %format!("{e:#}"), "could not send the CANCEL"),
            }
        });
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
                let realm_ok = c.realm == self.domain_for(src);
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
                self.domain_for(src),
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
        let key: ClientTxnKey = (branch, req.method);
        self.client_txns
            .lock()
            .unwrap()
            .insert(key.clone(), ClientTxnEntry { tx, stop_retransmit: stop.clone() });

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

        Ok(ClientTxn { key, rx, core: self.clone(), stop })
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

/// RFC 3261 §17.1.1.3: the ACK for a non-2xx final response to an INVITE.
///
/// It belongs to the INVITE's own transaction, so it reuses the INVITE's
/// branch and sequence number, and takes the To header - with the peer's tag
/// - from the response it acknowledges.
pub fn ack_for_failure(invite: &Request, resp: &Response) -> Request {
    let mut ack = invite.clone();
    ack.method = Method::Ack;
    ack.body.clear();
    ack.headers.remove("content-type");
    if let Some((cseq, _)) = invite.headers.cseq() {
        ack.headers.set("cseq", format!("{cseq} ACK"));
    }
    if let Some(to) = resp.headers.get("to") {
        ack.headers.set("to", to.to_string());
    }
    ack
}

/// RFC 3261 §9.1: the CANCEL for an INVITE we sent.  Same branch, same
/// sequence number, no body.
fn cancel_for(invite: &Request) -> Request {
    let mut cancel = invite.clone();
    cancel.method = Method::Cancel;
    cancel.body.clear();
    cancel.headers.remove("content-type");
    if let Some((cseq, _)) = invite.headers.cseq() {
        cancel.headers.set("cseq", format!("{cseq} CANCEL"));
    }
    cancel
}

/// The route set a dialog we started has to use (RFC 3261 §12.1.2): the
/// Record-Route entries of the 2xx, in reverse order.
///
/// A proxy that record-routes expects every later request of the dialog to
/// come back through it; a BYE sent straight to the peer's Contact instead
/// is dropped by the proxy's NAT or rejected by the peer.
pub fn route_set_from_response(resp: &Response) -> Vec<String> {
    let mut routes: Vec<String> =
        resp.headers.get_all("record-route").into_iter().flat_map(split_contacts).collect();
    routes.reverse();
    routes
}

/// The route set of a dialog the peer started (RFC 3261 §12.1.1): the
/// Record-Route entries of its INVITE, in the order they appear.
pub fn route_set_from_request(req: &Request) -> Vec<String> {
    req.headers.get_all("record-route").into_iter().flat_map(split_contacts).collect()
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

    /// Responses keep arriving after the gateway has finished with an INVITE
    /// it sent, and each kind needs its own answer.  Dropping them all, as
    /// used to happen, cost a connected call for one lost ACK.
    #[test]
    fn late_responses_to_an_invite_we_sent_are_still_answered() {
        // Answered and ACKed: the 2xx coming again means the ACK was lost.
        assert_eq!(late_reply(true, 200), LateReply::RepeatAck);
        assert_eq!(late_reply(true, 180), LateReply::Ignore);
        assert_eq!(late_reply(true, 487), LateReply::Ignore);

        // Given up on: the 487 that follows our CANCEL (or any other
        // rejection) has to be acknowledged...
        assert_eq!(late_reply(false, 487), LateReply::AckFailure);
        assert_eq!(late_reply(false, 603), LateReply::AckFailure);
        // ...and a peer that picked up at the same moment must not be left
        // holding a call nobody else knows about.
        assert_eq!(late_reply(false, 200), LateReply::AckAndBye);
        assert_eq!(late_reply(false, 183), LateReply::Ignore);
    }

    /// The ACK for a rejection and the CANCEL both belong to the INVITE's
    /// transaction: same branch, same sequence number, no body.
    #[test]
    fn the_ack_and_the_cancel_stay_inside_the_invite_transaction() {
        let parse = |text: &str| Message::parse(text.as_bytes()).unwrap();
        let Message::Request(invite) = parse(
            "INVITE sip:phone1@10.0.0.5 SIP/2.0\r\nVia: SIP/2.0/UDP gw;branch=z9hG4bKabc\r\n\
             From: <sip:010@gw>;tag=ours\r\nTo: <sip:phone1@10.0.0.5>\r\nCall-ID: c1\r\n\
             CSeq: 7 INVITE\r\nContent-Type: application/sdp\r\nContent-Length: 3\r\n\r\nv=0",
        ) else {
            panic!("expected a request")
        };
        let Message::Response(busy) = parse(
            "SIP/2.0 486 Busy Here\r\nVia: SIP/2.0/UDP gw;branch=z9hG4bKabc\r\n\
             From: <sip:010@gw>;tag=ours\r\nTo: <sip:phone1@10.0.0.5>;tag=theirs\r\n\
             Call-ID: c1\r\nCSeq: 7 INVITE\r\n\r\n",
        ) else {
            panic!("expected a response")
        };

        let ack = ack_for_failure(&invite, &busy);
        assert_eq!(ack.method, Method::Ack);
        assert_eq!(ack.headers.top_via().unwrap().branch(), Some("z9hG4bKabc"));
        assert_eq!(ack.headers.cseq(), Some((7, Method::Ack)));
        assert_eq!(ack.headers.to().unwrap().tag(), Some("theirs"));
        assert!(ack.body.is_empty() && ack.headers.content_type().is_none());

        let cancel = cancel_for(&invite);
        assert_eq!(cancel.method, Method::Cancel);
        assert_eq!(cancel.headers.top_via().unwrap().branch(), Some("z9hG4bKabc"));
        assert_eq!(cancel.headers.cseq(), Some((7, Method::Cancel)));
        assert!(cancel.headers.to().unwrap().tag().is_none());
        assert!(cancel.body.is_empty());
    }

    /// The caller's route set is the Record-Route of the 2xx read backwards,
    /// whether the proxies wrote one header each or folded them into one.
    #[test]
    fn the_route_set_is_the_record_route_reversed() {
        let Message::Response(ok) = Message::parse(
            b"SIP/2.0 200 OK\r\nVia: SIP/2.0/UDP gw;branch=z9hG4bK1\r\n\
              Record-Route: <sip:p2.example;lr>, <sip:p1.example;lr>\r\n\
              Record-Route: <sip:p0.example;lr>\r\n\
              From: <sip:a@gw>;tag=1\r\nTo: <sip:b@h>;tag=2\r\nCall-ID: c\r\nCSeq: 1 INVITE\r\n\r\n",
        )
        .unwrap() else {
            panic!("expected a response")
        };
        assert_eq!(
            route_set_from_response(&ok),
            vec!["<sip:p0.example;lr>", "<sip:p1.example;lr>", "<sip:p2.example;lr>"]
        );
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

    // ---- the client side of an INVITE, over a real socket ----------------

    use tokio::net::UdpSocket;

    /// A SIP element on loopback and a bare socket standing in for its peer.
    async fn element_and_peer() -> (Arc<SipCore>, UdpSocket, SocketAddr) {
        let mut cfg = Config::default();
        cfg.sip.bind = "127.0.0.1:0".into();
        let (events, _unused) = mpsc::channel(8);
        let core = SipCore::new(Arc::new(cfg), events, Arc::new(AtomicBool::new(true)))
            .await
            .unwrap();
        tokio::spawn(core.clone().run());
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        (core, peer, peer_addr)
    }

    fn invite_to(core: &Arc<SipCore>, peer: SocketAddr) -> Request {
        let mut invite = core.build_request(
            Method::Invite,
            Uri::parse(&format!("sip:phone1@{peer}")).unwrap(),
            NameAddr::parse("<sip:01012345678@gw>;tag=ours").unwrap(),
            NameAddr::parse(&format!("<sip:phone1@{peer}>")).unwrap(),
            "call-under-test",
            core.next_cseq(),
            peer,
            true,
        );
        invite.headers.set("content-type", "application/sdp");
        invite.body = b"v=0\r\n".to_vec();
        invite
    }

    /// Everything the element sends the peer within `window`.
    async fn requests_within(peer: &UdpSocket, window: Duration) -> Vec<Request> {
        let deadline = tokio::time::Instant::now() + window;
        let mut buf = vec![0u8; 4096];
        let mut seen = Vec::new();
        while let Ok(Ok((len, _))) =
            tokio::time::timeout_at(deadline, peer.recv_from(&mut buf)).await
        {
            if let Some(Message::Request(req)) = Message::parse(&buf[..len]) {
                seen.push(req);
            }
        }
        seen
    }

    async fn next_request(peer: &UdpSocket) -> Request {
        let mut buf = vec![0u8; 4096];
        let (len, _) = tokio::time::timeout(Duration::from_secs(3), peer.recv_from(&mut buf))
            .await
            .expect("the element sent nothing")
            .unwrap();
        match Message::parse(&buf[..len]) {
            Some(Message::Request(req)) => req,
            other => panic!("expected a request, got {other:?}"),
        }
    }

    /// The peer's answer to `req`, with its own tag on the To header.
    async fn answer(peer: &UdpSocket, core: &Arc<SipCore>, req: &Request, code: u16) {
        let mut resp = req.reply(code, reason_phrase(code));
        if let Some(to) = resp.headers.to() {
            resp.headers.set("to", to.with_tag("theirs").to_string());
        }
        resp.headers.set("contact", format!("<sip:phone1@{}>", peer.local_addr().unwrap()));
        peer.send_to(&resp.encode(), core.transport.bound()).await.unwrap();
    }

    fn branch_of(req: &Request) -> String {
        req.headers.top_via().unwrap().branch().unwrap().to_string()
    }

    /// One lost ACK used to cost the call: the peer repeats its 200, nobody
    /// answers the repeat, and after 32 s it hangs up.
    #[tokio::test]
    async fn a_retransmitted_200_is_acknowledged_again() {
        let (core, peer, peer_addr) = element_and_peer().await;
        let invite = invite_to(&core, peer_addr);
        let mut txn = core.send_request(invite.clone(), peer_addr).await.unwrap();

        let seen = next_request(&peer).await;
        assert_eq!(seen.method, Method::Invite);
        answer(&peer, &core, &seen, 200).await;
        let ok = txn.next_response(Duration::from_secs(3)).await.expect("no 200 delivered");
        assert_eq!(ok.code, 200);

        // What the gateway does on an answer: ACK, and let go of the INVITE.
        let ack = core.build_request(
            Method::Ack,
            invite.uri.clone(),
            invite.headers.from().unwrap(),
            ok.headers.to().unwrap(),
            "call-under-test",
            invite.headers.cseq().unwrap().0,
            peer_addr,
            false,
        );
        core.invite_answered(&invite, &ack, peer_addr);
        core.send_raw(&ack, peer_addr).await.unwrap();
        drop(txn);
        assert_eq!(next_request(&peer).await.method, Method::Ack);

        // The ACK "was lost": the peer says 200 again, twice.
        for _ in 0..2 {
            answer(&peer, &core, &seen, 200).await;
            let again = next_request(&peer).await;
            assert_eq!(again.method, Method::Ack);
            assert_eq!(again.headers.cseq(), Some((invite.headers.cseq().unwrap().0, Method::Ack)));
            assert_eq!(again.headers.to().unwrap().tag(), Some("theirs"));
        }
        // And nothing but ACKs: this call is up, it must not be hung up.
        let extra = requests_within(&peer, Duration::from_millis(300)).await;
        assert!(extra.iter().all(|r| r.method != Method::Bye), "a live call was sent a BYE");
    }

    /// Giving up on a ringing INVITE: the CANCEL is repeated until it is
    /// answered, and the 487 that follows is acknowledged inside the
    /// INVITE's own transaction.
    #[tokio::test]
    async fn a_cancel_is_retransmitted_and_its_487_acknowledged() {
        let (core, peer, peer_addr) = element_and_peer().await;
        let invite = invite_to(&core, peer_addr);
        let txn = core.send_request(invite.clone(), peer_addr).await.unwrap();
        let seen = next_request(&peer).await;
        answer(&peer, &core, &seen, 180).await;

        core.cancel_invite(&invite, peer_addr);
        drop(txn);

        // Unanswered, the CANCEL comes again.
        let first = next_request(&peer).await;
        assert_eq!(first.method, Method::Cancel);
        assert_eq!(branch_of(&first), branch_of(&invite), "a CANCEL shares the INVITE's branch");
        let second = next_request(&peer).await;
        assert_eq!(second.method, Method::Cancel, "the CANCEL was sent only once");

        answer(&peer, &core, &second, 200).await;
        answer(&peer, &core, &seen, 487).await;
        let acks: Vec<Request> = requests_within(&peer, Duration::from_millis(600))
            .await
            .into_iter()
            .filter(|r| r.method == Method::Ack)
            .collect();
        assert_eq!(acks.len(), 1, "the 487 was not acknowledged exactly once");
        assert_eq!(branch_of(&acks[0]), branch_of(&invite));
        assert_eq!(acks[0].headers.to().unwrap().tag(), Some("theirs"));
    }

    /// The peer picks up in the same instant we give up.  It now believes a
    /// call is up, and used to be left believing it: no ACK, no BYE.
    #[tokio::test]
    async fn an_answer_that_crosses_the_cancel_is_acknowledged_and_hung_up() {
        let (core, peer, peer_addr) = element_and_peer().await;
        let invite = invite_to(&core, peer_addr);
        let txn = core.send_request(invite.clone(), peer_addr).await.unwrap();
        let seen = next_request(&peer).await;
        answer(&peer, &core, &seen, 180).await;

        core.cancel_invite(&invite, peer_addr);
        drop(txn);
        let cancel = next_request(&peer).await;
        assert_eq!(cancel.method, Method::Cancel);
        answer(&peer, &core, &cancel, 200).await;

        // ...but the phone had already been answered.
        answer(&peer, &core, &seen, 200).await;
        let sent = requests_within(&peer, Duration::from_millis(400)).await;
        let ack = sent.iter().find(|r| r.method == Method::Ack).expect("the 200 was not ACKed");
        let bye = sent.iter().find(|r| r.method == Method::Bye).expect("the call was not ended");
        assert_ne!(branch_of(ack), branch_of(&invite), "the ACK for a 2xx is its own transaction");
        for req in [ack, bye] {
            assert_eq!(req.headers.call_id(), Some("call-under-test"));
            assert_eq!(req.headers.from().unwrap().tag(), Some("ours"));
            assert_eq!(req.headers.to().unwrap().tag(), Some("theirs"));
        }
        let invite_cseq = invite.headers.cseq().unwrap().0;
        assert_eq!(ack.headers.cseq(), Some((invite_cseq, Method::Ack)));
        assert!(bye.headers.cseq().unwrap().0 > invite_cseq, "the BYE must be numbered after the INVITE");
        answer(&peer, &core, bye, 200).await;

        // A repeat of that 200 gets its ACK again, but one BYE is enough.
        answer(&peer, &core, &seen, 200).await;
        let sent = requests_within(&peer, Duration::from_millis(400)).await;
        assert!(sent.iter().any(|r| r.method == Method::Ack), "the repeated 200 was ignored");
        assert!(sent.iter().all(|r| r.method != Method::Bye), "the call was hung up twice");
    }
}
