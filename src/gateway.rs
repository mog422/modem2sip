//! The gateway state machine: one modem, one SIP endpoint, one call at a time.
//!
//! Everything that mutates call state runs in this single task, so there are
//! no locks around the call and no chance of two events racing each other.
//! Slow side effects (sending an SMS, submitting an MMS) are spawned off.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use zvariant::OwnedObjectPath;

use crate::audio::{AudioParams, AudioStream};
use crate::db::{Direction, NewMessage, StoredMessage};
use crate::media::{MediaConfig, MediaSession};
use crate::mm::watcher::ModemEvent;
use crate::mm::{call_state, sms_state, CallInfo, ModemHandle, SmsInfo};
use crate::sip::core::{resolve_target, ClientTxn, SipCore, SipEvent};
use crate::sip::message::{Method, Request, Response};
use crate::sip::sdp::{Codec, Sdp};
use crate::sip::transport;
use crate::sip::uri::{NameAddr, Uri};
use crate::state::Shared;

/// Events the gateway generates for itself (responses to its own requests,
/// timers, ...).
#[derive(Debug)]
enum Internal {
    SipProgress { call_id: String, code: u16 },
    SipAnswered { call_id: String, resp: Box<Response> },
    SipFailed { call_id: String, code: u16 },
    RingTimeout { call_id: String },
    /// An RFC 2833 digit arrived in the RTP stream (SIP -> modem).
    Dtmf { call_id: String, digit: char },
    /// A DTMF tone was heard in the audio from the mobile side (modem -> SIP).
    DtmfFromModem { call_id: String, digit: char },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    /// SIP -> modem: we are the UAS.
    FromSip,
    /// Modem -> SIP: we are the UAC.
    ToSip,
}

struct ActiveCall {
    role: Role,
    /// ModemManager call object.
    mm_path: Option<OwnedObjectPath>,
    peer_number: String,

    // --- SIP dialog ---
    call_id: String,
    local: NameAddr,
    remote: NameAddr,
    remote_target: Uri,
    remote_addr: SocketAddr,
    local_cseq: u32,
    /// The INVITE we received (FromSip) or sent (ToSip).
    invite: Request,
    invite_src: Option<SocketAddr>,

    // --- media ---
    media: Option<MediaSession>,
    rtp_socket: Option<tokio::net::UdpSocket>,
    local_rtp_port: u16,
    codec: Codec,
    remote_sdp: Option<Sdp>,

    answered: bool,
    ringing_sent: bool,
    cdr_id: Option<i64>,
    tasks: Vec<JoinHandle<()>>,
}

impl ActiveCall {
    fn drop_tasks(&mut self) {
        for t in self.tasks.drain(..) {
            t.abort();
        }
    }

    fn stop_media(&mut self) {
        if let Some(m) = self.media.take() {
            m.stop();
        }
        self.rtp_socket = None;
    }
}

pub struct Gateway {
    shared: Arc<Shared>,
    core: Arc<SipCore>,
    modem: Option<Arc<ModemHandle>>,
    call: Option<ActiveCall>,
    internal_tx: mpsc::Sender<Internal>,
    /// Set once `Call.SendDtmf` has failed on this modem, so the gateway
    /// stops asking and goes straight to in-band tones.
    modem_dtmf_broken: bool,
}

pub async fn run(
    shared: Arc<Shared>,
    core: Arc<SipCore>,
    mut sip_rx: mpsc::Receiver<SipEvent>,
    mut modem_rx: mpsc::Receiver<ModemEvent>,
) {
    let (internal_tx, mut internal_rx) = mpsc::channel::<Internal>(64);
    let mut gw = Gateway {
        shared,
        core,
        modem: None,
        call: None,
        internal_tx,
        modem_dtmf_broken: false,
    };
    let mut watchdog = tokio::time::interval(Duration::from_secs(2));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = watchdog.tick() => gw.check_media().await,
            Some(ev) = sip_rx.recv() => {
                let SipEvent::Request { req, src } = ev;
                if let Err(e) = gw.on_sip_request(req, src).await {
                    warn!(error = %format!("{e:#}"), "SIP request handling failed");
                }
            }
            Some(ev) = modem_rx.recv() => {
                if let Err(e) = gw.on_modem_event(ev).await {
                    warn!(error = %format!("{e:#}"), "modem event handling failed");
                }
            }
            Some(ev) = internal_rx.recv() => {
                if let Err(e) = gw.on_internal(ev).await {
                    warn!(error = %format!("{e:#}"), "internal event handling failed");
                }
            }
            else => break,
        }
    }
    warn!("gateway loop finished");
}

impl Gateway {
    // ------------------------------------------------------------ modem side

    async fn on_modem_event(&mut self, ev: ModemEvent) -> Result<()> {
        match ev {
            ModemEvent::Up(handle) => {
                self.modem = Some(handle.clone());
                self.modem_dtmf_broken = false;
                self.shared.set_modem(Some(handle)).await;
                info!("modem is up; SIP service enabled");
            }
            ModemEvent::Down { reason } => {
                self.modem = None;
                self.shared.set_modem(None).await;
                warn!(%reason, "modem is down; SIP will answer 503");
                if self.call.is_some() {
                    self.teardown_call("modem disappeared", 503).await;
                }
            }
            ModemEvent::CallAdded(path) => self.on_mm_call_added(path).await?,
            ModemEvent::CallDeleted(path) => {
                if self.call.as_ref().and_then(|c| c.mm_path.clone()) == Some(path) {
                    self.teardown_call("modem call object deleted", 200).await;
                }
            }
            ModemEvent::CallState { path, old, new, reason } => {
                self.on_mm_call_state(path, old, new, reason).await?
            }
            ModemEvent::Dtmf { path, digit } => {
                if self.call.as_ref().and_then(|c| c.mm_path.clone()) == Some(path) {
                    self.send_dtmf_info(&digit).await;
                }
            }
            ModemEvent::SmsAdded { path, received } => {
                self.on_sms_added(path, received).await?;
            }
        }
        Ok(())
    }

    async fn on_mm_call_added(&mut self, path: OwnedObjectPath) -> Result<()> {
        let Some(modem) = self.modem.clone() else { return Ok(()) };
        let info = modem.call_info(&path).await?;

        // Our own outgoing call, already tracked.
        if self.call.as_ref().and_then(|c| c.mm_path.clone()).as_ref() == Some(&path) {
            return Ok(());
        }
        if info.direction != call_state::DIR_INCOMING {
            debug!(path = path.as_str(), "ignoring non-incoming call object");
            return Ok(());
        }
        if matches!(info.state, call_state::TERMINATED) {
            return Ok(());
        }
        if self.call.is_some() {
            info!(from = %info.number, "rejecting incoming call: already busy");
            let _ = modem.hangup(&path).await;
            return Ok(());
        }

        info!(from = %info.number, "incoming call from the mobile network");
        match self.start_sip_call(path.clone(), info).await {
            Ok(()) => Ok(()),
            Err(e) => {
                error!(error = %format!("{e:#}"), "could not offer the call to SIP");
                let Some(modem) = self.modem.clone() else { return Ok(()) };
                let _ = modem.hangup(&path).await;
                Ok(())
            }
        }
    }

    /// Modem -> SIP: send an INVITE to the configured target / registration.
    async fn start_sip_call(
        &mut self,
        mm_path: OwnedObjectPath,
        info: CallInfo,
    ) -> Result<()> {
        let (target_uri, target_addr) =
            resolve_target(&self.core, self.shared.cfg.sip.call_target.as_deref()).await?;

        // Media transport is allocated before the INVITE so the SDP offer is
        // truthful even if the modem hangs up in the meantime.
        let rtp_ip = self.rtp_bind_ip(target_addr);
        let (sock, port) = MediaSession::bind_port(
            rtp_ip,
            self.shared.cfg.rtp.port_min,
            self.shared.cfg.rtp.port_max,
        )
        .await?;

        let advertised = self.core.transport.advertised_ip(target_addr);
        let sdp = Sdp::offer(
            advertised,
            port,
            Some(self.shared.cfg.rtp.dtmf_payload_type),
            self.shared.cfg.audio.period_ms,
        );

        let caller = sanitize_number(&info.number);
        let domain = self.core.domain();
        let from_user = if caller.is_empty() { "anonymous".to_string() } else { caller.clone() };
        let local_tag = crate::sip::auth::random_hex(6);
        let from = NameAddr {
            display: Some(caller.clone()),
            uri: Uri::new(Some(&from_user), &domain, None),
            params: Vec::new(),
        }
        .with_tag(&local_tag);
        let to = NameAddr::new(target_uri.bare());
        let call_id = format!("{}@modem2sip", crate::sip::auth::random_hex(10));
        let cseq = self.core.next_cseq();

        let mut invite = self.core.build_request(
            Method::Invite,
            target_uri.clone(),
            from.clone(),
            to.clone(),
            &call_id,
            cseq,
            target_addr,
            true,
        );
        invite.headers.set("content-type", "application/sdp");
        invite.headers.set("allow", "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, MESSAGE");
        invite.body = sdp.to_string().into_bytes();

        let txn = self.core.send_request(invite.clone(), target_addr).await?;

        let cdr_id = self.shared.db.start_call(Direction::Incoming, &caller).await.ok();

        let mut call = ActiveCall {
            role: Role::ToSip,
            mm_path: Some(mm_path),
            peer_number: caller,
            call_id: call_id.clone(),
            local: from,
            remote: to,
            remote_target: target_uri,
            remote_addr: target_addr,
            local_cseq: cseq,
            invite,
            invite_src: None,
            media: None,
            rtp_socket: Some(sock),
            local_rtp_port: port,
            codec: Codec::Pcmu,
            remote_sdp: None,
            answered: false,
            ringing_sent: false,
            cdr_id,
            tasks: Vec::new(),
        };

        let tx = self.internal_tx.clone();
        let ring_timeout = Duration::from_secs(self.shared.cfg.sip.ring_timeout_secs);
        let id = call_id.clone();
        call.tasks.push(tokio::spawn(invite_response_task(txn, id, tx, ring_timeout)));

        self.call = Some(call);
        Ok(())
    }

    async fn on_mm_call_state(
        &mut self,
        path: OwnedObjectPath,
        _old: i32,
        new: i32,
        reason: u32,
    ) -> Result<()> {
        let Some((role, ringing_sent, answered)) = self
            .call
            .as_ref()
            .filter(|c| c.mm_path.as_ref() == Some(&path))
            .map(|c| (c.role, c.ringing_sent, c.answered))
        else {
            return Ok(());
        };
        debug!(state = call_state::state_name(new), reason, "modem call state");

        match new {
            call_state::RINGING_OUT | call_state::DIALING => {
                if role == Role::FromSip && !ringing_sent {
                    self.send_provisional(180).await;
                }
            }
            call_state::ACTIVE => {
                if !answered {
                    self.on_modem_answered().await?;
                }
            }
            call_state::TERMINATED => {
                let code = match reason {
                    call_state::REASON_REFUSED_OR_BUSY => 486,
                    call_state::REASON_ERROR | call_state::REASON_AUDIO_SETUP_FAILED => 500,
                    _ if answered => 200,
                    _ => 480,
                };
                self.teardown_call("the mobile call ended", code).await;
            }
            _ => {}
        }
        Ok(())
    }

    /// The mobile network reports the call as connected.
    async fn on_modem_answered(&mut self) -> Result<()> {
        let Some((role, cdr_id, remote_sdp, codec, peer)) = self.call.as_ref().map(|c| {
            (c.role, c.cdr_id, c.remote_sdp.clone(), c.codec, c.peer_number.clone())
        }) else {
            return Ok(());
        };
        if let Some(c) = self.call.as_mut() {
            c.answered = true;
        }
        if let Some(id) = cdr_id {
            let _ = self.shared.db.call_answered(id).await;
        }

        match role {
            Role::FromSip => {
                // Answer the SIP side now that the network is through.
                let remote_sdp = remote_sdp.ok_or_else(|| anyhow!("no remote SDP"))?;
                let remote_media = SocketAddr::new(remote_sdp.address, remote_sdp.port);
                self.start_media(remote_media, codec).await?;

                let dtmf_pt = self.shared.cfg.rtp.dtmf_payload_type;
                let ptime = self.shared.cfg.audio.period_ms;
                let core = self.core.clone();
                let Some(call) = self.call.as_ref() else { return Ok(()) };
                let advertised = core.transport.advertised_ip(call.remote_addr);
                let answer = remote_sdp.answer(
                    advertised,
                    call.local_rtp_port,
                    codec,
                    Some(dtmf_pt),
                    ptime,
                );
                let src = call.invite_src.unwrap_or(call.remote_addr);
                let mut resp = core.make_response(&call.invite, 200, None);
                resp.headers.set("content-type", "application/sdp");
                resp.headers.set("to", call.local.to_string());
                resp.headers.set("allow", "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, MESSAGE");
                resp.body = answer.to_string().into_bytes();
                core.respond(&call.invite, src, resp).await?;
                info!(peer = %peer, "call answered by the network");
            }
            Role::ToSip => {
                // Media is already running (started when SIP answered).
                info!(peer = %peer, "modem call is active");
            }
        }
        Ok(())
    }

    // -------------------------------------------------------------- SIP side

    async fn on_sip_request(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        match req.method {
            Method::Invite => self.on_sip_invite(req, src).await,
            Method::Ack => {
                debug!("ACK received");
                Ok(())
            }
            Method::Bye => self.on_sip_bye(req, src).await,
            Method::Cancel => self.on_sip_cancel(req, src).await,
            Method::Info => self.on_sip_info(req, src).await,
            Method::Message => self.on_sip_message(req, src).await,
            _ => {
                let resp = self.core.make_response(&req, 405, None);
                self.core.respond(&req, src, resp).await
            }
        }
    }

    async fn reply(&self, req: &Request, src: SocketAddr, code: u16) -> Result<()> {
        let mut resp = self.core.make_response(req, code, None);
        if code == 503 {
            resp.headers
                .set("retry-after", self.shared.cfg.sip.retry_after_secs.to_string());
            resp.headers.set("warning", "399 modem2sip \"modem not available\"");
        }
        self.core.respond(req, src, resp).await
    }

    async fn on_sip_invite(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        // Re-INVITE inside the existing dialog: accept without changing media.
        if let Some(call) = &self.call {
            if req.headers.call_id() == Some(call.call_id.as_str()) {
                let mut resp = self.core.make_response(&req, 200, None);
                if let Some(sdp) = call.remote_sdp.as_ref() {
                    let advertised = self.core.transport.advertised_ip(src);
                    let answer = sdp.answer(
                        advertised,
                        call.local_rtp_port,
                        call.codec,
                        Some(self.shared.cfg.rtp.dtmf_payload_type),
                        self.shared.cfg.audio.period_ms,
                    );
                    resp.headers.set("content-type", "application/sdp");
                    resp.body = answer.to_string().into_bytes();
                }
                return self.core.respond(&req, src, resp).await;
            }
            info!("rejecting INVITE: the modem is already on a call");
            return self.reply(&req, src, 486).await;
        }

        let Some(modem) = self.modem.clone() else {
            info!("rejecting INVITE: no modem");
            return self.reply(&req, src, 503).await;
        };

        let number = sanitize_number(req.uri.user.as_deref().unwrap_or(""));
        if number.is_empty() {
            return self.reply(&req, src, 484).await;
        }

        let Some(remote_sdp) = Sdp::parse(&req.body_str()) else {
            warn!("INVITE without a usable SDP offer");
            return self.reply(&req, src, 488).await;
        };
        let Some(codec) = remote_sdp.negotiate() else {
            warn!(offered = ?remote_sdp.payload_types, "no common codec (PCMU/PCMA required)");
            return self.reply(&req, src, 488).await;
        };

        self.reply(&req, src, 100).await?;

        let rtp_ip = self.rtp_bind_ip(src);
        let (sock, port) = MediaSession::bind_port(
            rtp_ip,
            self.shared.cfg.rtp.port_min,
            self.shared.cfg.rtp.port_max,
        )
        .await?;

        let local_tag = crate::sip::auth::random_hex(6);
        let to = req
            .headers
            .to()
            .ok_or_else(|| anyhow!("INVITE without To"))?
            .with_tag(&local_tag);
        let from = req.headers.from().ok_or_else(|| anyhow!("INVITE without From"))?;
        let remote_target = req
            .headers
            .contact()
            .map(|c| c.uri)
            .unwrap_or_else(|| from.uri.clone());
        let call_id = req
            .headers
            .call_id()
            .ok_or_else(|| anyhow!("INVITE without Call-ID"))?
            .to_string();

        info!(%number, "placing a call through the modem");
        let mm_path = match modem.dial(&number).await {
            Ok(p) => p,
            Err(e) => {
                error!(error = %format!("{e:#}"), "the modem refused to dial");
                return self.reply(&req, src, 503).await;
            }
        };

        let cdr_id = self.shared.db.start_call(Direction::Outgoing, &number).await.ok();

        self.call = Some(ActiveCall {
            role: Role::FromSip,
            mm_path: Some(mm_path),
            peer_number: number,
            call_id,
            local: to,
            remote: from,
            remote_target,
            remote_addr: src,
            local_cseq: self.core.next_cseq(),
            invite: req,
            invite_src: Some(src),
            media: None,
            rtp_socket: Some(sock),
            local_rtp_port: port,
            codec,
            remote_sdp: Some(remote_sdp),
            answered: false,
            ringing_sent: false,
            cdr_id,
            tasks: Vec::new(),
        });
        Ok(())
    }

    async fn on_sip_bye(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        let matches = self
            .call
            .as_ref()
            .map(|c| Some(c.call_id.as_str()) == req.headers.call_id())
            .unwrap_or(false);
        let code = if matches { 200 } else { 481 };
        let resp = self.core.make_response(&req, code, None);
        self.core.respond(&req, src, resp).await?;
        if matches {
            info!("SIP peer hung up");
            if let Some(modem) = self.modem.clone() {
                if let Some(path) = self.call.as_ref().and_then(|c| c.mm_path.clone()) {
                    let _ = modem.hangup(&path).await;
                }
            }
            self.finish_call("sip bye").await;
        }
        Ok(())
    }

    async fn on_sip_cancel(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        let resp = self.core.make_response(&req, 200, None);
        self.core.respond(&req, src, resp).await?;

        let matches = self
            .call
            .as_ref()
            .map(|c| Some(c.call_id.as_str()) == req.headers.call_id())
            .unwrap_or(false);
        if matches {
            info!("SIP peer cancelled the call");
            if let Some(call) = self.call.as_ref() {
                let invite = call.invite.clone();
                let dest = call.invite_src.unwrap_or(call.remote_addr);
                let mut resp = self.core.make_response(&invite, 487, None);
                resp.headers.set("to", call.local.to_string());
                let _ = self.core.respond(&invite, dest, resp).await;
            }
            if let Some(modem) = self.modem.clone() {
                if let Some(path) = self.call.as_ref().and_then(|c| c.mm_path.clone()) {
                    let _ = modem.hangup(&path).await;
                }
            }
            self.finish_call("sip cancel").await;
        }
        Ok(())
    }

    async fn on_sip_info(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        let body = req.body_str();
        let digits = parse_dtmf_info(req.headers.content_type().unwrap_or(""), &body);
        // Only accept INFO for the dialog that owns the current call.
        let in_dialog = self
            .call
            .as_ref()
            .map(|c| Some(c.call_id.as_str()) == req.headers.call_id())
            .unwrap_or(false);
        let mm_path = if in_dialog {
            self.call.as_ref().and_then(|c| c.mm_path.clone())
        } else {
            None
        };
        let mut code = 200;
        match digits {
            Some(d) if d.is_empty() => {}
            Some(d) if mm_path.is_some() => {
                if !self.deliver_dtmf(&d).await {
                    code = 500;
                }
            }
            Some(_) => code = 481,
            None => code = 415,
        }
        let resp = self.core.make_response(&req, code, None);
        self.core.respond(&req, src, resp).await
    }

    /// SIP MESSAGE: send an SMS (text/plain) or an MMS (JSON body).
    async fn on_sip_message(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        let Some(modem) = self.modem.clone() else {
            return self.reply(&req, src, 503).await;
        };
        let content_type = req.headers.content_type().unwrap_or("text/plain").to_string();
        let to = req
            .headers
            .to()
            .map(|t| t.uri)
            .unwrap_or_else(|| req.uri.clone());
        let number = sanitize_number(to.user.as_deref().unwrap_or(""));

        if content_type.contains("json") {
            // MMS submission over SIP.
            let mms = self.shared.mms.clone();
            let core = self.core.clone();
            let body = req.body.clone();
            let req2 = req.clone();
            tokio::spawn(async move {
                let parsed: Result<crate::mms::SendRequest> = serde_json::from_slice(&body)
                    .context("parsing the MMS JSON body");
                let code = match parsed {
                    Ok(mut sr) => {
                        if sr.to.is_empty() && !number.is_empty() {
                            sr.to.push(number.clone());
                        }
                        match mms.send(sr).await {
                            Ok(id) => {
                                info!(id, "MMS submitted from SIP");
                                202
                            }
                            Err(e) => {
                                warn!(error = %format!("{e:#}"), "MMS submission failed");
                                500
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %format!("{e:#}"), "bad MMS request body");
                        400
                    }
                };
                let resp = core.make_response(&req2, code, None);
                let _ = core.respond(&req2, src, resp).await;
            });
            return Ok(());
        }

        if !content_type.starts_with("text/plain") {
            return self.reply(&req, src, 415).await;
        }
        if number.is_empty() {
            return self.reply(&req, src, 484).await;
        }

        let text = req.body_str();
        let core = self.core.clone();
        let shared = self.shared.clone();
        let req2 = req.clone();
        tokio::spawn(async move {
            let delivery_report = shared.cfg.sms.delivery_report;
            let code = match modem.send_sms(&number, &text, delivery_report).await {
                Ok(path) => {
                    info!(to = %number, "SMS submitted to the modem");
                    let _ = shared
                        .db
                        .insert_message(NewMessage {
                            kind: "sms",
                            direction: Direction::Outgoing,
                            peer: number.clone(),
                            own_number: None,
                            subject: None,
                            text: Some(text.clone()),
                            timestamp: None,
                            status: "sent".into(),
                            external_id: Some(path.to_string()),
                            raw: None,
                        })
                        .await;
                    202
                }
                Err(e) => {
                    warn!(to = %number, error = %format!("{e:#}"), "sending the SMS failed");
                    500
                }
            };
            let resp = core.make_response(&req2, code, None);
            let _ = core.respond(&req2, src, resp).await;
        });
        Ok(())
    }

    // --------------------------------------------------------- internal events

    async fn on_internal(&mut self, ev: Internal) -> Result<()> {
        match ev {
            Internal::SipProgress { call_id, code } => {
                if self.call_id_matches(&call_id) {
                    debug!(code, "SIP peer is ringing");
                }
                Ok(())
            }
            Internal::SipAnswered { call_id, resp } => {
                if !self.call_id_matches(&call_id) {
                    return Ok(());
                }
                self.on_sip_answered(*resp).await
            }
            Internal::SipFailed { call_id, code } => {
                if !self.call_id_matches(&call_id) {
                    return Ok(());
                }
                warn!(code, "the SIP side rejected the call");
                if let (Some(modem), Some(path)) =
                    (self.modem.clone(), self.call.as_ref().and_then(|c| c.mm_path.clone()))
                {
                    let _ = modem.hangup(&path).await;
                }
                self.finish_call("sip rejected").await;
                Ok(())
            }
            Internal::Dtmf { call_id, digit } => {
                if self.call_id_matches(&call_id) {
                    self.deliver_dtmf(&digit.to_string()).await;
                }
                Ok(())
            }
            Internal::DtmfFromModem { call_id, digit } => {
                if self.call_id_matches(&call_id) {
                    info!(%digit, "DTMF heard from the mobile side, relaying as INFO");
                    self.send_dtmf_info(&digit.to_string()).await;
                }
                Ok(())
            }
            Internal::RingTimeout { call_id } => {
                if !self.call_id_matches(&call_id) {
                    return Ok(());
                }
                warn!("no answer from the SIP side, cancelling");
                self.cancel_outgoing_invite().await;
                if let (Some(modem), Some(path)) =
                    (self.modem.clone(), self.call.as_ref().and_then(|c| c.mm_path.clone()))
                {
                    let _ = modem.hangup(&path).await;
                }
                self.finish_call("ring timeout").await;
                Ok(())
            }
        }
    }

    /// Send digits towards the mobile network.  Returns false only when
    /// every configured method failed.
    ///
    /// On a VoLTE call there is no CS domain, and this modem's firmware maps
    /// both `Call.SendDtmf` (QMI) and `AT+VTS` onto a request the IMS network
    /// rejects ("network rejected request").  The gateway therefore falls
    /// back to playing the tones into the uplink audio itself.
    async fn deliver_dtmf(&mut self, digits: &str) -> bool {
        use crate::config::DtmfMethod;
        let method = self.shared.cfg.rtp.dtmf_method;
        if method == DtmfMethod::None || digits.is_empty() {
            return true;
        }

        let try_modem = matches!(method, DtmfMethod::Modem | DtmfMethod::Auto)
            && !(method == DtmfMethod::Auto && self.modem_dtmf_broken);

        if try_modem {
            let target = self
                .call
                .as_ref()
                .and_then(|c| c.mm_path.clone())
                .zip(self.modem.clone());
            if let Some((path, modem)) = target {
                match modem.send_dtmf(&path, digits).await {
                    Ok(()) => {
                        info!(digits, "DTMF sent through ModemManager");
                        return true;
                    }
                    Err(e) => {
                        if method == DtmfMethod::Modem {
                            warn!(error = %format!("{e:#}"), "SendDtmf failed");
                            return false;
                        }
                        warn!(
                            error = %format!("{e:#}"),
                            "SendDtmf failed; falling back to in-band tones for the rest of \
                             this modem's lifetime"
                        );
                        self.modem_dtmf_broken = true;
                    }
                }
            } else if method == DtmfMethod::Modem {
                return false;
            }
        }

        if matches!(method, DtmfMethod::Inband | DtmfMethod::Auto) {
            let (tone_ms, gap_ms) =
                (self.shared.cfg.rtp.dtmf_tone_ms, self.shared.cfg.rtp.dtmf_gap_ms);
            if let Some(media) = self.call.as_ref().and_then(|c| c.media.as_ref()) {
                let n = media.send_dtmf_inband(digits, tone_ms, gap_ms);
                if n > 0 {
                    info!(digits, "DTMF played in-band into the uplink audio");
                    return true;
                }
                warn!(digits, "no usable DTMF digit in the request");
                return false;
            }
            warn!("cannot play DTMF in-band: no media session");
        }
        false
    }

    fn call_id_matches(&self, call_id: &str) -> bool {
        self.call.as_ref().map(|c| c.call_id == call_id).unwrap_or(false)
    }

    /// Our INVITE (modem -> SIP) was answered with a 2xx.
    async fn on_sip_answered(&mut self, resp: Response) -> Result<()> {
        if self.call.is_none() {
            return Ok(());
        }
        // Learn the remote tag and the dialog target from the 2xx.
        let contact_addr = match resp.headers.contact() {
            Some(c) => transport::resolve_uri(&c.uri).await.ok().map(|a| (c.uri, a)),
            None => None,
        };
        if let Some(call) = self.call.as_mut() {
            if let Some(to) = resp.headers.to() {
                call.remote = to;
            }
            if let Some((uri, addr)) = contact_addr {
                call.remote_target = uri;
                call.remote_addr = addr;
            }
        }

        // ACK first: the peer stops retransmitting the 200 as soon as it
        // arrives, which matters more than the media being up.
        let (ack, dest) = {
            let Some(call) = self.call.as_ref() else { return Ok(()) };
            (build_ack(&self.core, call), call.remote_addr)
        };
        self.core.send_raw(&ack, dest).await?;

        let Some(sdp) = Sdp::parse(&resp.body_str()) else {
            warn!("2xx without SDP; dropping the call");
            self.teardown_call("no SDP in the answer", 200).await;
            return Ok(());
        };
        let codec = sdp.negotiate().unwrap_or(Codec::Pcmu);
        let remote_media = SocketAddr::new(sdp.address, sdp.port);
        if let Some(call) = self.call.as_mut() {
            call.codec = codec;
            call.remote_sdp = Some(sdp);
        }

        if let Err(e) = self.start_media(remote_media, codec).await {
            error!(error = %format!("{e:#}"), "media setup failed");
            self.teardown_call("media setup failed", 500).await;
            return Ok(());
        }

        // Only now tell the network to pick up, so the first words are not
        // lost while ALSA is still opening.
        if let (Some(modem), Some(path)) =
            (self.modem.clone(), self.call.as_ref().and_then(|c| c.mm_path.clone()))
        {
            if let Err(e) = modem.accept(&path).await {
                error!(error = %format!("{e:#}"), "Call.Accept failed");
                self.teardown_call("accept failed", 500).await;
                return Ok(());
            }
        }
        if let Some(call) = self.call.as_mut() {
            call.answered = true;
            if let Some(id) = call.cdr_id {
                let _ = self.shared.db.call_answered(id).await;
            }
        }
        info!("call connected (modem -> SIP)");
        Ok(())
    }

    // ------------------------------------------------------------------ media

    fn rtp_bind_ip(&self, peer: SocketAddr) -> std::net::IpAddr {
        if let Some(cfg_ip) = self
            .shared
            .cfg
            .rtp
            .bind
            .as_deref()
            .and_then(|s| s.parse::<std::net::IpAddr>().ok())
        {
            return cfg_ip;
        }
        let bound = self.core.transport.bound().ip();
        if bound.is_unspecified() {
            // Bind the same address we advertise so symmetric RTP works.
            self.core.transport.advertised_ip(peer)
        } else {
            bound
        }
    }

    /// The ALSA threads give up on a card that keeps failing (a modem that is
    /// being unplugged, for instance).  A call with no audio is worse than no
    /// call, so end it.
    async fn check_media(&mut self) {
        let dead = self
            .call
            .as_ref()
            .and_then(|c| c.media.as_ref())
            .map(|m| m.audio_failed())
            .unwrap_or(false);
        if dead {
            self.teardown_call("the modem audio stream died", 200).await;
        }
    }

    async fn start_media(&mut self, remote: SocketAddr, codec: Codec) -> Result<()> {
        let modem = self.modem.clone().ok_or_else(|| anyhow!("no modem"))?;
        let call_info = match self.call.as_ref().and_then(|c| c.mm_path.clone()) {
            Some(path) => modem.call_info(&path).await.ok(),
            None => None,
        };
        let (capture, playback) = resolve_audio_devices(&self.shared, &modem, call_info.as_ref())?;

        // A modem that reset itself since the last call would have dropped its
        // USB voice path again, which shows up as a perfectly connected but
        // completely silent call.  Cheap to re-check right here.
        if crate::vendor::applies(self.shared.cfg.audio.vendor_audio_setup, &modem.info) {
            if let Err(e) = crate::vendor::enable_usb_audio(&modem.info, false).await {
                warn!(
                    error = %format!("{e:#}"),
                    "could not confirm the modem's USB voice path; audio may be silent"
                );
            }
        }

        let params = AudioParams {
            capture_device: capture,
            playback_device: playback,
            card_rate: self.shared.cfg.audio.rate,
            period_ms: self.shared.cfg.audio.period_ms,
            periods: self.shared.cfg.audio.periods,
            tx_gain: self.shared.cfg.audio.tx_gain,
            rx_gain: self.shared.cfg.audio.rx_gain,
        };
        let ptime_ms = self.shared.cfg.audio.period_ms;
        let jitter_ms = self.shared.cfg.rtp.jitter_ms;
        let symmetric = self.shared.cfg.rtp.symmetric;
        let detect_inband_dtmf = self.shared.cfg.rtp.detect_inband_dtmf;

        let audio = tokio::task::spawn_blocking(move || AudioStream::start(params))
            .await
            .context("audio thread panicked")??;

        let call = self.call.as_mut().ok_or_else(|| anyhow!("call vanished"))?;
        let sock = call.rtp_socket.take().ok_or_else(|| anyhow!("no RTP socket"))?;
        let dtmf_payload_type = call.remote_sdp.as_ref().and_then(|s| s.telephone_event);

        let (dtmf_tx, mut dtmf_rx) = mpsc::channel::<char>(16);
        let (inband_tx, mut inband_rx) = mpsc::channel::<char>(16);
        let media = MediaSession::start(
            sock,
            call.local_rtp_port,
            remote,
            audio,
            MediaConfig {
                codec,
                dtmf_payload_type,
                ptime_ms,
                jitter_ms,
                symmetric,
                detect_inband_dtmf,
            },
            dtmf_tx,
            inband_tx,
        );
        call.media = Some(media);

        // Tones heard from the mobile side become SIP INFO for the peer.
        let internal_tx2 = self.internal_tx.clone();
        let call_id2 = call.call_id.clone();
        call.tasks.push(tokio::spawn(async move {
            while let Some(d) = inband_rx.recv().await {
                let _ = internal_tx2
                    .send(Internal::DtmfFromModem { call_id: call_id2.clone(), digit: d })
                    .await;
            }
        }));

        // RFC 2833 digits go back through the gateway loop so they take the
        // same path (and the same fallback) as digits from SIP INFO.
        let internal_tx = self.internal_tx.clone();
        let call_id = call.call_id.clone();
        call.tasks.push(tokio::spawn(async move {
            while let Some(d) = dtmf_rx.recv().await {
                debug!(digit = %d, "RFC2833 digit received");
                let _ = internal_tx
                    .send(Internal::Dtmf { call_id: call_id.clone(), digit: d })
                    .await;
            }
        }));
        info!(remote = %remote, codec = ?codec, "media session started");
        Ok(())
    }

    // ---------------------------------------------------------------- teardown

    /// End the call, informing the SIP side with `code` (200 => BYE).
    async fn teardown_call(&mut self, reason: &str, code: u16) {
        struct Snapshot {
            role: Role,
            answered: bool,
            invite: Request,
            invite_dest: SocketAddr,
            local_header: String,
            remote_addr: SocketAddr,
            mm_path: Option<OwnedObjectPath>,
            bye: Option<Request>,
        }

        let Some(snap) = self.call.as_ref().map(|c| Snapshot {
            role: c.role,
            answered: c.answered,
            invite: c.invite.clone(),
            invite_dest: c.invite_src.unwrap_or(c.remote_addr),
            local_header: c.local.to_string(),
            remote_addr: c.remote_addr,
            mm_path: c.mm_path.clone(),
            bye: c
                .answered
                .then(|| build_in_dialog_request(&self.core, c, Method::Bye)),
        }) else {
            return;
        };
        info!(reason, code, "tearing down the call");

        if let Some(bye) = snap.bye {
            let core = self.core.clone();
            let dest = snap.remote_addr;
            tokio::spawn(async move {
                if let Ok(mut txn) = core.send_request(bye, dest).await {
                    let _ = txn.final_response(Duration::from_secs(5)).await;
                }
            });
        } else {
            match snap.role {
                Role::FromSip => {
                    let mut resp = self.core.make_response(&snap.invite, code.max(400), None);
                    resp.headers.set("to", snap.local_header);
                    if code == 503 {
                        resp.headers
                            .set("retry-after", self.shared.cfg.sip.retry_after_secs.to_string());
                    }
                    let _ = self.core.respond(&snap.invite, snap.invite_dest, resp).await;
                }
                Role::ToSip => self.cancel_outgoing_invite().await,
            }
        }

        if let (Some(modem), Some(path)) = (self.modem.clone(), snap.mm_path) {
            let _ = modem.hangup(&path).await;
        }
        self.finish_call(reason).await;
    }

    /// CANCEL an INVITE we sent that has not been answered yet.
    async fn cancel_outgoing_invite(&mut self) {
        let Some(call) = self.call.as_ref() else { return };
        if call.role != Role::ToSip || call.answered {
            return;
        }
        let mut cancel = call.invite.clone();
        cancel.method = Method::Cancel;
        cancel.body.clear();
        cancel.headers.set("cseq", format!("{} CANCEL", call.local_cseq));
        cancel.headers.remove("content-type");
        let dest = call.remote_addr;
        let _ = self.core.send_raw(&cancel, dest).await;
    }

    async fn finish_call(&mut self, disposition: &str) {
        if let Some(mut call) = self.call.take() {
            call.stop_media();
            call.drop_tasks();
            if let Some(id) = call.cdr_id {
                let _ = self.shared.db.call_ended(id, disposition).await;
            }
        }
    }

    async fn send_provisional(&mut self, code: u16) {
        let Some((invite, dest, to_header)) = self.call.as_ref().map(|c| {
            (c.invite.clone(), c.invite_src.unwrap_or(c.remote_addr), c.local.to_string())
        }) else {
            return;
        };
        if code == 180 {
            if let Some(c) = self.call.as_mut() {
                c.ringing_sent = true;
            }
        }
        let mut resp = self.core.make_response(&invite, code, None);
        resp.headers.set("to", to_header);
        let _ = self.core.respond(&invite, dest, resp).await;
    }

    async fn send_dtmf_info(&self, digit: &str) {
        let Some((mut req, dest)) = self
            .call
            .as_ref()
            .map(|c| (build_in_dialog_request(&self.core, c, Method::Info), c.remote_addr))
        else {
            return;
        };
        req.headers.set("content-type", "application/dtmf-relay");
        req.body = format!("Signal={digit}\r\nDuration=250\r\n").into_bytes();
        let core = self.core.clone();
        tokio::spawn(async move {
            if let Ok(mut txn) = core.send_request(req, dest).await {
                let _ = txn.final_response(Duration::from_secs(5)).await;
            }
        });
    }

    // --------------------------------------------------------------- messaging

    async fn on_sms_added(&mut self, path: OwnedObjectPath, received: bool) -> Result<()> {
        let Some(modem) = self.modem.clone() else { return Ok(()) };
        let info = match modem.sms_info(&path).await {
            Ok(i) => i,
            Err(e) => {
                debug!(path = path.as_str(), error = %format!("{e:#}"), "SMS vanished before it could be read");
                return Ok(());
            }
        };

        let is_incoming = info.pdu_type == sms_state::PDU_DELIVER
            || (received && info.pdu_type != sms_state::PDU_SUBMIT);
        let is_ready = matches!(info.state, sms_state::STATE_RECEIVED | sms_state::STATE_STORED);
        if !is_incoming || !is_ready {
            debug!(
                path = path.as_str(),
                state = info.state,
                pdu_type = info.pdu_type,
                "ignoring SMS object (not an incoming message)"
            );
            return Ok(());
        }

        if info.looks_like_wap_push() {
            self.handle_mms_push(modem, path, info).await
        } else {
            self.handle_text_sms(modem, path, info).await
        }
    }

    async fn handle_text_sms(
        &mut self,
        modem: Arc<ModemHandle>,
        path: OwnedObjectPath,
        info: SmsInfo,
    ) -> Result<()> {
        let peer = sanitize_number(&info.number);
        // Object paths are reused after a restart, so the identity also uses
        // the network timestamp and the sender.
        let external_id = format!(
            "{}|{}|{}",
            path.as_str(),
            info.timestamp.clone().unwrap_or_default(),
            peer
        );

        let stored = self
            .shared
            .db
            .insert_message(NewMessage {
                kind: "sms",
                direction: Direction::Incoming,
                peer: peer.clone(),
                own_number: modem.info.own_number.clone(),
                subject: None,
                text: Some(info.text.clone()),
                timestamp: info.timestamp.clone(),
                status: "received".into(),
                external_id: Some(external_id),
                raw: if info.data.is_empty() { None } else { Some(info.data.clone()) },
            })
            .await?;

        match stored {
            Some(id) => {
                info!(id, from = %peer, chars = info.text.chars().count(), "SMS received");
                if self.shared.cfg.sms.notify_sip {
                    self.notify_sip(&peer, "text/plain", info.text.clone());
                }
            }
            None => debug!("duplicate SMS ignored"),
        }

        if self.shared.cfg.sms.delete_from_modem {
            if let Err(e) = modem.delete_sms(&path).await {
                debug!(error = %format!("{e:#}"), "could not delete the SMS from the modem");
            }
        }
        Ok(())
    }

    async fn handle_mms_push(
        &mut self,
        modem: Arc<ModemHandle>,
        path: OwnedObjectPath,
        info: SmsInfo,
    ) -> Result<()> {
        let shared = self.shared.clone();
        let core = self.core.clone();
        let delete = self.shared.cfg.sms.delete_from_modem;
        let peer_fallback = sanitize_number(&info.number);

        // Retrieval talks to the MMSC over HTTP: never block the gateway.
        tokio::spawn(async move {
            match shared.mms.handle_push(&info).await {
                Ok(Some(id)) => {
                    if let Ok(Some(msg)) = shared.db.get_message(id).await {
                        let body = format_mms_summary(&shared, &msg);
                        let peer = if msg.peer.is_empty() { peer_fallback } else { msg.peer.clone() };
                        notify_sip_message(shared.clone(), core, &peer, "text/plain", body).await;
                    }
                }
                Ok(None) => {}
                Err(e) => warn!(error = %format!("{e:#}"), "handling the MMS notification failed"),
            }
            if delete {
                let _ = modem.delete_sms(&path).await;
            }
        });
        Ok(())
    }

    /// Fire-and-forget: a SIP peer that does not answer must never stall the
    /// gateway loop (a MESSAGE transaction can take 32 s to time out).
    fn notify_sip(&self, peer: &str, content_type: &'static str, body: String) {
        let shared = self.shared.clone();
        let core = self.core.clone();
        let peer = peer.to_string();
        tokio::spawn(async move {
            notify_sip_message(shared, core, &peer, content_type, body).await;
        });
    }
}

/// Deliver an incoming message to the SIP side as a MESSAGE request.
pub async fn notify_sip_message(
    shared: Arc<Shared>,
    core: Arc<SipCore>,
    peer: &str,
    content_type: &str,
    body: String,
) {
    let target_cfg = shared
        .cfg
        .sip
        .sms_target
        .clone()
        .or_else(|| shared.cfg.sip.call_target.clone());
    let (target_uri, addr) = match resolve_target(&core, target_cfg.as_deref()).await {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %format!("{e:#}"), "no SIP destination for the incoming message");
            return;
        }
    };

    let domain = core.domain();
    let from_user = if peer.is_empty() { "unknown".to_string() } else { peer.to_string() };
    let from = NameAddr {
        display: Some(from_user.clone()),
        uri: Uri::new(Some(&from_user), &domain, None),
        params: Vec::new(),
    }
    .with_tag(&crate::sip::auth::random_hex(6));
    let to = NameAddr::new(target_uri.bare());
    let call_id = format!("{}@modem2sip", crate::sip::auth::random_hex(10));

    let mut req = core.build_request(
        Method::Message,
        target_uri,
        from,
        to,
        &call_id,
        core.next_cseq(),
        addr,
        false,
    );
    req.headers.set("content-type", content_type);
    req.body = body.into_bytes();

    let creds = shared
        .cfg
        .sip
        .register
        .as_ref()
        .map(|u| (u.username.clone(), u.password.clone()));
    let creds_ref = creds.as_ref().map(|(u, p)| (u.as_str(), p.as_str()));

    match core.transact(req, addr, creds_ref, Duration::from_secs(32)).await {
        Ok(resp) if resp.is_success() => debug!(code = resp.code, "message delivered over SIP"),
        Ok(resp) => warn!(code = resp.code, "SIP peer rejected the message"),
        Err(e) => warn!(error = %format!("{e:#}"), "could not deliver the message over SIP"),
    }
}

/// Human readable, deliberately small: the full MMS with its attachments
/// stays in SQLite and is reachable over the HTTP API.
pub fn format_mms_summary(shared: &Arc<Shared>, msg: &StoredMessage) -> String {
    let base = shared.http_base_url();
    let mut out = String::new();
    out.push_str(&format!("[MMS] from {}\n", msg.peer));
    if let Some(subject) = &msg.subject {
        if !subject.is_empty() {
            out.push_str(&format!("Subject: {subject}\n"));
        }
    }
    if let Some(text) = &msg.text {
        if !text.is_empty() {
            out.push('\n');
            out.push_str(text);
            out.push('\n');
        }
    }
    if !msg.attachments.is_empty() {
        out.push_str(&format!("\n-- {} attachment(s) --\n", msg.attachments.len()));
        for att in &msg.attachments {
            out.push_str(&format!(
                "{}. {} ({}, {})\n   {}/messages/{}/attachments/{}\n",
                att.index,
                att.name.clone().unwrap_or_else(|| format!("part{}", att.index)),
                att.content_type,
                human_size(att.size),
                base,
                msg.id,
                att.index
            ));
        }
    }
    if msg.status != "received" {
        out.push_str(&format!("\n(status: {})\n", msg.status));
    }
    out
}

fn human_size(bytes: i64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

async fn invite_response_task(
    mut txn: ClientTxn,
    call_id: String,
    tx: mpsc::Sender<Internal>,
    ring_timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + ring_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let _ = tx.send(Internal::RingTimeout { call_id }).await;
            return;
        }
        match txn.next_response(remaining).await {
            Some(resp) if resp.is_provisional() => {
                if resp.code > 100 {
                    let _ = tx
                        .send(Internal::SipProgress { call_id: call_id.clone(), code: resp.code })
                        .await;
                }
            }
            Some(resp) if resp.is_success() => {
                let _ = tx
                    .send(Internal::SipAnswered { call_id, resp: Box::new(resp) })
                    .await;
                return;
            }
            Some(resp) => {
                let _ = tx.send(Internal::SipFailed { call_id, code: resp.code }).await;
                return;
            }
            None => {
                let _ = tx.send(Internal::RingTimeout { call_id }).await;
                return;
            }
        }
    }
}

fn build_in_dialog_request(core: &Arc<SipCore>, call: &ActiveCall, method: Method) -> Request {
    let cseq = core.next_cseq();
    core.build_request(
        method,
        call.remote_target.clone(),
        call.local.clone(),
        call.remote.clone(),
        &call.call_id,
        cseq,
        call.remote_addr,
        true,
    )
}

fn build_ack(core: &Arc<SipCore>, call: &ActiveCall) -> Request {
    let mut ack = core.build_request(
        Method::Ack,
        call.remote_target.clone(),
        call.local.clone(),
        call.remote.clone(),
        &call.call_id,
        call.local_cseq,
        call.remote_addr,
        true,
    );
    // The ACK for a 2xx keeps the INVITE's sequence number.
    ack.headers.set("cseq", format!("{} ACK", call.local_cseq));
    ack
}

/// Which ALSA devices to use for this call.
fn resolve_audio_devices(
    shared: &Arc<Shared>,
    modem: &Arc<ModemHandle>,
    call: Option<&CallInfo>,
) -> Result<(String, String)> {
    let cfg = &shared.cfg.audio;

    let explicit_capture = cfg.capture_device.clone().or_else(|| cfg.device.clone());
    let explicit_playback = cfg.playback_device.clone().or_else(|| cfg.device.clone());
    if let (Some(c), Some(p)) = (explicit_capture, explicit_playback) {
        return Ok((c, p));
    }

    // Some drivers publish the audio device on the call object itself.
    if cfg.use_mm_audio_port {
        if let Some(port) = call.and_then(|c| c.audio_port.as_deref()) {
            if looks_like_alsa(port) {
                let dev = normalise_alsa(port);
                info!(port, "using the ALSA device reported by ModemManager");
                return Ok((dev.clone(), dev));
            }
            warn!(port, "ModemManager reports a non-ALSA audio port; falling back to the sound card");
        }
    }

    let card = modem.alsa.as_ref().ok_or_else(|| {
        anyhow!(
            "no ALSA card is associated with this modem; set audio.device (e.g. \"plughw:1,0\")"
        )
    })?;
    let dev = card.device_string(true);
    Ok((dev.clone(), dev))
}

fn looks_like_alsa(port: &str) -> bool {
    port.starts_with("hw:")
        || port.starts_with("plughw:")
        || port.starts_with("default")
        || port.starts_with("/dev/snd/")
}

fn normalise_alsa(port: &str) -> String {
    if let Some(rest) = port.strip_prefix("/dev/snd/") {
        // pcmC1D0c -> plughw:1,0
        if let Some(rest) = rest.strip_prefix("pcmC") {
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            let after = &rest[digits.len()..];
            let dev: String = after
                .strip_prefix('D')
                .unwrap_or("")
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if !digits.is_empty() {
                return format!("plughw:{},{}", digits, if dev.is_empty() { "0" } else { &dev });
            }
        }
    }
    port.to_string()
}

/// Keep digits and the few characters that are meaningful for dialling.
pub fn sanitize_number(raw: &str) -> String {
    let raw = raw.trim();
    let mut out = String::with_capacity(raw.len());
    for (i, c) in raw.chars().enumerate() {
        match c {
            '+' if i == 0 => out.push('+'),
            '0'..='9' | '*' | '#' => out.push(c),
            _ => {}
        }
    }
    out
}

/// application/dtmf-relay ("Signal=5\r\nDuration=250") and application/dtmf ("5").
fn parse_dtmf_info(content_type: &str, body: &str) -> Option<String> {
    let ct = content_type.to_ascii_lowercase();
    if ct.contains("dtmf-relay") {
        for line in body.lines() {
            if let Some((k, v)) = line.split_once('=') {
                if k.trim().eq_ignore_ascii_case("signal") {
                    return Some(v.trim().to_string());
                }
            }
        }
        return Some(String::new());
    }
    if ct.contains("dtmf") {
        return Some(body.trim().to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_sanitisation() {
        assert_eq!(sanitize_number("+82 10-1234-5678"), "+821012345678");
        assert_eq!(sanitize_number("tel:0212345678"), "0212345678");
        assert_eq!(sanitize_number("*123#"), "*123#");
    }

    #[test]
    fn dtmf_info_parsing() {
        assert_eq!(
            parse_dtmf_info("application/dtmf-relay", "Signal=5\r\nDuration=250\r\n").as_deref(),
            Some("5")
        );
        assert_eq!(parse_dtmf_info("application/dtmf", "7\n").as_deref(), Some("7"));
        assert_eq!(parse_dtmf_info("application/sdp", "v=0"), None);
    }

    #[test]
    fn alsa_device_normalisation() {
        assert_eq!(normalise_alsa("/dev/snd/pcmC1D0c"), "plughw:1,0");
        assert_eq!(normalise_alsa("hw:2,0"), "hw:2,0");
    }
}
