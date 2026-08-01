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

/// Messages crossing the SIP boundary carry `messagetype=sms` as a parameter
/// of the To header - `To: <sip:user@host>;messagetype=sms` - set on
/// everything the gateway sends out and required on everything it is asked to
/// send, so that a MESSAGE meant for something else is never put on the air.
const MESSAGE_TYPE_PARAM: &str = "messagetype";
const MESSAGE_TYPE_SMS: &str = "sms";

/// Events the gateway generates for itself (responses to its own requests,
/// timers, ...).
#[derive(Debug)]
enum Internal {
    SipProgress { call_id: String, code: u16 },
    SipAnswered { call_id: String, resp: Box<Response> },
    SipFailed { call_id: String, resp: Box<Response> },
    RingTimeout { call_id: String },
    /// The network started sending audio while the call was still ringing.
    EarlyMediaAudio { call_id: String, level: u32 },
    /// An RFC 2833 digit arrived in the RTP stream (SIP -> modem).
    Dtmf { call_id: String, digit: char },
    /// A DTMF tone was heard in the audio from the mobile side (modem -> SIP).
    DtmfFromModem { call_id: String, digit: char },
    /// Look at an incoming SMS again: it was still being assembled last time.
    SmsRetry { path: OwnedObjectPath, attempt: u32 },
}

/// How often, and how far apart, an SMS that is still arriving is re-read.
///
/// ModemManager announces a concatenated message when its *first* part lands,
/// with `State = RECEIVING`, and never signals again once the rest arrives -
/// so a message that is not complete on arrival has to be looked at again or
/// it is lost.  Ten seconds covers the segments of a long message; a message
/// still incomplete after that has lost parts on the air and never completes.
const SMS_ASSEMBLY_ATTEMPTS: u32 = 10;
const SMS_ASSEMBLY_INTERVAL: Duration = Duration::from_secs(1);

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
    /// The telephone-event payload type the running media session decodes.
    /// Fixed when the session starts, so a re-INVITE cannot move it.
    media_dtmf_pt: Option<u8>,

    answered: bool,
    /// A digest challenge on our own INVITE has already been answered once,
    /// so a second one is a refusal rather than an invitation to try again.
    auth_retried: bool,
    ringing_sent: bool,
    /// A 183 with SDP has been sent and the audio path is already up.
    early_media: bool,
    /// `Call.SendDtmf` was refused for this call - the codec it negotiated
    /// leaves the firmware no way to signal a digit - so the rest of its
    /// digits are played in-band.  Deliberately per call: the next one may
    /// negotiate a codec that works.
    dtmf_via_modem_failed: bool,
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
}

pub async fn run(
    shared: Arc<Shared>,
    core: Arc<SipCore>,
    mut sip_rx: mpsc::Receiver<SipEvent>,
    mut modem_rx: mpsc::Receiver<ModemEvent>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let (internal_tx, mut internal_rx) = mpsc::channel::<Internal>(64);
    let mut gw = Gateway { shared, core, modem: None, call: None, internal_tx };
    let mut watchdog = tokio::time::interval(Duration::from_secs(2));
    watchdog.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // Leaving a call up on the way out strands the mobile leg on the
            // network - connected, silent and still billed - so it is hung up
            // before the process goes away.
            _ = &mut shutdown => {
                if gw.call.is_some() {
                    gw.teardown_call("modem2sip is shutting down", 200).await;
                    // The BYE goes out from a task of its own; give it long
                    // enough to reach the socket before the process exits.
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                break;
            }
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
                self.on_sms_added(path, received, 0).await?;
            }
        }
        Ok(())
    }

    async fn on_mm_call_added(&mut self, path: OwnedObjectPath) -> Result<()> {
        let Some(modem) = self.modem.clone() else { return Ok(()) };

        // Our own outgoing call, already tracked.
        if self.call.as_ref().and_then(|c| c.mm_path.clone()).as_ref() == Some(&path) {
            return Ok(());
        }
        let Ok(info) = modem.call_info(&path).await else {
            // A call we just cleaned up after a failed dial; the signal for it
            // arrives once the object has already gone.
            debug!(path = path.as_str(), "call object vanished before it could be read");
            return Ok(());
        };

        if matches!(info.state, call_state::TERMINATED) {
            // A finished call ModemManager still lists.  Nothing owns it and
            // nothing will delete it later, and every one left behind is
            // another record the modem carries into the next call.
            debug!(path = path.as_str(), "deleting a call object that has already ended");
            modem.delete_call(&path).await;
            return Ok(());
        }
        if info.direction != call_state::DIR_INCOMING {
            // Outgoing, but not the call this process is running - so it is
            // left over from a previous run, still up on the network and
            // holding the modem.
            warn!(
                path = path.as_str(),
                number = %info.number,
                state = call_state::state_name(info.state),
                "an outgoing call from an earlier run is still up; ending it"
            );
            let _ = modem.hangup(&path).await;
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

        let advertised = self.rtp_advertised_ip(target_addr);
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
            media_dtmf_pt: None,
            answered: false,
            auth_retried: false,
            ringing_sent: false,
            early_media: false,
            dtmf_via_modem_failed: false,
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
                    if self.shared.cfg.sip.early_media {
                        // The network is already sending audio - its ringback
                        // tone, or an announcement telling the caller why this
                        // call is not going to connect.  Let them hear it.
                        if let Err(e) = self.start_early_media().await {
                            warn!(
                                error = %format!("{e:#}"),
                                "early media failed, falling back to 180 Ringing"
                            );
                            self.send_provisional(180).await;
                        }
                    } else {
                        self.send_provisional(180).await;
                    }
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
        let Some((role, cdr_id, peer)) =
            self.call.as_ref().map(|c| (c.role, c.cdr_id, c.peer_number.clone()))
        else {
            return Ok(());
        };

        match role {
            Role::FromSip => {
                // Answering the SIP side can still fail (the sound card may
                // be gone).  If it does the mobile leg is live and billed
                // while the caller has had no final response at all, so the
                // whole call has to come down rather than the error just
                // being logged by the event loop.
                // What did the caller actually hear while it was ringing?
                // Silence here means the network sent no ringback, and a
                // plain 180 would have served them better.
                if let Some((level, frames, upgraded)) =
                    self.call.as_ref().filter(|c| c.ringing_sent).and_then(|c| {
                        c.media.as_ref().map(|m| {
                            let (l, f) = m.uplink_level();
                            (l, f, c.early_media)
                        })
                    })
                {
                    info!(
                        level,
                        ms = frames * 20,
                        early_media = upgraded,
                        "ringing ended; level is what the caller was sent while waiting"
                    );
                }
                if let Err(e) = self.answer_sip_caller().await {
                    error!(error = %format!("{e:#}"), "could not answer the SIP caller");
                    self.teardown_call("answering the SIP caller failed", 500).await;
                    return Ok(());
                }
                info!(peer = %peer, "call answered by the network");
            }
            Role::ToSip => {
                // Media is already running (started when SIP answered).
                info!(peer = %peer, "modem call is active");
            }
        }

        if let Some(c) = self.call.as_mut() {
            c.answered = true;
        }
        if let Some(id) = cdr_id {
            let _ = self.shared.db.call_answered(id).await;
        }
        Ok(())
    }

    /// Send the 200 OK that connects a SIP -> modem call, media first.
    async fn answer_sip_caller(&mut self) -> Result<()> {
        self.respond_with_media(200).await
    }

    /// Open the audio path early and tell the caller to play what arrives.
    ///
    /// The mobile network starts sending audio while it is alerting - its own
    /// ringback, or an announcement explaining why the call will not connect -
    /// and a bare `180 Ringing` would throw that away and have the caller's
    /// phone generate a local tone instead.
    /// Alerting has started.  Open the audio path and answer `180`, then wait
    /// to see whether the network actually sends anything.
    ///
    /// Announcing early media the moment the network says "alerting" is a bet
    /// that it has something to play, and it does not always: a call can ring
    /// for ten seconds at a level of 22, which is the noise floor.  A caller
    /// told to listen to that hears silence, where `180` would have had their
    /// own phone ring.  So the `183` waits for audio to actually turn up.
    async fn start_early_media(&mut self) -> Result<()> {
        self.open_media_for_early_media().await?;
        self.send_provisional(180).await;

        let Some(call) = self.call.as_mut() else { return Ok(()) };
        let Some(media) = call.media.as_ref() else { return Ok(()) };
        media.reset_uplink_level();

        let rings = media.rings();
        let tx = self.internal_tx.clone();
        let call_id = call.call_id.clone();
        call.tasks.push(tokio::spawn(async move {
            watch_for_early_media(rings, call_id, tx).await;
        }));
        Ok(())
    }

    /// Start the media session without answering anything yet, so the audio
    /// the network sends while alerting is already being captured.
    async fn open_media_for_early_media(&mut self) -> Result<()> {
        if self.call.as_ref().and_then(|c| c.media.as_ref()).is_some() {
            return Ok(());
        }
        let Some((remote_sdp, codec)) =
            self.call.as_ref().map(|c| (c.remote_sdp.clone(), c.codec))
        else {
            return Ok(());
        };
        let remote_sdp = remote_sdp.ok_or_else(|| anyhow!("no remote SDP"))?;
        let remote_media = SocketAddr::new(remote_sdp.address, remote_sdp.port);
        self.start_media(remote_media, codec).await
    }

    /// The network started sending audio while the call was ringing: tell the
    /// caller to listen to it.
    async fn upgrade_to_early_media(&mut self, level: u32) -> Result<()> {
        if self.call.as_ref().map(|c| c.early_media || c.answered).unwrap_or(true) {
            return Ok(());
        }
        self.respond_with_media(183).await?;
        if let Some(call) = self.call.as_mut() {
            call.early_media = true;
        }
        info!(level, "early media: the caller now hears the network");
        Ok(())
    }

    /// Answer the pending INVITE with `code` and an SDP answer, opening the
    /// media session first if it is not already running.  Used for both the
    /// early-media `183` and the final `200`, which must describe the same
    /// transport.
    async fn respond_with_media(&mut self, code: u16) -> Result<()> {
        let Some((remote_sdp, codec)) =
            self.call.as_ref().map(|c| (c.remote_sdp.clone(), c.codec))
        else {
            return Ok(());
        };
        let remote_sdp = remote_sdp.ok_or_else(|| anyhow!("no remote SDP"))?;

        if self.call.as_ref().and_then(|c| c.media.as_ref()).is_none() {
            let remote_media = SocketAddr::new(remote_sdp.address, remote_sdp.port);
            self.start_media(remote_media, codec).await?;
        }

        let dtmf_pt = self.shared.cfg.rtp.dtmf_payload_type;
        let ptime = self.shared.cfg.audio.period_ms;
        let core = self.core.clone();
        let advertised = self.rtp_advertised_ip(
            self.call.as_ref().map(|c| c.remote_addr).ok_or_else(|| anyhow!("call vanished"))?,
        );
        let call = self.call.as_ref().ok_or_else(|| anyhow!("call vanished"))?;
        let answer = remote_sdp.answer(advertised, call.local_rtp_port, codec, Some(dtmf_pt), ptime);
        let src = call.invite_src.unwrap_or(call.remote_addr);
        let mut resp = core.make_response(&call.invite, code, None);
        resp.headers.set("content-type", "application/sdp");
        resp.headers.set("to", call.local.to_string());
        resp.headers.set("allow", "INVITE, ACK, CANCEL, BYE, OPTIONS, INFO, MESSAGE");
        resp.body = answer.to_string().into_bytes();
        core.respond(&call.invite, src, resp).await
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
        if let Some(call) = self.call.as_ref() {
            // `call_id()` is None on a malformed request; comparing straight
            // against it would make that match an idle gateway's `None`.
            if req.headers.call_id() == Some(call.call_id.as_str()) {
                // Before the call is answered this is a UDP retransmission of
                // the original INVITE, not a re-INVITE: repeat the last
                // provisional instead of answering a call the network has not
                // connected yet.
                if !call.answered {
                    let (code, early) = (
                        if call.early_media { 183 } else { 180 },
                        call.early_media,
                    );
                    debug!(code, "INVITE retransmitted during setup, repeating the provisional");
                    if early {
                        return self.respond_with_media(183).await;
                    }
                    self.send_provisional(code).await;
                    return Ok(());
                }
                return self.on_sip_reinvite(req, src).await;
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
        if !remote_sdp.has_media() {
            warn!(port = remote_sdp.port, address = %remote_sdp.address, "INVITE offers no media");
            return self.reply(&req, src, 488).await;
        }

        // Everything that can be rejected has been; from the 100 Trying on,
        // the caller stops retransmitting and waits for us, so every path out
        // of here owes it a final response.
        let (Some(to), Some(from), Some(call_id)) =
            (req.headers.to(), req.headers.from(), req.headers.call_id().map(str::to_string))
        else {
            warn!("INVITE without To/From/Call-ID");
            return self.reply(&req, src, 400).await;
        };
        let to = to.with_tag(&crate::sip::auth::random_hex(6));
        let remote_target = req
            .headers
            .contact()
            .map(|c| c.uri)
            .unwrap_or_else(|| from.uri.clone());

        self.reply(&req, src, 100).await?;

        let rtp_ip = self.rtp_bind_ip(src);
        let (sock, port) = match MediaSession::bind_port(
            rtp_ip,
            self.shared.cfg.rtp.port_min,
            self.shared.cfg.rtp.port_max,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                error!(error = %format!("{e:#}"), "no RTP port available");
                return self.reply(&req, src, 500).await;
            }
        };

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
            media_dtmf_pt: None,
            answered: false,
            auth_retried: false,
            ringing_sent: false,
            early_media: false,
            dtmf_via_modem_failed: false,
            cdr_id,
            tasks: Vec::new(),
        });
        Ok(())
    }

    /// A second INVITE carrying the Call-ID of the call in progress.
    ///
    /// Only one inside the established dialog with a higher CSeq is a real
    /// re-INVITE.  A repeat of the initial INVITE used to be answered with a
    /// 200, which told the caller the call was up while the modem was still
    /// dialling - and a second, different 200 followed once the network
    /// actually connected.
    async fn on_sip_reinvite(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        let Some(call) = self.call.as_ref() else { return Ok(()) };
        // Until the first INVITE has its final response there is no dialog to
        // re-negotiate.  On a call we answered the peer also owns the
        // sequence space of `call.invite`, so anything not numbered above it
        // is that same INVITE arriving again.  On a call we placed, that
        // INVITE is ours and the peer's re-INVITE opens a sequence space of
        // its own, with nothing to compare it against.
        let pending = !call.answered
            || (call.role == Role::FromSip
                && req.headers.cseq().map(|(n, _)| n)
                    <= call.invite.headers.cseq().map(|(n, _)| n));
        if pending {
            // RFC 3261 §14.2: an INVITE is already outstanding on this dialog.
            debug!("491 for an INVITE while the first one is still in progress");
            return self.reply(&req, src, 491).await;
        }
        if !self.in_dialog(&req) {
            debug!("481 for an INVITE whose dialog tags do not match the call");
            return self.reply(&req, src, 481).await;
        }

        let offer = Sdp::parse(&req.body_str()).filter(|s| s.has_media());
        // Switching codec would mean tearing the audio down and rebuilding
        // it; refusing is honest, and no real peer re-offers a narrower list
        // mid-call.
        if let Some(o) = offer.as_ref() {
            if !o.payload_types.contains(&call.codec.payload_type()) {
                warn!(offered = ?o.payload_types, current = ?call.codec,
                      "re-INVITE drops the codec in use");
                return self.reply(&req, src, 488).await;
            }
        }

        // Follow the peer if it moved its RTP endpoint (hold, resume, or a
        // transfer that re-homed the media).
        if let (Some(o), Some(media)) = (offer.as_ref(), call.media.as_ref()) {
            let remote = SocketAddr::new(o.address, o.port);
            info!(%remote, "re-INVITE moved the remote media endpoint");
            media.set_remote(remote);
        }

        let base = offer.clone().or_else(|| call.remote_sdp.clone());
        let mut resp = self.core.make_response(&req, 200, None);
        resp.headers.set("to", call.local.to_string());
        if let Some(sdp) = base.as_ref() {
            // The running media session decodes the telephone-event payload
            // type it was started with.  If the new offer moved it we cannot
            // receive those digits, so the answer stays quiet about it rather
            // than promising a payload type nothing is listening for.
            let dtmf_pt = call.media_dtmf_pt.filter(|pt| sdp.telephone_event == Some(*pt));
            let advertised = self.rtp_advertised_ip(src);
            let answer = sdp.answer(
                advertised,
                call.local_rtp_port,
                call.codec,
                dtmf_pt,
                self.shared.cfg.audio.period_ms,
            );
            resp.headers.set("content-type", "application/sdp");
            resp.body = answer.to_string().into_bytes();
        }
        if let (Some(call), Some(sdp)) = (self.call.as_mut(), offer) {
            // The stream was just renegotiated, so whatever silence built up
            // while the peer was on hold - or moving its media - says nothing
            // about whether it is there now.  Start the clock again, or the
            // next watchdog tick ends a call that has only just resumed.
            if let Some(media) = call.media.as_ref() {
                media.reset_silence();
            }
            call.remote_sdp = Some(sdp);
        }
        self.core.respond(&req, src, resp).await
    }

    async fn on_sip_bye(&mut self, req: Request, src: SocketAddr) -> Result<()> {
        let matches = self.in_dialog(&req);
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

        // A CANCEL has no To tag - it matches the INVITE *transaction*, so
        // the Call-ID and the INVITE's sequence number are what identify it.
        //
        // RFC 3261 §9.2: it only has an effect while that transaction is
        // still pending.  Acting on a late one (UDP reordering, or anyone who
        // saw the Call-ID on the wire - a CANCEL is never challenged) used to
        // hang up a connected call and drop it without a BYE, leaving the
        // peer on a dialog nothing would ever end.  Nor can it touch a call
        // we placed: that INVITE is ours, and its transaction is not the
        // peer's to cancel.
        let matches = self
            .call
            .as_ref()
            .map(|c| {
                c.role == Role::FromSip
                    && !c.answered
                    && Some(c.call_id.as_str()) == req.headers.call_id()
                    && req.headers.cseq().map(|(n, _)| n)
                        == c.invite.headers.cseq().map(|(n, _)| n)
            })
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
        let in_dialog = self.in_dialog(&req);
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
        let to_header = req.headers.to();
        let to = to_header
            .as_ref()
            .map(|t| t.uri.clone())
            .unwrap_or_else(|| req.uri.clone());

        // Only messages explicitly marked as SMS are put on the air.  The
        // marker belongs to the To header; a sender that puts it inside the
        // URI instead is still understood.
        let declared = to_header
            .as_ref()
            .and_then(|t| t.param(MESSAGE_TYPE_PARAM))
            .or_else(|| to.param(MESSAGE_TYPE_PARAM))
            .map(|v| v.to_string());
        match declared.as_deref() {
            Some(v) if v.eq_ignore_ascii_case(MESSAGE_TYPE_SMS) => {}
            other => {
                info!(
                    messagetype = other.unwrap_or("<absent>"),
                    "rejecting MESSAGE: not marked as {MESSAGE_TYPE_PARAM}={MESSAGE_TYPE_SMS}"
                );
                return self.reply(&req, src, 415).await;
            }
        }

        let Some(modem) = self.modem.clone() else {
            return self.reply(&req, src, 503).await;
        };
        let content_type = req.headers.content_type().unwrap_or("text/plain").to_string();
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
                    let stored = shared
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
                            external_id: Some(outgoing_sms_id(&path)),
                            raw: None,
                        })
                        .await;
                    if let Err(e) = stored {
                        warn!(error = %format!("{e:#}"), "the SMS went out but could not be recorded");
                    }
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
            Internal::SipFailed { call_id, resp } => {
                if !self.call_id_matches(&call_id) {
                    return Ok(());
                }
                // A challenge is not a rejection: a PBX that authenticates its
                // endpoints - Asterisk does by default, registration or no
                // registration - answers the first INVITE with a 401, and
                // giving up on it meant no call from the mobile network could
                // ever be delivered in that deployment.
                if matches!(resp.code, 401 | 407) {
                    self.ack_failure(&resp).await;
                    match self.retry_invite_with_auth(&resp).await {
                        Ok(true) => return Ok(()),
                        Ok(false) => {}
                        Err(e) => {
                            warn!(error = %format!("{e:#}"), "could not answer the INVITE challenge")
                        }
                    }
                } else {
                    self.ack_failure(&resp).await;
                }
                warn!(code = resp.code, "the SIP side rejected the call");
                if let (Some(modem), Some(path)) =
                    (self.modem.clone(), self.call.as_ref().and_then(|c| c.mm_path.clone()))
                {
                    let _ = modem.hangup(&path).await;
                }
                self.finish_call("sip rejected").await;
                Ok(())
            }
            Internal::EarlyMediaAudio { call_id, level } => {
                if self.call_id_matches(&call_id) {
                    self.upgrade_to_early_media(level).await?;
                }
                Ok(())
            }
            Internal::Dtmf { call_id, digit } => {
                if self.call_id_matches(&call_id) {
                    self.deliver_dtmf(&digit.to_string()).await;
                }
                Ok(())
            }
            Internal::SmsRetry { path, attempt } => self.on_sms_added(path, true, attempt).await,
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
    /// Whether the modem can signal a digit is decided per call, not per
    /// modem: `Call.SendDtmf` fails when the codec negotiated for *this* call
    /// leaves the firmware no way to send one, and the next call may
    /// negotiate something that works.  So every call asks ModemManager
    /// first, and only that call falls back to playing the tones into the
    /// uplink audio itself.
    async fn deliver_dtmf(&mut self, digits: &str) -> bool {
        use crate::config::DtmfMethod;
        let method = self.shared.cfg.rtp.dtmf_method;
        if method == DtmfMethod::None || digits.is_empty() {
            return true;
        }

        let failed_here =
            self.call.as_ref().map(|c| c.dtmf_via_modem_failed).unwrap_or(false);
        let try_modem = matches!(method, DtmfMethod::Modem | DtmfMethod::Auto)
            && !(method == DtmfMethod::Auto && failed_here);

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
                            "SendDtmf failed; playing tones in-band for the rest of this call"
                        );
                        if let Some(call) = self.call.as_mut() {
                            call.dtmf_via_modem_failed = true;
                        }
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

    /// Does `req` belong to the dialog of the call in progress?
    fn in_dialog(&self, req: &Request) -> bool {
        self.call.as_ref().map(|c| in_dialog_of(c, req)).unwrap_or(false)
    }

    /// RFC 3261 §17.1.1.3: a non-2xx final response to an INVITE has to be
    /// ACKed by the transaction, reusing the INVITE's branch and taking the
    /// To header (with the peer's tag) from the response.  Without it the
    /// peer retransmits the rejection for 32 s and logs a timeout.
    async fn ack_failure(&self, resp: &Response) {
        let Some(call) = self.call.as_ref() else { return };
        if call.role != Role::ToSip {
            return;
        }
        let mut ack = call.invite.clone();
        ack.method = Method::Ack;
        ack.body.clear();
        ack.headers.remove("content-type");
        ack.headers.set("cseq", format!("{} ACK", call.local_cseq));
        if let Some(to) = resp.headers.get("to") {
            ack.headers.set("to", to.to_string());
        }
        let _ = self.core.send_raw(&ack, call.remote_addr).await;
    }

    /// Answer a digest challenge on the INVITE we sent and send it again.
    ///
    /// Returns false when there is nothing to retry - no credentials, no
    /// parsable challenge, or the challenge has already been answered once -
    /// in which case the caller treats the response as the rejection it is.
    async fn retry_invite_with_auth(&mut self, resp: &Response) -> Result<bool> {
        let Some(call) = self.call.as_ref() else { return Ok(false) };
        if call.role != Role::ToSip || call.answered || call.auth_retried {
            return Ok(false);
        }
        let Some(up) = self.shared.cfg.sip.register.as_ref() else {
            debug!("challenged on an INVITE but no credentials are configured");
            return Ok(false);
        };

        let (hdr, out_hdr) = if resp.code == 401 {
            ("www-authenticate", "authorization")
        } else {
            ("proxy-authenticate", "proxy-authorization")
        };
        let Some(challenge) = resp
            .headers
            .get_all(hdr)
            .into_iter()
            .filter_map(crate::sip::auth::Challenge::parse)
            .find(crate::sip::auth::Challenge::is_supported)
        else {
            warn!(code = resp.code, "no digest challenge in {hdr} that we can answer (MD5 only)");
            return Ok(false);
        };

        // A fresh branch and sequence number: this is a new transaction, not a
        // retransmission of the one that was just refused.
        let cseq = self.core.next_cseq();
        let mut invite = call.invite.clone();
        let uri = invite.uri.to_string();
        let creds = crate::sip::auth::answer(
            &challenge,
            &up.username,
            &up.password,
            Method::Invite.as_str(),
            &uri,
            &invite.body,
            1,
        );
        invite.headers.set("cseq", format!("{cseq} INVITE"));
        invite.headers.set(out_hdr, creds.to_header());
        crate::sip::core::refresh_via_branch(&mut invite.headers);

        let dest = call.remote_addr;
        let txn = self.core.send_request(invite.clone(), dest).await?;

        let (tx, call_id, ring_timeout) = (
            self.internal_tx.clone(),
            call.call_id.clone(),
            Duration::from_secs(self.shared.cfg.sip.ring_timeout_secs),
        );
        let call = self.call.as_mut().expect("checked above");
        call.auth_retried = true;
        call.local_cseq = cseq;
        call.invite = invite;
        call.tasks.push(tokio::spawn(invite_response_task(txn, call_id, tx, ring_timeout)));
        info!(code = resp.code, "answering the INVITE challenge and dialling again");
        Ok(true)
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

        // The dialog is established the moment that ACK goes out, so every
        // failure below has to end it with a BYE.  Leaving `answered` false
        // until the modem picks up would make teardown send a CANCEL for a
        // transaction that is already complete, and the peer would sit on a
        // call the gateway has forgotten.
        if let Some(call) = self.call.as_mut() {
            call.answered = true;
        }

        let Some(sdp) = Sdp::parse(&resp.body_str()).filter(|s| s.has_media()) else {
            warn!("2xx without a usable SDP answer; dropping the call");
            self.teardown_call("no media in the answer", 200).await;
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
        if let Some(id) = self.call.as_ref().and_then(|c| c.cdr_id) {
            let _ = self.shared.db.call_answered(id).await;
        }
        info!("call connected (modem -> SIP)");
        Ok(())
    }

    // ------------------------------------------------------------------ media

    fn rtp_bind_ip(&self, peer: SocketAddr) -> std::net::IpAddr {
        // Validated at start-up, so a value here always parses.
        if let Some(cfg_ip) = parse_ip(self.shared.cfg.rtp.bind.as_deref()) {
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

    fn rtp_advertised_ip(&self, peer: SocketAddr) -> std::net::IpAddr {
        media_address(&self.shared.cfg.rtp, self.core.transport.advertised_ip(peer))
    }

    /// The ALSA threads give up on a card that keeps failing (a modem that is
    /// being unplugged, for instance).  A call with no audio is worse than no
    /// call, so end it.  The same tick notices a SIP peer that stopped
    /// sending, which is the only sign we get that it is gone.
    async fn check_media(&mut self) {
        let Some(media) = self.call.as_ref().and_then(|c| c.media.as_ref()) else { return };
        if media.audio_failed() {
            self.teardown_call("the modem audio stream died", 200).await;
            return;
        }
        // A peer that offered a=recvonly or a=inactive has told us it will
        // send nothing, so its silence is not evidence that it has gone away.
        // Reading it that way would hang up a perfectly good call whenever a
        // PBX holds it without music.
        let peer_sends = self
            .call
            .as_ref()
            .and_then(|c| c.remote_sdp.as_ref())
            .map(|s| s.sendrecv.peer_sends())
            .unwrap_or(true);
        let timeout = Duration::from_secs(self.shared.cfg.rtp.timeout_secs);
        if peer_sends && !timeout.is_zero() && media.silence() > timeout {
            // Nothing else would ever end this call: a peer that vanished
            // without a BYE cannot be asked, and the mobile leg stays up -
            // and billed - until the process is restarted.
            warn!(
                seconds = timeout.as_secs(),
                "no RTP from the SIP peer; assuming it is gone and ending the call"
            );
            self.teardown_call("the SIP peer stopped sending RTP", 200).await;
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
            if let Err(e) = crate::vendor::enable_usb_audio(&modem, false).await {
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
            jitter_ms: self.shared.cfg.rtp.jitter_ms,
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
        call.media_dtmf_pt = dtmf_payload_type;

        let (dtmf_tx, mut dtmf_rx) = mpsc::channel::<char>(16);
        let (inband_tx, mut inband_rx) = mpsc::channel::<char>(16);
        let media = MediaSession::start(
            sock,
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
            invite: Request,
            invite_dest: SocketAddr,
            local_header: String,
            remote_addr: SocketAddr,
            mm_path: Option<OwnedObjectPath>,
            bye: Option<Request>,
        }

        let Some(snap) = self.call.as_ref().map(|c| Snapshot {
            role: c.role,
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
        if let Some((level, frames, upgraded)) = self
            .call
            .as_ref()
            .filter(|c| c.ringing_sent && !c.answered)
            .and_then(|c| {
                c.media.as_ref().map(|m| {
                    let (l, f) = m.uplink_level();
                    (l, f, c.early_media)
                })
            })
        {
            info!(
                level,
                ms = frames * 20,
                early_media = upgraded,
                "ringing ended without an answer"
            );
        }

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
                    // The caller never got a 2xx, so it needs a failure code:
                    // a "normal end" here just means we could not complete it.
                    let code = if code < 400 { 480 } else { code };
                    let mut resp = self.core.make_response(&snap.invite, code, None);
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

    async fn on_sms_added(
        &mut self,
        path: OwnedObjectPath,
        received: bool,
        attempt: u32,
    ) -> Result<()> {
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
        if is_incoming && info.state == sms_state::STATE_RECEIVING {
            // A message that arrives in more than one part is announced when
            // the first part lands and is never announced again, so this is
            // the only chance to notice the rest.  Anything longer than 160
            // GSM-7 or 70 UCS-2 characters comes this way - most Korean text
            // messages - and used to be dropped here and never stored.
            if attempt >= SMS_ASSEMBLY_ATTEMPTS {
                warn!(
                    path = path.as_str(),
                    "an incoming SMS never finished arriving; parts must have been lost"
                );
                return Ok(());
            }
            let tx = self.internal_tx.clone();
            let path = path.clone();
            tokio::spawn(async move {
                tokio::time::sleep(SMS_ASSEMBLY_INTERVAL).await;
                let _ = tx.send(Internal::SmsRetry { path, attempt: attempt + 1 }).await;
            });
            return Ok(());
        }
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
                external_id: Some(incoming_sms_id(&path, &info, &peer)),
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
    // The peer is a number the network reported - for MMS it is a free-form
    // address decoded from a PDU an SMS sender wrote, so it can hold anything
    // at all, CRLF included.  It goes into a header, so it is reduced to the
    // dialling characters here rather than being trusted to be a number.
    let peer = sanitize_number(peer);
    let from_user = if peer.is_empty() { "unknown".to_string() } else { peer.to_string() };
    let from = NameAddr {
        display: Some(from_user.clone()),
        uri: Uri::new(Some(&from_user), &domain, None),
        params: Vec::new(),
    }
    .with_tag(&crate::sip::auth::random_hex(6));
    // Mark what this is: `To: <sip:user@host>;messagetype=sms`.
    let mut to = NameAddr::new(target_uri.bare());
    to.set_param(MESSAGE_TYPE_PARAM, Some(MESSAGE_TYPE_SMS));
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

/// Watch what the modem is sending towards the caller while a call is
/// ringing, and report the moment it stops being silence.
///
/// Measured levels: the noise floor of an idle path is around 20, a ringback
/// tone or an announcement is a few thousand.  Two consecutive windows above
/// the threshold keep a click from counting.
async fn watch_for_early_media(
    rings: Arc<crate::audio::AudioRings>,
    call_id: String,
    tx: mpsc::Sender<Internal>,
) {
    const WINDOW: Duration = Duration::from_millis(120);
    const THRESHOLD: u32 = 150;
    const NEEDED: u8 = 2;

    let (mut last_energy, mut last_frames) = rings.uplink_raw();
    let mut streak = 0u8;
    let mut ticker = tokio::time::interval(WINDOW);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let (energy, frames) = rings.uplink_raw();
        let (d_energy, d_frames) =
            (energy.saturating_sub(last_energy), frames.saturating_sub(last_frames));
        last_energy = energy;
        last_frames = frames;
        if d_frames == 0 {
            continue;
        }
        let level = d_energy / d_frames;
        if level >= THRESHOLD {
            streak += 1;
            if streak >= NEEDED {
                let _ = tx.send(Internal::EarlyMediaAudio { call_id, level }).await;
                return;
            }
        } else {
            streak = 0;
        }
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
                let _ = tx.send(Internal::SipFailed { call_id, resp: Box::new(resp) }).await;
                return;
            }
            None => {
                let _ = tx.send(Internal::RingTimeout { call_id }).await;
                return;
            }
        }
    }
}

/// The address to put in the SDP for our own media.
///
/// Not necessarily the one SIP is reachable on: pinning RTP to an interface
/// is pointless if the peer is still told to send it to wherever the
/// signalling happened to arrive.
fn media_address(cfg: &crate::config::Rtp, sip_advertised: std::net::IpAddr) -> std::net::IpAddr {
    if let Some(ip) = parse_ip(cfg.public_ip.as_deref()) {
        return ip;
    }
    match parse_ip(cfg.bind.as_deref()) {
        // A wildcard bind says nothing about where to reach us.
        Some(ip) if !ip.is_unspecified() => ip,
        _ => sip_advertised,
    }
}

/// Addresses in the config are validated at start-up, so anything that
/// reaches here parses.
fn parse_ip(value: Option<&str>) -> Option<std::net::IpAddr> {
    value.and_then(|s| s.parse().ok())
}

/// Does `req` belong to `call`'s dialog?
///
/// The Call-ID alone is not enough: it travels in the clear on every packet
/// of the call, so matching on it would let anyone who can reach the SIP port
/// hang the call up with a BYE or push digits into it with an INFO.  A dialog
/// is identified by the Call-ID *and* both tags.
fn in_dialog_of(call: &ActiveCall, req: &Request) -> bool {
    if req.headers.call_id() != Some(call.call_id.as_str()) {
        return false;
    }
    let to_tag = req.headers.to().and_then(|t| t.tag().map(str::to_string));
    let from_tag = req.headers.from().and_then(|f| f.tag().map(str::to_string));
    if call.local.tag().is_some() && to_tag.as_deref() != call.local.tag() {
        return false;
    }
    // The peer's tag is only known once it has answered or offered one.
    match call.remote.tag() {
        Some(remote) => from_tag.as_deref() == Some(remote),
        None => true,
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

/// Identity for a message we sent ourselves.
///
/// ModemManager numbers its SMS objects from zero on every start, so the bare
/// object path collides with one from a previous run - and the database, which
/// outlives it, threw the new message away as a duplicate of the old one.  A
/// message we originated needs no de-duplication in the first place; the path
/// is kept only because it is what links the row to the modem's object.
fn outgoing_sms_id(path: &OwnedObjectPath) -> String {
    format!("{}|{}", path.as_str(), crate::db::now_iso())
}

/// Identity of a received message, for de-duplication.
///
/// A message the modem keeps in its own storage is announced again every time
/// the modem comes back, and ModemManager renumbers its objects when it does,
/// so `/SMS/4` becomes `/SMS/11` and a key built on the object path lets the
/// same message in twice.  What does not change is what the network said:
/// when it arrived, who sent it, and what it contained.
///
/// Without a network timestamp there is nothing stable to key on, so the
/// object path is used after all - storing a message twice is better than
/// dropping a genuinely new one.
fn incoming_sms_id(path: &OwnedObjectPath, info: &SmsInfo, peer: &str) -> String {
    let mut content = info.text.clone().into_bytes();
    content.extend_from_slice(&info.data);
    let digest = crate::sip::auth::md5_hex(&content);
    match info.timestamp.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        Some(ts) => format!("{ts}|{peer}|{digest}"),
        None => format!("{}|{peer}|{digest}", path.as_str()),
    }
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

    /// The MMS sender is free-form text decoded from a PDU whoever sent the
    /// message wrote, and it goes into the From header of the MESSAGE the
    /// gateway then sends to its own SIP peer.
    #[test]
    fn a_sender_cannot_smuggle_headers_through_the_number() {
        assert_eq!(
            sanitize_number("0100\r\nContact: <sip:evil@h>\r\nSubject: pwned"),
            "0100"
        );
        assert_eq!(sanitize_number("someone@example.com"), "");
        assert!(sanitize_number("\r\n\r\n").is_empty());
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

    fn request(method: Method, headers: &[(&str, &str)]) -> Request {
        let mut req = Request::new(method, Uri::parse("sip:gw").unwrap());
        for (k, v) in headers {
            req.headers.push(k, v.to_string());
        }
        req
    }

    /// A call the gateway answered as the UAS: our tag is on the To header of
    /// everything the peer sends back.
    fn answered_call() -> ActiveCall {
        ActiveCall {
            role: Role::FromSip,
            mm_path: None,
            peer_number: "+8210".into(),
            call_id: "call-1".into(),
            local: NameAddr::parse("<sip:gw@h>;tag=ours").unwrap(),
            remote: NameAddr::parse("<sip:alice@h>;tag=theirs").unwrap(),
            remote_target: Uri::parse("sip:alice@h").unwrap(),
            remote_addr: "10.0.0.5:5060".parse().unwrap(),
            local_cseq: 1,
            invite: request(Method::Invite, &[("call-id", "call-1"), ("cseq", "1 INVITE")]),
            invite_src: None,
            media: None,
            rtp_socket: None,
            local_rtp_port: 16384,
            codec: Codec::Pcmu,
            remote_sdp: None,
            media_dtmf_pt: None,
            answered: true,
            auth_retried: false,
            ringing_sent: true,
            early_media: false,
            dtmf_via_modem_failed: false,
            cdr_id: None,
            tasks: Vec::new(),
        }
    }

    fn bye(to: &str, from: &str, call_id: &str) -> Request {
        request(Method::Bye, &[("call-id", call_id), ("to", to), ("from", from)])
    }

    /// Matching in-dialog requests on the Call-ID alone let anyone who could
    /// see one packet of the call tear it down or push digits into it.
    #[test]
    fn in_dialog_requires_both_tags() {
        let call = answered_call();
        assert!(in_dialog_of(&call, &bye("<sip:gw@h>;tag=ours", "<sip:a@h>;tag=theirs", "call-1")));

        // Right Call-ID, guessed or absent tags.
        for wrong in [
            bye("<sip:gw@h>;tag=guess", "<sip:a@h>;tag=theirs", "call-1"),
            bye("<sip:gw@h>", "<sip:a@h>;tag=theirs", "call-1"),
            bye("<sip:gw@h>;tag=ours", "<sip:eve@h>;tag=other", "call-1"),
            bye("<sip:gw@h>;tag=ours", "<sip:a@h>", "call-1"),
            bye("<sip:gw@h>;tag=ours", "<sip:a@h>;tag=theirs", "another-call"),
        ] {
            assert!(!in_dialog_of(&call, &wrong), "accepted {:?}", wrong.headers);
        }
    }

    /// Before the peer has offered a tag there is nothing to compare, so only
    /// ours is checked - otherwise a legitimate early BYE would be refused.
    #[test]
    fn in_dialog_tolerates_an_unknown_remote_tag() {
        let mut call = answered_call();
        call.remote = NameAddr::parse("<sip:alice@h>").unwrap();
        assert!(in_dialog_of(&call, &bye("<sip:gw@h>;tag=ours", "<sip:a@h>;tag=x", "call-1")));
        assert!(!in_dialog_of(&call, &bye("<sip:gw@h>;tag=no", "<sip:a@h>;tag=x", "call-1")));
    }
    /// Pinning the media to an interface is pointless if the peer is still
    /// told to send it wherever the signalling arrived.
    #[test]
    fn the_sdp_follows_the_rtp_bind_address() {
        let sip: std::net::IpAddr = "10.0.0.1".parse().unwrap();
        let rtp = |bind: Option<&str>, public: Option<&str>| {
            let cfg = crate::config::Rtp {
                bind: bind.map(str::to_string),
                public_ip: public.map(str::to_string),
                ..Default::default()
            };
            media_address(&cfg, sip).to_string()
        };

        // Nothing configured: whatever SIP advertises.
        assert_eq!(rtp(None, None), "10.0.0.1");
        // A specific bind is where the peer has to send.
        assert_eq!(rtp(Some("192.168.9.5"), None), "192.168.9.5");
        // A wildcard says nothing about where to reach us.
        assert_eq!(rtp(Some("0.0.0.0"), None), "10.0.0.1");
        assert_eq!(rtp(Some("::"), None), "10.0.0.1");
        // An explicit public address wins over both.
        assert_eq!(rtp(Some("192.168.9.5"), Some("203.0.113.7")), "203.0.113.7");
        assert_eq!(rtp(None, Some("203.0.113.7")), "203.0.113.7");
        assert_eq!(rtp(Some("0.0.0.0"), Some("203.0.113.7")), "203.0.113.7");
    }
}
