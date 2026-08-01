//! RTP <-> ALSA plumbing for one call.
//!
//! Narrow band only: G.711 at 8 kHz, 20 ms packets, plus RFC 2833 digit
//! reception.  The ALSA ring buffers double as the jitter buffer - the
//! playback thread drains them at a constant rate, which is exactly the
//! behaviour a jitter buffer needs.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use rand::Rng;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use crate::audio::codec::{self, Law};
use crate::audio::{AudioRings, AudioStream, RTP_RATE};
use crate::sip::sdp::Codec;

pub struct MediaSession {
    pub remote: Arc<Mutex<SocketAddr>>,
    /// Where the signalling says the peer is, and whether the receiver has
    /// latched onto the address the packets actually come from.  Separate
    /// from `remote`, which symmetric RTP overwrites with the observed
    /// source: the receiver has to keep checking against what was signalled.
    signalled: Arc<Mutex<Signalled>>,
    clock: Arc<Clock>,
    rings: Arc<AudioRings>,
    stop: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
    audio: Option<AudioStream>,
}

struct Signalled {
    addr: SocketAddr,
    latched: bool,
}

/// Milliseconds since the session started, for the inactivity check.  An
/// `Instant` cannot live in an atomic, and the receiver has to be able to
/// stamp this without taking a lock on every packet.
///
/// The counter is 32 bits because the 32-bit OpenWrt targets have no 64-bit
/// atomics.  It saturates after 49 days, which is far past any call.
struct Clock {
    started: std::time::Instant,
    last_rtp_ms: AtomicU32,
}

impl Clock {
    fn new() -> Self {
        Self { started: std::time::Instant::now(), last_rtp_ms: AtomicU32::new(0) }
    }
    fn elapsed_ms(&self) -> u32 {
        self.started.elapsed().as_millis().min(u32::MAX as u128) as u32
    }
    fn stamp(&self) {
        self.last_rtp_ms.store(self.elapsed_ms(), Ordering::Relaxed);
    }
    /// How long since the last RTP packet, counting from the session start
    /// while none has arrived at all.
    fn since_last_rtp(&self) -> Duration {
        let idle = self.elapsed_ms().saturating_sub(self.last_rtp_ms.load(Ordering::Relaxed));
        Duration::from_millis(idle as u64)
    }
}

pub struct MediaConfig {
    pub codec: Codec,
    pub dtmf_payload_type: Option<u8>,
    pub ptime_ms: u32,
    pub jitter_ms: u32,
    pub symmetric: bool,
    /// Watch the audio coming from the mobile network for DTMF tones.  The
    /// modem's own `DtmfReceived` signal is not reliable (and never fires at
    /// all on modems whose DTMF handling the network rejects), so the tones
    /// are detected in the audio instead.
    pub detect_inband_dtmf: bool,
}

impl MediaSession {
    /// Bind an even RTP port inside the configured range.
    pub async fn bind_port(ip: IpAddr, min: u16, max: u16) -> Result<(UdpSocket, u16)> {
        // Every candidate is an even port whose odd RTCP partner is also in
        // range, so the walk is over `first ..= last` stepping by two.
        let first = min.saturating_add(1) & !1;
        let last = match max.checked_sub(1) {
            Some(m) if m >= first => m & !1,
            _ => return Err(anyhow!("no even RTP port pair fits in {min}..{max}")),
        };
        let slots = ((last - first) / 2 + 1) as u32;
        let mut port = first + 2 * (rand::thread_rng().gen_range(0..slots) as u16);

        for _ in 0..slots {
            match UdpSocket::bind(SocketAddr::new(ip, port)).await {
                Ok(sock) => return Ok((sock, port)),
                // An address that is not on this host fails the same way on
                // every port in the range.  Walking all of them to report "no
                // free port" would send the operator hunting for a conflict
                // that is not there, so anything but a busy port stops here.
                Err(e) if e.kind() != std::io::ErrorKind::AddrInUse => {
                    return Err(anyhow!("cannot bind RTP to {ip}: {e}"));
                }
                Err(_) => {}
            }
            port = if port >= last { first } else { port + 2 };
        }
        Err(anyhow!("every RTP port in {min}..{max} on {ip} is already in use"))
    }

    pub fn start(
        sock: UdpSocket,
        remote: SocketAddr,
        audio: AudioStream,
        cfg: MediaConfig,
        dtmf_tx: mpsc::Sender<char>,
        inband_dtmf_tx: mpsc::Sender<char>,
    ) -> Self {
        let law = match cfg.codec {
            Codec::Pcmu => Law::Ulaw,
            Codec::Pcma => Law::Alaw,
        };
        let payload_type = cfg.codec.payload_type();
        let rings = audio.rings.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let signalled = Arc::new(Mutex::new(Signalled { addr: remote, latched: false }));
        let clock = Arc::new(Clock::new());
        let remote = Arc::new(Mutex::new(remote));
        let sock = Arc::new(sock);

        // Pre-fill the playback side so the first packets do not underrun.
        let prefill = (RTP_RATE as usize * cfg.jitter_ms as usize) / 1000;
        if prefill > 0 {
            rings.push_from_network(&vec![0i16; prefill]);
        }

        let tasks = vec![
            tokio::spawn(sender_loop(
                sock.clone(),
                remote.clone(),
                rings.clone(),
                stop.clone(),
                law,
                payload_type,
                cfg.ptime_ms,
                cfg.detect_inband_dtmf.then_some(inband_dtmf_tx),
            )),
            tokio::spawn(receiver_loop(
                sock,
                remote.clone(),
                signalled.clone(),
                clock.clone(),
                rings,
                stop.clone(),
                law,
                payload_type,
                cfg.dtmf_payload_type,
                cfg.symmetric,
                dtmf_tx,
            )),
        ];

        Self { remote, signalled, clock, rings: audio.rings.clone(), stop, tasks, audio: Some(audio) }
    }

    /// Play DTMF digits into the uplink audio.  Used when the modem cannot
    /// signal them itself (VoLTE calls on modems whose DTMF request the
    /// network rejects).  Returns the number of digits queued.
    pub fn send_dtmf_inband(&self, digits: &str, tone_ms: u32, gap_ms: u32) -> usize {
        let gap = vec![0i16; (RTP_RATE as usize * gap_ms as usize) / 1000];
        let mut queued = 0;
        for digit in digits.chars().filter(|c| !c.is_whitespace()) {
            match codec::dtmf_samples(digit, tone_ms, RTP_RATE) {
                Some(tone) => {
                    // Stop at the first digit that will not fit rather than
                    // pushing one in that gets cut short.
                    if !self.rings.queue_tone(&tone) {
                        warn!(queued, "the in-band DTMF queue is full; the rest was dropped");
                        break;
                    }
                    self.rings.queue_tone(&gap);
                    queued += 1;
                }
                None => debug!(%digit, "not a DTMF digit, ignored"),
            }
        }
        queued
    }

    /// Point the session at a new destination (re-INVITE / late SDP).
    ///
    /// The receiver only accepts audio from the host the signalling named, so
    /// a peer that re-homes its media has to move that expectation too - it
    /// used to keep checking against the original address, and every packet
    /// from the new one was dropped for the rest of the call.  Moving to a
    /// different host also clears the symmetric-RTP latch, so the new
    /// endpoint's own source port can be learned.
    pub fn set_remote(&self, addr: SocketAddr) {
        let mut signalled = self.signalled.lock().unwrap();
        if signalled.addr.ip() != addr.ip() {
            signalled.latched = false;
        }
        signalled.addr = addr;
        *self.remote.lock().unwrap() = addr;
    }

    /// How long the peer has been silent at the RTP level.
    ///
    /// A SIP peer that dies without a BYE - a PBX that is restarted, a
    /// network that goes away - leaves the mobile leg connected and billed
    /// for as long as the process runs, because nothing else on either side
    /// ever notices.  Its RTP stops immediately, so that is what is watched.
    pub fn silence(&self) -> Duration {
        self.clock.since_last_rtp()
    }

    /// Treat the peer as having just been heard from.
    ///
    /// Called when a re-INVITE renegotiates the stream: the silence that
    /// built up while it was on hold, or while it was moving its media, is
    /// not evidence that the peer has gone away now that it is talking again.
    pub fn reset_silence(&self) {
        self.clock.stamp();
    }

    /// Average level of what has been sent to the SIP peer, and over how many
    /// 20 ms frames.  Answers "was the network actually sending ringback?"
    pub fn uplink_level(&self) -> (u32, u32) {
        self.rings.uplink_level()
    }

    pub fn reset_uplink_level(&self) {
        self.rings.reset_uplink_level();
    }

    /// Shared handle on the buffers, for watching the level while a call is
    /// still being set up.
    pub fn rings(&self) -> Arc<AudioRings> {
        self.rings.clone()
    }

    pub fn audio_failed(&self) -> bool {
        self.audio.as_ref().map(|a| a.is_failed()).unwrap_or(false)
    }

    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.tasks.drain(..) {
            t.abort();
        }
        if let Some(audio) = self.audio.take() {
            // Joining the ALSA threads can take a period or two; keep it off
            // the async worker.
            tokio::task::spawn_blocking(move || audio.shutdown());
        }
    }
}

impl Drop for MediaSession {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.tasks.drain(..) {
            t.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn sender_loop(
    sock: Arc<UdpSocket>,
    remote: Arc<Mutex<SocketAddr>>,
    rings: Arc<AudioRings>,
    stop: Arc<AtomicBool>,
    law: Law,
    payload_type: u8,
    ptime_ms: u32,
    inband_dtmf_tx: Option<mpsc::Sender<char>>,
) {
    let samples_per_packet = (RTP_RATE as usize * ptime_ms as usize) / 1000;
    let mut seq: u16 = rand::thread_rng().gen();
    let mut timestamp: u32 = rand::thread_rng().gen();
    let ssrc: u32 = rand::thread_rng().gen();
    let mut payload = Vec::with_capacity(samples_per_packet);
    let mut packet = Vec::with_capacity(12 + samples_per_packet);
    let mut ticker = tokio::time::interval(Duration::from_millis(ptime_ms as u64));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut marker = true;

    // Report a digit once it has been present for three consecutive frames
    // (60 ms - a real digit lasts at least 40 ms and usually far longer), and
    // not again until it goes away.  Two frames turned out to be short enough
    // for a network announcement to trip it.
    const DTMF_FRAMES: u8 = 3;
    let mut dtmf_candidate: Option<char> = None;
    let mut dtmf_streak = 0u8;
    let mut dtmf_reported: Option<char> = None;
    let mut dtmf_gap = 0u8;
    let mut dtmf_guard = 0u8;

    while !stop.load(Ordering::Relaxed) {
        ticker.tick().await;
        let pcm = rings.take_for_network(samples_per_packet);

        // While we are playing a digit towards the modem, ignore the receive
        // detector: the modem's sidetone would otherwise echo our own tone
        // straight back and the SIP peer would see the digit twice.
        if rings.tone_pending() {
            dtmf_guard = 25; // ~500 ms after the tone has drained
        } else {
            dtmf_guard = dtmf_guard.saturating_sub(1);
        }

        if let Some(tx) = inband_dtmf_tx.as_ref().filter(|_| dtmf_guard == 0) {
            match codec::detect_dtmf_detailed(&pcm, RTP_RATE) {
                Some(hit) => {
                    let d = hit.digit;
                    dtmf_gap = 0;
                    if dtmf_candidate == Some(d) {
                        dtmf_streak = dtmf_streak.saturating_add(1);
                    } else {
                        dtmf_candidate = Some(d);
                        dtmf_streak = 1;
                    }
                    if dtmf_streak >= DTMF_FRAMES && dtmf_reported != Some(d) {
                        dtmf_reported = Some(d);
                        debug!(
                            digit = %d,
                            dominance = hit.dominance,
                            twist = hit.twist,
                            "in-band DTMF from the mobile side"
                        );
                        let _ = tx.try_send(d);
                    }
                }
                None => {
                    dtmf_gap = dtmf_gap.saturating_add(1);
                    if dtmf_gap >= 2 {
                        dtmf_candidate = None;
                        dtmf_streak = 0;
                        dtmf_reported = None;
                    }
                }
            }
        }

        codec::encode(law, &pcm, &mut payload);

        packet.clear();
        packet.push(0x80); // V=2
        packet.push(if marker { 0x80 | payload_type } else { payload_type });
        packet.extend_from_slice(&seq.to_be_bytes());
        packet.extend_from_slice(&timestamp.to_be_bytes());
        packet.extend_from_slice(&ssrc.to_be_bytes());
        packet.extend_from_slice(&payload);
        marker = false;

        let dest = *remote.lock().unwrap();
        // A held or rejected stream signals 0.0.0.0:0, which the kernel would
        // happily turn into loopback and get sprayed with call audio.
        if dest.port() != 0 && !dest.ip().is_unspecified() {
            if let Err(e) = sock.send_to(&packet, dest).await {
                trace!(error = %e, "RTP send failed");
            }
        }
        seq = seq.wrapping_add(1);
        timestamp = timestamp.wrapping_add(samples_per_packet as u32);
    }
}

#[allow(clippy::too_many_arguments)]
async fn receiver_loop(
    sock: Arc<UdpSocket>,
    remote: Arc<Mutex<SocketAddr>>,
    signalled: Arc<Mutex<Signalled>>,
    clock: Arc<Clock>,
    rings: Arc<AudioRings>,
    stop: Arc<AtomicBool>,
    law: Law,
    payload_type: u8,
    dtmf_pt: Option<u8>,
    symmetric: bool,
    dtmf_tx: mpsc::Sender<char>,
) {
    let mut buf = vec![0u8; 2048];
    let mut pcm = Vec::with_capacity(320);
    let mut last_dtmf_ts: Option<u32> = None;

    while !stop.load(Ordering::Relaxed) {
        let (len, src) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                trace!(error = %e, "RTP recv failed");
                // Some errors (a queued ICMP unreachable, an interface going
                // away) reproduce on the very next call, which would turn
                // this into a busy loop that starves the sender.
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
        };

        // Only the host the signalling named gets to be heard.  The port is
        // allowed to differ - that is what makes RTP work behind NAT - but a
        // completely unrelated host must not be able to take over the call
        // audio by winning a race with the peer's first packet.  Re-read on
        // every packet: a re-INVITE may have moved the peer since the session
        // started, and audio from where it moved to has to be accepted.
        let expected = signalled.lock().unwrap().addr;
        if !expected.ip().is_unspecified() && src.ip() != expected.ip() {
            trace!(from = %src, %expected, "ignoring RTP from an unexpected host");
            continue;
        }

        if len < 12 {
            continue;
        }
        let header = &buf[..12];
        if header[0] >> 6 != 2 {
            continue;
        }
        // Anything that parses as RTP from the expected host counts as the
        // peer being alive, whether or not we can decode its payload type.
        clock.stamp();

        // Symmetric RTP: reply to wherever the peer actually turned out to
        // be.  Done only once the datagram has proved to be RTP, so a stray
        // packet cannot redirect the call audio.
        if symmetric {
            let mut state = signalled.lock().unwrap();
            if !state.latched {
                if src != state.addr {
                    debug!(from = %src, previous = %state.addr, "latching remote RTP address");
                    *remote.lock().unwrap() = src;
                }
                state.latched = true;
            }
        }

        let csrc_count = (header[0] & 0x0F) as usize;
        let has_extension = header[0] & 0x10 != 0;
        let has_padding = header[0] & 0x20 != 0;
        let pt = header[1] & 0x7F;
        let timestamp = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

        let mut offset = 12 + 4 * csrc_count;
        if has_extension {
            if len < offset + 4 {
                continue;
            }
            let ext_words = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
            offset += 4 + 4 * ext_words;
        }
        if offset >= len {
            continue;
        }
        // The last byte of a padded packet says how much of the tail is pad;
        // decoding it would put a click in the audio on every packet.
        let mut end = len;
        if has_padding {
            let pad = buf[len - 1] as usize;
            if pad == 0 || pad > len - offset {
                continue;
            }
            end = len - pad;
        }
        let payload = &buf[offset..end];

        if Some(pt) == dtmf_pt {
            // RFC 2833: event, E|R|volume, duration.  Report on the first
            // packet of an event rather than the end packet - that one is
            // the likeliest of the burst to be lost, and losing it used to
            // mean losing the digit entirely.
            if payload.len() >= 4 && last_dtmf_ts != Some(timestamp) {
                last_dtmf_ts = Some(timestamp);
                if let Some(d) = dtmf_char(payload[0]) {
                    let _ = dtmf_tx.try_send(d);
                }
            }
            continue;
        }

        if pt != payload_type {
            // Comfort noise (13) and anything else we do not handle.
            continue;
        }
        codec::decode(law, payload, &mut pcm);
        rings.push_from_network(&pcm);
    }
    warn!("RTP receiver stopped");
}

fn dtmf_char(event: u8) -> Option<char> {
    Some(match event {
        0..=9 => (b'0' + event) as char,
        10 => '*',
        11 => '#',
        12..=15 => (b'A' + (event - 12)) as char,
        _ => return None,
    })
}
