//! RTP <-> ALSA plumbing for one call.
//!
//! Narrow band only: G.711 at 8 kHz, 20 ms packets, plus RFC 2833 digit
//! reception.  The ALSA ring buffers double as the jitter buffer - the
//! playback thread drains them at a constant rate, which is exactly the
//! behaviour a jitter buffer needs.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
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
    pub local_port: u16,
    pub remote: Arc<Mutex<SocketAddr>>,
    rings: Arc<AudioRings>,
    stop: Arc<AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
    audio: Option<AudioStream>,
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
        let start = {
            let span = ((max - min) / 2).max(1);
            min + 2 * rand::thread_rng().gen_range(0..span)
        };
        let mut port = start & !1;
        let first = port;
        for _ in 0..((max - min) / 2 + 1) {
            let fits = port >= min && port.checked_add(1).map(|p| p <= max).unwrap_or(false);
            if fits {
                if let Ok(sock) = UdpSocket::bind(SocketAddr::new(ip, port)).await {
                    return Ok((sock, port));
                }
            }
            port = match port.checked_add(2) {
                Some(p) if p + 1 <= max => p,
                _ => (min + 1) & !1, // wrap to the first even port in range
            };
            if port == first {
                break;
            }
        }
        Err(anyhow!("no free RTP port in {min}..{max}"))
    }

    pub fn start(
        sock: UdpSocket,
        local_port: u16,
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
        let remote = Arc::new(Mutex::new(remote));
        let sock = Arc::new(sock);

        // Pre-fill the playback side so the first packets do not underrun.
        let prefill = (RTP_RATE as usize * cfg.jitter_ms as usize) / 1000;
        if prefill > 0 {
            rings.push_from_network(&vec![0i16; prefill]);
        }

        let mut tasks = Vec::new();
        tasks.push(tokio::spawn(sender_loop(
            sock.clone(),
            remote.clone(),
            rings.clone(),
            stop.clone(),
            law,
            payload_type,
            cfg.ptime_ms,
            cfg.detect_inband_dtmf.then_some(inband_dtmf_tx),
        )));
        tasks.push(tokio::spawn(receiver_loop(
            sock,
            remote.clone(),
            rings,
            stop.clone(),
            law,
            payload_type,
            cfg.dtmf_payload_type,
            cfg.symmetric,
            dtmf_tx,
        )));

        Self { local_port, remote, rings: audio.rings.clone(), stop, tasks, audio: Some(audio) }
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
                    self.rings.queue_tone(&tone);
                    self.rings.queue_tone(&gap);
                    queued += 1;
                }
                None => debug!(%digit, "not a DTMF digit, ignored"),
            }
        }
        queued
    }

    /// Point the sender at a new destination (re-INVITE / late SDP).
    pub fn set_remote(&self, addr: SocketAddr) {
        *self.remote.lock().unwrap() = addr;
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

    // Report a digit once, when it has been present for two frames, and not
    // again until it goes away.
    let mut dtmf_candidate: Option<char> = None;
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
            match codec::detect_dtmf(&pcm, RTP_RATE) {
                Some(d) => {
                    dtmf_gap = 0;
                    if dtmf_candidate == Some(d) {
                        if dtmf_reported != Some(d) {
                            dtmf_reported = Some(d);
                            debug!(digit = %d, "in-band DTMF from the mobile side");
                            let _ = tx.try_send(d);
                        }
                    } else {
                        dtmf_candidate = Some(d);
                    }
                }
                None => {
                    dtmf_gap = dtmf_gap.saturating_add(1);
                    if dtmf_gap >= 2 {
                        dtmf_candidate = None;
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
        if let Err(e) = sock.send_to(&packet, dest).await {
            trace!(error = %e, "RTP send failed");
        }
        seq = seq.wrapping_add(1);
        timestamp = timestamp.wrapping_add(samples_per_packet as u32);
    }
}

#[allow(clippy::too_many_arguments)]
async fn receiver_loop(
    sock: Arc<UdpSocket>,
    remote: Arc<Mutex<SocketAddr>>,
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
    let mut latched = false;
    let mut last_dtmf_ts: Option<u32> = None;

    while !stop.load(Ordering::Relaxed) {
        let (len, src) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                trace!(error = %e, "RTP recv failed");
                continue;
            }
        };
        if len < 12 {
            continue;
        }
        let header = &buf[..12];
        if header[0] >> 6 != 2 {
            continue;
        }
        let csrc_count = (header[0] & 0x0F) as usize;
        let has_extension = header[0] & 0x10 != 0;
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
        let payload = &buf[offset..len];

        // Symmetric RTP: trust the first source we hear from, then stick to it.
        if symmetric && !latched {
            let mut cur = remote.lock().unwrap();
            if *cur != src {
                debug!(from = %src, previous = %*cur, "latching remote RTP address");
                *cur = src;
            }
            latched = true;
        }

        if Some(pt) == dtmf_pt {
            // RFC 2833: event, E|R|volume, duration.  Report once per event.
            if payload.len() >= 4 {
                let event = payload[0];
                let end = payload[1] & 0x80 != 0;
                if end && last_dtmf_ts != Some(timestamp) {
                    last_dtmf_ts = Some(timestamp);
                    if let Some(d) = dtmf_char(event) {
                        let _ = dtmf_tx.try_send(d);
                    }
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
