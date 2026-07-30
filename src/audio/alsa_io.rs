//! Full-duplex ALSA bridge for the modem's sound card.
//!
//! alsa-lib is blocking, so capture and playback each get a dedicated OS
//! thread.  They exchange 8 kHz mono PCM with the RTP side through two small
//! ring buffers; rate conversion to/from the card's native rate happens here.
//!
//! Each thread opens its own PCM handle: that keeps the handles thread-local
//! (no assumptions about them being movable between threads) and lets a busy
//! or missing card fail the call instead of the process.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use alsa::pcm::{Access, Format, HwParams, PCM};
use alsa::{Direction, ValueOr};
use anyhow::{anyhow, Result};
use tracing::{debug, info, warn};

use super::codec::{apply_gain, Resampler};

/// The RTP side always runs at 8 kHz narrow band.
pub const RTP_RATE: u32 = 8000;

/// How long we wait for the card to open before failing the call.
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct AudioParams {
    pub capture_device: String,
    pub playback_device: String,
    pub card_rate: u32,
    pub period_ms: u32,
    pub periods: u32,
    pub tx_gain: f32,
    pub rx_gain: f32,
}

/// Two ring buffers, both holding 8 kHz mono PCM.
#[derive(Default)]
pub struct AudioRings {
    /// Modem microphone -> RTP.
    to_network: Mutex<VecDeque<i16>>,
    /// RTP -> modem speaker.
    to_modem: Mutex<VecDeque<i16>>,
    /// Locally generated audio (DTMF) that takes over the uplink while it
    /// lasts.  Replacing rather than mixing keeps the tone clean for the
    /// detector at the far end.
    tone: Mutex<VecDeque<i16>>,
    pub underruns: AtomicU64,
    pub overruns: AtomicU64,
}

const MAX_RING_SAMPLES: usize = RTP_RATE as usize; // one second

impl AudioRings {
    /// Take exactly `n` samples for the network, padding with silence when
    /// the card has not produced enough yet (call setup, xrun recovery).
    pub fn take_for_network(&self, n: usize) -> Vec<i16> {
        let mut ring = self.to_network.lock().unwrap();
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(ring.pop_front().unwrap_or(0));
        }
        out
    }

    pub fn available_for_network(&self) -> usize {
        self.to_network.lock().unwrap().len()
    }

    fn push_from_card(&self, samples: &[i16]) {
        let mut ring = self.to_network.lock().unwrap();
        ring.extend(samples.iter().copied());
        while ring.len() > MAX_RING_SAMPLES {
            ring.pop_front();
            self.overruns.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Feed decoded RTP audio towards the modem.
    pub fn push_from_network(&self, samples: &[i16]) {
        let mut ring = self.to_modem.lock().unwrap();
        ring.extend(samples.iter().copied());
        while ring.len() > MAX_RING_SAMPLES {
            ring.pop_front();
            self.overruns.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Queue locally generated uplink audio (a DTMF digit).
    pub fn queue_tone(&self, samples: &[i16]) {
        let mut tone = self.tone.lock().unwrap();
        tone.extend(samples.iter().copied());
        while tone.len() > MAX_RING_SAMPLES * 4 {
            tone.pop_front();
        }
    }

    pub fn tone_pending(&self) -> bool {
        !self.tone.lock().unwrap().is_empty()
    }

    fn take_for_card(&self, n: usize) -> Vec<i16> {
        {
            let mut tone = self.tone.lock().unwrap();
            if !tone.is_empty() {
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    out.push(tone.pop_front().unwrap_or(0));
                }
                // Drop the far-end audio we are talking over, otherwise the
                // call would run late by exactly the length of the tone.
                let mut ring = self.to_modem.lock().unwrap();
                for _ in 0..n {
                    ring.pop_front();
                }
                return out;
            }
        }
        let mut ring = self.to_modem.lock().unwrap();
        if ring.len() < n {
            self.underruns.fetch_add(1, Ordering::Relaxed);
        }
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(ring.pop_front().unwrap_or(0));
        }
        out
    }

    pub fn clear(&self) {
        self.to_network.lock().unwrap().clear();
        self.to_modem.lock().unwrap().clear();
        self.tone.lock().unwrap().clear();
    }
}

pub struct AudioStream {
    pub rings: Arc<AudioRings>,
    pub failed: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl AudioStream {
    /// Blocking: opens both directions and returns once audio is flowing.
    pub fn start(params: AudioParams) -> Result<Self> {
        let rings = Arc::new(AudioRings::default());
        let stop = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(&'static str, u32), String>>();

        let mut threads = Vec::new();
        threads.push({
            let (rings, stop, failed, params, ready) =
                (rings.clone(), stop.clone(), failed.clone(), params.clone(), ready_tx.clone());
            std::thread::Builder::new()
                .name("alsa-capture".into())
                .spawn(move || capture_thread(params, rings, stop, failed, ready))?
        });
        threads.push({
            let (rings, stop, failed, params, ready) =
                (rings.clone(), stop.clone(), failed.clone(), params.clone(), ready_tx);
            std::thread::Builder::new()
                .name("alsa-playback".into())
                .spawn(move || playback_thread(params, rings, stop, failed, ready))?
        });

        let stream = Self { rings, failed, stop, threads };

        // Both threads must report success, otherwise the call is not viable.
        for _ in 0..2 {
            match ready_rx.recv_timeout(OPEN_TIMEOUT) {
                Ok(Ok((direction, rate))) => {
                    info!(direction, rate, "modem audio open");
                }
                Ok(Err(e)) => return Err(anyhow!("{e}")),
                Err(_) => return Err(anyhow!("timed out opening the modem sound card")),
            }
        }
        Ok(stream)
    }

    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // `Drop` is implemented (as a safety net), so the handles have to be
        // moved out rather than consumed by value.
        for t in std::mem::take(&mut self.threads) {
            let _ = t.join();
        }
        debug!("modem audio closed");
    }
}

impl Drop for AudioStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

type Ready = mpsc::Sender<Result<(&'static str, u32), String>>;

/// Returns the PCM plus the rate the card actually accepted.
fn open_pcm(device: &str, dir: Direction, params: &AudioParams) -> Result<(PCM, u32), alsa::Error> {
    let pcm = PCM::new(device, dir, false)?;
    let rate = {
        let hwp = HwParams::any(&pcm)?;
        hwp.set_access(Access::RWInterleaved)?;
        hwp.set_format(Format::s16())?;
        hwp.set_channels(1)?;
        hwp.set_rate(params.card_rate, ValueOr::Nearest)?;
        let period = (params.card_rate as u64 * params.period_ms as u64 / 1000) as i64;
        hwp.set_period_size_near(period, ValueOr::Nearest)?;
        hwp.set_buffer_size_near(period * params.periods.max(2) as i64)?;
        pcm.hw_params(&hwp)?;
        hwp.get_rate()?
    };
    {
        let period = (rate as u64 * params.period_ms as u64 / 1000) as i64;
        let swp = pcm.sw_params_current()?;
        swp.set_start_threshold(if matches!(dir, Direction::Playback) { period } else { 1 })?;
        swp.set_avail_min(period)?;
        pcm.sw_params(&swp)?;
    }
    pcm.prepare()?;
    Ok((pcm, rate))
}

fn capture_thread(
    params: AudioParams,
    rings: Arc<AudioRings>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    ready: Ready,
) {
    let (pcm, card_rate) = match open_pcm(&params.capture_device, Direction::Capture, &params) {
        Ok(v) => {
            let _ = ready.send(Ok(("capture", v.1)));
            v
        }
        Err(e) => {
            let _ = ready.send(Err(format!(
                "opening ALSA capture {}: {e}",
                params.capture_device
            )));
            failed.store(true, Ordering::Relaxed);
            return;
        }
    };

    let frames = (card_rate as usize * params.period_ms as usize) / 1000;
    let mut buf = vec![0i16; frames];
    let mut resampler = Resampler::new(card_rate, RTP_RATE);
    let mut converted: Vec<i16> = Vec::with_capacity(frames * 2);
    let mut errors = 0u32;

    let io = match pcm.io_i16() {
        Ok(io) => io,
        Err(e) => {
            warn!(error = %e, "capture io_i16 failed");
            failed.store(true, Ordering::Relaxed);
            return;
        }
    };
    if let Err(e) = pcm.start() {
        debug!(error = %e, "capture start (may already be running)");
    }

    while !stop.load(Ordering::Relaxed) {
        match io.readi(&mut buf) {
            Ok(n) if n > 0 => {
                errors = 0;
                let mut chunk = buf[..n].to_vec();
                apply_gain(&mut chunk, params.tx_gain);
                resampler.process(&chunk, &mut converted);
                rings.push_from_card(&converted);
            }
            Ok(_) => std::thread::sleep(Duration::from_millis(2)),
            Err(e) => {
                if !recover(&pcm, e, &mut errors, "capture") {
                    failed.store(true, Ordering::Relaxed);
                    return;
                }
            }
        }
    }
}

fn playback_thread(
    params: AudioParams,
    rings: Arc<AudioRings>,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    ready: Ready,
) {
    let (pcm, card_rate) = match open_pcm(&params.playback_device, Direction::Playback, &params) {
        Ok(v) => {
            let _ = ready.send(Ok(("playback", v.1)));
            v
        }
        Err(e) => {
            let _ = ready.send(Err(format!(
                "opening ALSA playback {}: {e}",
                params.playback_device
            )));
            failed.store(true, Ordering::Relaxed);
            return;
        }
    };

    let rtp_frames = (RTP_RATE as usize * params.period_ms as usize) / 1000;
    let mut resampler = Resampler::new(RTP_RATE, card_rate);
    let mut converted: Vec<i16> = Vec::new();
    let mut errors = 0u32;

    let io = match pcm.io_i16() {
        Ok(io) => io,
        Err(e) => {
            warn!(error = %e, "playback io_i16 failed");
            failed.store(true, Ordering::Relaxed);
            return;
        }
    };

    while !stop.load(Ordering::Relaxed) {
        let mut chunk = rings.take_for_card(rtp_frames);
        apply_gain(&mut chunk, params.rx_gain);
        resampler.process(&chunk, &mut converted);

        let mut offset = 0;
        while offset < converted.len() && !stop.load(Ordering::Relaxed) {
            match io.writei(&converted[offset..]) {
                Ok(n) if n > 0 => {
                    offset += n;
                    errors = 0;
                }
                Ok(_) => std::thread::sleep(Duration::from_millis(2)),
                Err(e) => {
                    if !recover(&pcm, e, &mut errors, "playback") {
                        failed.store(true, Ordering::Relaxed);
                        return;
                    }
                    break;
                }
            }
        }
    }
    let _ = pcm.drain();
}

/// Handle xruns and transient errors; returns false when the stream is dead.
fn recover(pcm: &PCM, err: alsa::Error, errors: &mut u32, what: &str) -> bool {
    *errors += 1;
    if *errors > 50 {
        warn!(direction = what, error = %err, "too many ALSA errors, giving up on this stream");
        return false;
    }
    let description = err.to_string();
    if let Err(e) = pcm.try_recover(err, true) {
        warn!(direction = what, error = %e, "ALSA recovery failed");
        std::thread::sleep(Duration::from_millis(20));
        return true;
    }
    let _ = pcm.prepare();
    debug!(direction = what, error = %description, "recovered from ALSA xrun");
    true
}
