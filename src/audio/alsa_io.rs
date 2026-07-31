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
// AtomicU32 rather than AtomicU64: the 32-bit OpenWrt targets have no 64-bit
// atomics, and these are event counters that will never come close to 2^32.
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use alsa::pcm::{Access, Format, Frames, HwParams, PCM};
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
    /// Target buffering depth; also the backlog the rings are trimmed to.
    pub jitter_ms: u32,
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
    /// Backlog each ring is trimmed back to once it runs long.
    target: usize,
    /// Backlog that triggers a trim.
    high_water: usize,
    pub underruns: AtomicU32,
    pub overruns: AtomicU32,
}

/// Hard ceiling on queued DTMF, in samples (~8 s at 8 kHz).
const MAX_TONE_SAMPLES: usize = RTP_RATE as usize * 8;

impl AudioRings {
    /// Sized from the target jitter depth: that is how much delay the call is
    /// meant to carry, and anything beyond it is drift to be given back.
    fn new(jitter_ms: u32, period_ms: u32) -> Self {
        let ms = |n: u32| (RTP_RATE as usize * n as usize) / 1000;
        let target = ms(jitter_ms.max(period_ms)).max(ms(20));
        Self { target, high_water: target * 3, ..Default::default() }
    }

    /// Give back the delay a ring has accumulated beyond its target.
    ///
    /// The card's crystal and the packet clock never run at exactly the same
    /// rate, and a scheduling hiccup adds a step change on top, so one side
    /// always drifts. Dropping a single sample per push only ever pinned the
    /// backlog at the ceiling, leaving the rest of the call with that much
    /// delay and no way back; trimming to the target restores it in one go.
    fn trim(&self, ring: &mut VecDeque<i16>) {
        if ring.len() > self.high_water {
            let excess = ring.len() - self.target;
            ring.drain(..excess);
            self.overruns.fetch_add(1, Ordering::Relaxed);
        }
    }

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

    fn push_from_card(&self, samples: &[i16]) {
        let mut ring = self.to_network.lock().unwrap();
        ring.extend(samples.iter().copied());
        self.trim(&mut ring);
    }

    /// Feed decoded RTP audio towards the modem.
    pub fn push_from_network(&self, samples: &[i16]) {
        let mut ring = self.to_modem.lock().unwrap();
        ring.extend(samples.iter().copied());
        self.trim(&mut ring);
    }

    /// Queue locally generated uplink audio (a DTMF digit).
    ///
    /// Returns false when the queue is full: dropping the head would cut a
    /// tone in half, so a caller pasting a long PIN is told the tail did not
    /// fit rather than being handed a corrupted string.
    pub fn queue_tone(&self, samples: &[i16]) -> bool {
        let mut tone = self.tone.lock().unwrap();
        if tone.len() + samples.len() > MAX_TONE_SAMPLES {
            return false;
        }
        tone.extend(samples.iter().copied());
        true
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
        let rings = Arc::new(AudioRings::new(params.jitter_ms, params.period_ms));
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
        // Whichever one did open has to be shut down before we give up, or it
        // keeps holding the card and the next call gets EBUSY from a modem
        // that then looks permanently broken.
        for _ in 0..2 {
            let (failure, timed_out) = match ready_rx.recv_timeout(OPEN_TIMEOUT) {
                Ok(Ok((direction, rate))) => {
                    info!(direction, rate, "modem audio open");
                    continue;
                }
                Ok(Err(e)) => (anyhow!("{e}"), false),
                Err(_) => (anyhow!("timed out opening the modem sound card"), true),
            };
            if timed_out {
                // A thread that has not reported is still inside a blocking
                // snd_pcm_open, where it cannot see `stop` - joining it would
                // wait exactly as long as the open does (on a wedged USB
                // audio function, forever) and take the gateway's event loop
                // down with it, since this runs on a blocking-pool task the
                // loop is awaiting.  Let them finish on their own instead:
                // they see `stop` as soon as the open returns and exit.
                warn!("the modem sound card did not open in time; abandoning the audio threads");
                stream.detach();
            } else {
                stream.shutdown();
            }
            return Err(failure);
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

    /// Ask the threads to stop but do not wait for them.
    ///
    /// Only for the open timeout: a thread stuck in `snd_pcm_open` cannot see
    /// `stop` until the call returns, so joining it has no deadline.
    fn detach(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        std::mem::take(&mut self.threads);
    }
}

impl Drop for AudioStream {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

type Ready = mpsc::Sender<Result<(&'static str, u32), String>>;

/// Frames in `ms` milliseconds at `rate`.
///
/// alsa-lib counts frames in a C long, so this is 32 bits wide on the 32-bit
/// OpenWrt targets.  `Config::validate` bounds the rate, the period and the
/// period count precisely so the products below stay well inside that.
fn period_frames(rate: u32, ms: u32) -> Frames {
    (rate as u64 * ms as u64 / 1000) as Frames
}

/// Returns the PCM plus the rate the card actually accepted.
fn open_pcm(device: &str, dir: Direction, params: &AudioParams) -> Result<(PCM, u32), alsa::Error> {
    let pcm = PCM::new(device, dir, false)?;
    let rate = {
        let hwp = HwParams::any(&pcm)?;
        hwp.set_access(Access::RWInterleaved)?;
        hwp.set_format(Format::s16())?;
        hwp.set_channels(1)?;
        hwp.set_rate(params.card_rate, ValueOr::Nearest)?;
        let period = period_frames(params.card_rate, params.period_ms);
        hwp.set_period_size_near(period, ValueOr::Nearest)?;
        hwp.set_buffer_size_near(period * params.periods.max(2) as Frames)?;
        pcm.hw_params(&hwp)?;
        hwp.get_rate()?
    };
    {
        let period = period_frames(rate, params.period_ms);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rings() -> AudioRings {
        AudioRings::new(60, 20)
    }

    /// Drift used to pin the backlog at the ceiling for the rest of the call.
    /// It has to come back down to the target instead.
    #[test]
    fn a_ring_that_runs_long_is_trimmed_back_to_the_target() {
        let r = rings();
        let target = r.target;
        for _ in 0..200 {
            r.push_from_network(&vec![0i16; 160]);
        }
        let depth = r.to_modem.lock().unwrap().len();
        assert!(depth <= r.high_water, "{depth} samples still queued");
        assert!(depth >= target, "trimmed below the target: {depth} < {target}");
        // One trim per overflow, not one per dropped sample.
        assert!(r.overruns.load(Ordering::Relaxed) < 200);
    }

    /// A backlog inside the target is the jitter buffer doing its job.
    #[test]
    fn normal_buffering_is_left_alone() {
        let r = rings();
        r.push_from_network(&vec![0i16; 480]); // 60 ms
        assert_eq!(r.to_modem.lock().unwrap().len(), 480);
        assert_eq!(r.overruns.load(Ordering::Relaxed), 0);
    }

    /// Truncating the queue mid-tone corrupted the digit that was cut; the
    /// caller is told it did not fit instead.
    #[test]
    fn a_full_tone_queue_refuses_rather_than_truncating() {
        let r = rings();
        let digit = vec![0i16; 2080]; // 260 ms, one digit plus its gap
        let mut accepted = 0;
        while r.queue_tone(&digit) {
            accepted += 1;
            assert!(accepted < 100, "the queue never filled up");
        }
        assert!(accepted > 20, "only {accepted} digits fitted");
        assert!(r.tone.lock().unwrap().len() <= MAX_TONE_SAMPLES);
    }
}
