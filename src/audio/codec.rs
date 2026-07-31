//! G.711 A-law / µ-law and the trivial 8k<->16k resampling the modem cards
//! occasionally need.

/// Linear 16-bit PCM -> µ-law (G.711, ITU-T).
pub fn linear_to_ulaw(sample: i16) -> u8 {
    const BIAS: i32 = 0x84;
    const CLIP: i32 = 32635;
    let mut pcm = sample as i32;
    let sign = if pcm < 0 {
        pcm = -pcm;
        0x80u8
    } else {
        0
    };
    if pcm > CLIP {
        pcm = CLIP;
    }
    pcm += BIAS;
    let exponent = {
        let mut exp = 7u8;
        let mut mask = 0x4000;
        while exp > 0 && (pcm & mask) == 0 {
            exp -= 1;
            mask >>= 1;
        }
        exp
    };
    let mantissa = ((pcm >> (exponent as i32 + 3)) & 0x0F) as u8;
    !(sign | (exponent << 4) | mantissa)
}

pub fn ulaw_to_linear(ulaw: u8) -> i16 {
    let u = !ulaw;
    let sign = u & 0x80;
    let exponent = (u >> 4) & 0x07;
    let mantissa = u & 0x0F;
    let mut sample = (((mantissa as i32) << 3) + 0x84) << exponent;
    sample -= 0x84;
    if sign != 0 {
        -sample as i16
    } else {
        sample as i16
    }
}

/// Linear 16-bit PCM -> A-law (G.711, ITU-T).
pub fn linear_to_alaw(sample: i16) -> u8 {
    const CLIP: i32 = 32635;
    let mut pcm = sample as i32;
    let sign = if pcm < 0 {
        pcm = -pcm - 1;
        0x00u8
    } else {
        0x80u8
    };
    if pcm > CLIP {
        pcm = CLIP;
    }
    let encoded = if pcm < 256 {
        (pcm >> 4) as u8
    } else {
        let mut exponent = 7u8;
        let mut mask = 0x4000;
        while exponent > 1 && (pcm & mask) == 0 {
            exponent -= 1;
            mask >>= 1;
        }
        let mantissa = ((pcm >> (exponent as i32 + 3)) & 0x0F) as u8;
        (exponent << 4) | mantissa
    };
    (sign | encoded) ^ 0x55
}

pub fn alaw_to_linear(alaw: u8) -> i16 {
    let a = alaw ^ 0x55;
    let sign = a & 0x80;
    let exponent = (a >> 4) & 0x07;
    let mantissa = (a & 0x0F) as i32;
    let mut sample = if exponent == 0 {
        (mantissa << 4) + 8
    } else {
        ((mantissa << 4) + 0x108) << (exponent - 1)
    };
    if sample > 32767 {
        sample = 32767;
    }
    if sign != 0 {
        sample as i16
    } else {
        -(sample as i16)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Law {
    Ulaw,
    Alaw,
}

pub fn encode(law: Law, pcm: &[i16], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(pcm.len());
    match law {
        Law::Ulaw => out.extend(pcm.iter().map(|s| linear_to_ulaw(*s))),
        Law::Alaw => out.extend(pcm.iter().map(|s| linear_to_alaw(*s))),
    }
}

pub fn decode(law: Law, payload: &[u8], out: &mut Vec<i16>) {
    out.clear();
    out.reserve(payload.len());
    match law {
        Law::Ulaw => out.extend(payload.iter().map(|b| ulaw_to_linear(*b))),
        Law::Alaw => out.extend(payload.iter().map(|b| alaw_to_linear(*b))),
    }
}

/// Apply a linear gain with saturation.
pub fn apply_gain(samples: &mut [i16], gain: f32) {
    if (gain - 1.0).abs() < f32::EPSILON {
        return;
    }
    for s in samples.iter_mut() {
        let v = (*s as f32 * gain).round();
        *s = v.clamp(i16::MIN as f32, i16::MAX as f32) as i16;
    }
}

/// The two frequencies that make up a DTMF digit (ITU-T Q.23).
pub fn dtmf_frequencies(digit: char) -> Option<(f32, f32)> {
    let low = match digit {
        '1' | '2' | '3' | 'A' | 'a' => 697.0,
        '4' | '5' | '6' | 'B' | 'b' => 770.0,
        '7' | '8' | '9' | 'C' | 'c' => 852.0,
        '*' | '0' | '#' | 'D' | 'd' => 941.0,
        _ => return None,
    };
    let high = match digit {
        '1' | '4' | '7' | '*' => 1209.0,
        '2' | '5' | '8' | '0' => 1336.0,
        '3' | '6' | '9' | '#' => 1477.0,
        'A' | 'a' | 'B' | 'b' | 'C' | 'c' | 'D' | 'd' => 1633.0,
        _ => return None,
    };
    Some((low, high))
}

/// Generate one DTMF digit as PCM.
///
/// Needed because a VoLTE call has no CS domain: both `Call.SendDtmf` (QMI)
/// and `AT+VTS` ask the network to generate the tone and get rejected, so the
/// gateway plays it into the modem's uplink audio instead - exactly what a
/// handset with in-band signalling does.  The high group is sent ~2 dB louder
/// than the low group (standard "twist"), and both ends are ramped so the
/// tone does not splatter.
pub fn dtmf_samples(digit: char, ms: u32, rate: u32) -> Option<Vec<i16>> {
    const LOW_AMP: f32 = 7000.0;
    const HIGH_AMP: f32 = 8800.0;
    let (f_low, f_high) = dtmf_frequencies(digit)?;
    let total = (rate as u64 * ms as u64 / 1000) as usize;
    let ramp = ((rate as u64 * 4 / 1000) as usize).min(total / 2).max(1);
    let two_pi = std::f32::consts::TAU;

    let mut out = Vec::with_capacity(total);
    for i in 0..total {
        let t = i as f32 / rate as f32;
        let mut s = LOW_AMP * (two_pi * f_low * t).sin() + HIGH_AMP * (two_pi * f_high * t).sin();
        // Raised-cosine attack/release.
        let env = if i < ramp {
            0.5 * (1.0 - (std::f32::consts::PI * i as f32 / ramp as f32).cos())
        } else if i >= total - ramp {
            let k = total - i;
            0.5 * (1.0 - (std::f32::consts::PI * k as f32 / ramp as f32).cos())
        } else {
            1.0
        };
        s *= env;
        out.push(s.clamp(i16::MIN as f32, i16::MAX as f32) as i16);
    }
    Some(out)
}

const DTMF_LOW: [f32; 4] = [697.0, 770.0, 852.0, 941.0];
const DTMF_HIGH: [f32; 4] = [1209.0, 1336.0, 1477.0, 1633.0];
const DTMF_KEYS: [[char; 4]; 4] = [
    ['1', '2', '3', 'A'],
    ['4', '5', '6', 'B'],
    ['7', '8', '9', 'C'],
    ['*', '0', '#', 'D'],
];

/// Energy at one frequency (Goertzel), normalised by the window length.
pub fn goertzel(samples: &[i16], freq: f32, rate: f32) -> f32 {
    let n = samples.len() as f32;
    if n < 8.0 {
        return 0.0;
    }
    let k = (0.5 + n * freq / rate).floor();
    let w = 2.0 * std::f32::consts::PI * k / n;
    let coeff = 2.0 * w.cos();
    let (mut q1, mut q2) = (0.0f32, 0.0f32);
    for s in samples {
        let q0 = coeff * q1 - q2 + *s as f32;
        q2 = q1;
        q1 = q0;
    }
    ((q1 * q1 + q2 * q2 - q1 * q2 * coeff).max(0.0)).sqrt() / n
}

/// What a detected digit looked like, for logging and for tuning.
#[derive(Debug, Clone, Copy)]
pub struct DtmfHit {
    pub digit: char,
    /// Share of the frame's energy sitting in the two winning tones.
    pub dominance: f32,
    /// High group over low group.  A real digit is roughly balanced.
    pub twist: f32,
}

/// Detect a DTMF digit in one analysis window.
///
/// Requires a clear winner in each group (4x the runner-up) so speech does
/// not produce phantom digits.
pub fn detect_dtmf_detailed(samples: &[i16], rate: u32) -> Option<DtmfHit> {
    let rate = rate as f32;
    let low: Vec<f32> = DTMF_LOW.iter().map(|f| goertzel(samples, *f, rate)).collect();
    let high: Vec<f32> = DTMF_HIGH.iter().map(|f| goertzel(samples, *f, rate)).collect();

    let best = |v: &[f32]| -> (usize, f32, f32) {
        let mut idx = 0;
        for (i, x) in v.iter().enumerate() {
            if *x > v[idx] {
                idx = i;
            }
        }
        let runner = v
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, x)| *x)
            .fold(0.0f32, f32::max);
        (idx, v[idx], runner)
    };

    let (li, lv, lr) = best(&low);
    let (hi, hv, hr) = best(&high);
    // The low group's tones are only 73-89 Hz apart, so a 25 ms window (40 Hz
    // bins) always leaks a good part of the neighbour.  Real detectors ask for
    // roughly 8 dB of separation, not the 12 dB a 4x rule would demand.
    const FLOOR: f32 = 120.0;
    const SEPARATION: f32 = 2.5;
    if lv < FLOOR || hv < FLOOR || lv < SEPARATION * lr || hv < SEPARATION * hr {
        return None;
    }

    // A digit is two tones and nothing else, so almost all of the frame's
    // energy has to sit in those two bins.  Speech and ringback have their
    // energy spread out and would otherwise pass the tests above now and
    // then - a caller listening to an announcement was getting phantom
    // digits relayed to them as SIP INFO.
    //
    // Measured on this ratio: a clean digit scores 0.99, one buried under
    // 80 % noise still scores 0.85, and the loudest frame of ten seconds of
    // recorded network announcement reaches 0.81.
    const DOMINANCE: f32 = 0.85;
    let mean_abs =
        samples.iter().map(|s| s.unsigned_abs() as f32).sum::<f32>() / samples.len() as f32;
    let dominance = if mean_abs > 0.0 { (lv + hv) / mean_abs } else { 0.0 };
    if dominance < DOMINANCE {
        return None;
    }

    // Twist: the two tones of a digit are generated together and arrive at
    // comparable levels (the standard allows the high group 4 dB above and
    // 8 dB below).  Two unrelated peaks in music or speech rarely are.
    const TWIST_MIN: f32 = 0.3;
    const TWIST_MAX: f32 = 2.0;
    let twist = hv / lv;
    if !(TWIST_MIN..=TWIST_MAX).contains(&twist) {
        return None;
    }

    Some(DtmfHit { digit: DTMF_KEYS[li][hi], dominance, twist })
}

/// Integer-ratio linear resampler good enough for narrow-band voice.
/// (8k <-> 16k, occasionally 8k <-> 48k on modems with a UAC2 card.)
#[derive(Debug, Clone)]
pub struct Resampler {
    from: u32,
    to: u32,
    last: i16,
    pos: f64,
}

impl Resampler {
    pub fn new(from: u32, to: u32) -> Self {
        Self { from, to, last: 0, pos: 0.0 }
    }

    pub fn process(&mut self, input: &[i16], out: &mut Vec<i16>) {
        out.clear();
        if self.from == self.to {
            out.extend_from_slice(input);
            return;
        }
        let step = self.from as f64 / self.to as f64;
        let mut pos = self.pos;
        while pos < input.len() as f64 {
            let idx = pos.floor() as usize;
            let frac = pos - idx as f64;
            let a = if idx == 0 { self.last } else { input[idx - 1] };
            let b = input[idx];
            let sample = a as f64 + (b as f64 - a as f64) * frac;
            out.push(sample.round().clamp(i16::MIN as f64, i16::MAX as f64) as i16);
            pos += step;
        }
        self.pos = pos - input.len() as f64;
        if let Some(last) = input.last() {
            self.last = *last;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk a buffer and return the digits in it.  Only the tests need this;
    /// the live path detects one digit at a time, per RTP packet.
    fn scan_dtmf(samples: &[i16], rate: u32) -> String {
        let window = (rate as usize) / 40; // 25 ms
        let mut out = String::new();
        let mut last: Option<char> = None;
        let mut miss = 0;
        for chunk in samples.chunks(window) {
            match detect_dtmf_detailed(chunk, rate).map(|h| h.digit) {
                Some(d) => {
                    if last != Some(d) {
                        out.push(d);
                    }
                    last = Some(d);
                    miss = 0;
                }
                None => {
                    miss += 1;
                    // Two silent windows end the digit, so a repeated key is
                    // not merged with the previous one.
                    if miss >= 2 {
                        last = None;
                    }
                }
            }
        }
        out
    }

    #[test]
    fn ulaw_round_trip_is_close() {
        for sample in [-32000i16, -1000, -1, 0, 1, 1000, 32000] {
            let decoded = ulaw_to_linear(linear_to_ulaw(sample));
            let err = (decoded as i32 - sample as i32).abs();
            assert!(err <= (sample.unsigned_abs() as i32 / 8) + 256, "sample {sample} -> {decoded}");
        }
    }

    #[test]
    fn alaw_round_trip_is_close() {
        for sample in [-32000i16, -1000, -1, 0, 1, 1000, 32000] {
            let decoded = alaw_to_linear(linear_to_alaw(sample));
            let err = (decoded as i32 - sample as i32).abs();
            assert!(err <= (sample.unsigned_abs() as i32 / 8) + 256, "sample {sample} -> {decoded}");
        }
    }

    #[test]
    fn dtmf_tone_carries_both_frequencies() {
        // '3' is 697 Hz + 1477 Hz.
        let tone = dtmf_samples('3', 180, 8000).unwrap();
        assert_eq!(tone.len(), 1440);

        let on_low = goertzel(&tone, 697.0, 8000.0);
        let on_high = goertzel(&tone, 1477.0, 8000.0);
        let off_low = goertzel(&tone, 770.0, 8000.0);
        let off_high = goertzel(&tone, 1209.0, 8000.0);

        assert!(on_low > 10.0 * off_low, "low group {on_low} vs {off_low}");
        assert!(on_high > 10.0 * off_high, "high group {on_high} vs {off_high}");
        // Standard twist: the high group is the louder one.
        assert!(on_high > on_low);
        // Ramped ends, so no sample may clip.
        assert!(tone.iter().all(|s| s.unsigned_abs() < 20000));
    }

    #[test]
    fn every_dtmf_digit_is_generated() {
        for d in "0123456789*#ABCD".chars() {
            assert!(dtmf_samples(d, 40, 8000).is_some(), "digit {d}");
        }
        assert!(dtmf_samples('x', 40, 8000).is_none());
    }

    #[test]
    fn generated_digits_are_detected_again() {
        // Generator and detector are independent implementations of the two
        // halves, so this pins both.
        let mut buf: Vec<i16> = Vec::new();
        for d in "0123456789*#".chars() {
            buf.extend(dtmf_samples(d, 120, 8000).unwrap());
            buf.extend(std::iter::repeat(0).take(800)); // 100 ms gap
        }
        assert_eq!(scan_dtmf(&buf, 8000), "0123456789*#");
    }

    /// A DTMF pair hidden inside other audio must be rejected: that is what
    /// a network announcement looks like, and it used to be relayed to the
    /// caller as a phantom digit.
    #[test]
    fn dtmf_pair_buried_in_other_audio_is_rejected() {
        let two_pi = std::f32::consts::TAU;
        let mut buf = Vec::new();
        for i in 0..1600 {
            let t = i as f32 / 8000.0;
            // digit '2' (697 + 1336) plus company at a similar level
            let s = 5000.0 * (two_pi * 697.0 * t).sin()
                + 5000.0 * (two_pi * 1336.0 * t).sin()
                + 4500.0 * (two_pi * 320.0 * t).sin()
                + 4500.0 * (two_pi * 2400.0 * t).sin();
            buf.push(s.clamp(-32768.0, 32767.0) as i16);
        }
        assert_eq!(scan_dtmf(&buf, 8000), "");

        // The same pair on its own is still a digit.
        let mut clean = Vec::new();
        for i in 0..1600 {
            let t = i as f32 / 8000.0;
            clean.push(
                (5000.0 * (two_pi * 697.0 * t).sin() + 5000.0 * (two_pi * 1336.0 * t).sin()) as i16,
            );
        }
        assert_eq!(scan_dtmf(&clean, 8000), "2");
    }

    #[test]
    fn speech_like_noise_produces_no_digits() {
        // A 1 kHz tone plus noise must not be mistaken for a digit.
        let mut buf = Vec::new();
        let mut seed = 12345u32;
        for i in 0..8000 {
            seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            let noise = ((seed >> 16) as i16 as f32) / 64.0;
            let t = i as f32 / 8000.0;
            buf.push((6000.0 * (std::f32::consts::TAU * 1000.0 * t).sin() + noise) as i16);
        }
        assert_eq!(scan_dtmf(&buf, 8000), "");
    }

    #[test]
    fn resample_doubles_and_halves() {
        let mut up = Resampler::new(8000, 16000);
        let mut out = Vec::new();
        up.process(&[100; 160], &mut out);
        assert!((out.len() as i32 - 320).abs() <= 1);

        let mut down = Resampler::new(16000, 8000);
        down.process(&[100; 320], &mut out);
        assert!((out.len() as i32 - 160).abs() <= 1);
    }
}
