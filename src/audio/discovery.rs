//! Map a ModemManager modem to *its own* ALSA card.
//!
//! On a box with several identical modems, "card 1" is meaningless: after a
//! reboot or a replug the numbering moves.  ModemManager tells us the sysfs
//! path of the physical device (`Device` property); every ALSA card carries a
//! `device` symlink back into sysfs.  The card whose device link lives under
//! the modem's device path is the modem's card - unambiguous by construction.

use std::path::{Path, PathBuf};

use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct AlsaCard {
    pub index: u32,
    /// Short ALSA id, e.g. "Module" or "EC25".
    pub id: String,
    /// Longer human readable name from /proc/asound/cards.
    pub name: String,
    /// Canonical sysfs path of the device backing the card.
    pub device_path: PathBuf,
    pub has_playback: bool,
    pub has_capture: bool,
}

impl AlsaCard {
    /// ALSA device string for this card.  `plughw` lets alsa-lib convert
    /// rate/format when the modem card is picky (many UAC1 modems only do
    /// 16 kHz mono S16_LE).
    pub fn device_string(&self, plug: bool) -> String {
        let prefix = if plug { "plughw" } else { "hw" };
        format!("{prefix}:{},0", self.id)
    }
}

/// Enumerate the sound cards present on the system.
pub fn list_cards() -> Vec<AlsaCard> {
    let mut out = Vec::new();
    let long_names = proc_asound_names();
    let Ok(entries) = std::fs::read_dir("/sys/class/sound") else {
        warn!("/sys/class/sound is not readable; ALSA auto-detection disabled");
        return out;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let Some(index) = name.strip_prefix("card").and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        let base = entry.path();
        let id = std::fs::read_to_string(base.join("id"))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| index.to_string());
        let device_path = std::fs::canonicalize(base.join("device")).unwrap_or_default();

        let mut has_playback = false;
        let mut has_capture = false;
        if let Ok(subs) = std::fs::read_dir(&base) {
            for sub in subs.flatten() {
                let n = sub.file_name();
                let n = n.to_string_lossy();
                if n.starts_with("pcmC") {
                    if n.ends_with('p') {
                        has_playback = true;
                    } else if n.ends_with('c') {
                        has_capture = true;
                    }
                }
            }
        }

        out.push(AlsaCard {
            index,
            name: long_names.get(&index).cloned().unwrap_or_else(|| id.clone()),
            id,
            device_path,
            has_playback,
            has_capture,
        });
    }
    out.sort_by_key(|c| c.index);
    out
}

fn proc_asound_names() -> std::collections::HashMap<u32, String> {
    let mut map = std::collections::HashMap::new();
    let Ok(text) = std::fs::read_to_string("/proc/asound/cards") else { return map };
    // " 1 [Module         ]: USB-Audio - Android
    //                        Android Android at usb-0000:00:14.0-3, high speed"
    for line in text.lines() {
        let trimmed = line.trim_start();
        let Some(idx_end) = trimmed.find(|c: char| c.is_whitespace()) else { continue };
        let Ok(index) = trimmed[..idx_end].parse::<u32>() else { continue };
        if let Some(pos) = trimmed.find(": ") {
            map.insert(index, trimmed[pos + 2..].trim().to_string());
        }
    }
    map
}

/// Find the card belonging to the modem whose sysfs path is `modem_device`.
///
/// `hint` (config `audio.card_hint`) is only used to disambiguate when the
/// sysfs walk yields more than one candidate, or as a last resort when the
/// modem exposes its audio through an unrelated device node.
pub fn find_for_modem(modem_device: &str, hint: Option<&str>) -> Option<AlsaCard> {
    let cards = list_cards();
    if cards.is_empty() {
        return None;
    }
    let modem_path = std::fs::canonicalize(modem_device).unwrap_or_else(|_| PathBuf::from(modem_device));

    let mut candidates: Vec<AlsaCard> = cards
        .iter()
        .filter(|c| c.has_playback && c.has_capture)
        .filter(|c| is_under(&c.device_path, &modem_path))
        .cloned()
        .collect();

    if candidates.is_empty() {
        // Some drivers anchor the sound card at the USB device while
        // ModemManager reports one of its interfaces (or the other way
        // round).  Try matching on the shared USB device directory.
        if let Some(usb_root) = usb_device_root(&modem_path) {
            candidates = cards
                .iter()
                .filter(|c| c.has_playback && c.has_capture)
                .filter(|c| {
                    usb_device_root(&c.device_path)
                        .map(|r| r == usb_root)
                        .unwrap_or(false)
                })
                .cloned()
                .collect();
        }
    }

    if candidates.len() > 1 {
        if let Some(h) = hint {
            let h = h.to_ascii_lowercase();
            let narrowed: Vec<AlsaCard> = candidates
                .iter()
                .filter(|c| {
                    c.id.to_ascii_lowercase().contains(&h) || c.name.to_ascii_lowercase().contains(&h)
                })
                .cloned()
                .collect();
            if !narrowed.is_empty() {
                candidates = narrowed;
            }
        }
        warn!(
            count = candidates.len(),
            "several ALSA cards match this modem; using card {}", candidates[0].index
        );
    }

    if let Some(card) = candidates.into_iter().next() {
        debug!(card = card.index, id = %card.id, path = %card.device_path.display(), "modem ALSA card matched via sysfs");
        return Some(card);
    }

    // Last resort: hint-only match, logged loudly because it cannot
    // distinguish two identical modems.
    if let Some(h) = hint {
        let h = h.to_ascii_lowercase();
        if let Some(card) = cards.into_iter().find(|c| {
            c.has_playback
                && c.has_capture
                && (c.id.to_ascii_lowercase().contains(&h) || c.name.to_ascii_lowercase().contains(&h))
        }) {
            warn!(
                card = card.index,
                id = %card.id,
                "ALSA card selected by name hint only - verify it belongs to this modem"
            );
            return Some(card);
        }
    }
    None
}

fn is_under(path: &Path, ancestor: &Path) -> bool {
    if path.as_os_str().is_empty() || ancestor.as_os_str().is_empty() {
        return false;
    }
    path.starts_with(ancestor)
}

/// For `/sys/devices/.../usb1/1-3/1-3:1.2` return `/sys/devices/.../usb1/1-3`.
///
/// The walk stops at the *first* (deepest) device it meets.  Continuing to
/// the top would return the hub two identical modems are plugged into, and
/// then both of their cards look like candidates for both of them - exactly
/// the mix-up this whole matching scheme exists to prevent.
fn usb_device_root(path: &Path) -> Option<PathBuf> {
    let mut current = Some(path);
    while let Some(p) = current {
        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            // USB interfaces look like "1-3:1.2", devices like "1-3" or "1-3.4".
            if !name.contains(':') && name.contains('-') && name.starts_with(|c: char| c.is_ascii_digit()) {
                return Some(p.to_path_buf());
            }
        }
        current = p.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usb_root_extraction() {
        let p = PathBuf::from("/sys/devices/pci0000:00/0000:00:14.0/usb1/1-3/1-3:1.2/sound/card1");
        assert_eq!(
            usb_device_root(&p).unwrap(),
            PathBuf::from("/sys/devices/pci0000:00/0000:00:14.0/usb1/1-3")
        );
    }

    /// Two identical modems on one hub must not share a root, or each one
    /// sees the other's sound card as a candidate.
    #[test]
    fn devices_behind_a_hub_stay_distinct() {
        let base = "/sys/devices/pci0000:00/0000:00:14.0/usb1/1-3";
        let a = PathBuf::from(format!("{base}/1-3.1/1-3.1:1.2/sound/card1"));
        let b = PathBuf::from(format!("{base}/1-3.2/1-3.2:1.2/sound/card2"));
        assert_eq!(usb_device_root(&a).unwrap(), PathBuf::from(format!("{base}/1-3.1")));
        assert_eq!(usb_device_root(&b).unwrap(), PathBuf::from(format!("{base}/1-3.2")));
        assert_ne!(usb_device_root(&a), usb_device_root(&b));
    }
}
