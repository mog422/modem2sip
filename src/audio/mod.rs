pub mod alsa_io;
pub mod codec;
pub mod discovery;

pub use alsa_io::{AudioParams, AudioRings, AudioStream, RTP_RATE};
pub use discovery::{find_for_modem, list_cards, AlsaCard};
