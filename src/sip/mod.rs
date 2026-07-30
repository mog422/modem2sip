pub mod auth;
pub mod core;
pub mod message;
pub mod register;
pub mod registrar;
pub mod sdp;
pub mod transport;
pub mod uri;

// `self::` is required: a bare `core::` would resolve to the built-in crate.
pub use self::core::SipCore;
