//! The async [`Client`], re-exported at the crate root, and the blocking
//! [`sync::Client`].

mod r#async;
pub mod sync;

pub use r#async::{Client, ClientBuilder};

/// Matches `Constants.SENSE_PROTOCOL_VERSION` in the Android app.
pub(crate) const PROTOCOL_VERSION: &str = "12";
