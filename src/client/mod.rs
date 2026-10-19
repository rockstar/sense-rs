//! The async [`Client`], re-exported at the crate root, and the blocking
//! [`sync::Client`].

mod r#async;
pub mod sync;

pub use r#async::{Client, ClientBuilder};

/// Sent as the `X-Sense-Protocol` header.
pub(crate) const PROTOCOL_VERSION: &str = "12";

/// How long establishing a connection (HTTP or websocket) may take.
pub(crate) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
