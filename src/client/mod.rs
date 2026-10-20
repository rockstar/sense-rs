//! The async [`Client`], re-exported at the crate root, and (with `native`)
//! the blocking `sync::Client`.

mod r#async;
mod http;
#[cfg(feature = "native")]
pub mod sync;

#[cfg(feature = "native")]
pub(crate) use r#async::api_error;
pub use r#async::{Client, ClientBuilder};

/// Sent as the `X-Sense-Protocol` header.
pub(crate) const PROTOCOL_VERSION: &str = "12";

#[cfg(feature = "native")]
/// How long establishing a connection (HTTP or websocket) may take.
pub(crate) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
