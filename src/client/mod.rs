//! The async [`Client`], re-exported at the crate root, and (on native
//! targets) the blocking `sync::Client`.

mod r#async;
mod request;
#[cfg(not(target_family = "wasm"))]
pub mod sync;

#[cfg(not(target_family = "wasm"))]
pub(crate) use r#async::api_error;
pub use r#async::{Client, ClientBuilder};

/// Sent as the `X-Sense-Protocol` header.
pub(crate) const PROTOCOL_VERSION: &str = "12";

#[cfg(not(target_family = "wasm"))]
/// How long establishing a connection (HTTP or websocket) may take.
pub(crate) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
