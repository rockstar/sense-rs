//! The HTTP backend: reqwest with `native`, wasi-http with `wasi`.

#[cfg(feature = "native")]
mod native;
#[cfg(feature = "wasi")]
mod wasi;

#[cfg(feature = "native")]
pub(crate) use native::Http;
#[cfg(feature = "wasi")]
pub(crate) use wasi::Http;

const USER_AGENT: &str = concat!("sense-rs/", env!("CARGO_PKG_VERSION"));
