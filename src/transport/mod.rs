//! How a [`Client`](crate::Client) sends HTTP requests.

use std::future::Future;

use crate::error::Result;

#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(all(target_os = "wasi", target_env = "p2"))]
mod wasi;

#[cfg(not(target_family = "wasm"))]
pub use native::NativeTransport;
#[cfg(all(target_os = "wasi", target_env = "p2"))]
pub use wasi::WasiTransport;

/// Sends HTTP requests for a [`Client`](crate::Client).
///
/// The client builds each request in full: URL and query, headers
/// (including `Authorization`), and the encoded body. An implementation only
/// sends it and reads the whole response. Non-success statuses are
/// responses, not errors. Return [`Error::Http`](crate::Error::Http) when no
/// response arrives.
///
/// Built in: [`NativeTransport`] on native targets and `WasiTransport` on
/// `wasm32-wasip2`. Implement it yourself for anything else.
pub trait HttpTransport {
    /// Send `req` and return the response with its whole body.
    fn send(
        &self,
        req: http::Request<Vec<u8>>,
    ) -> impl Future<Output = Result<http::Response<Vec<u8>>>>;
}
