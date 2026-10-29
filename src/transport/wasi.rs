use wstd::http::Client;

use super::HttpTransport;
use crate::error::{Error, Result};

/// [`HttpTransport`] backed by [wasi-http](https://github.com/WebAssembly/wasi-http),
/// using `wstd`. `wasm32-wasip2` only.
///
/// No timeouts are set: hosts that don't support wasi-http's timeout
/// options fail every request that sets one. The host's own limits apply.
#[derive(Debug, Clone, Default)]
pub struct WasiTransport(Client);

impl WasiTransport {
    /// A transport with no timeouts of its own.
    pub fn new() -> Self {
        Self(Client::new())
    }
}

impl HttpTransport for WasiTransport {
    async fn send(&self, req: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>> {
        let mut resp = self.0.send(req).await.map_err(|e| Error::Http(e.into()))?;
        let body = resp
            .body_mut()
            .contents()
            .await
            .map_err(|e| Error::Http(e.into()))?
            .to_vec();
        Ok(resp.map(|_| body))
    }
}
