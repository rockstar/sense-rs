use std::time::Duration;

use super::HttpTransport;
use crate::client::CONNECT_TIMEOUT;
use crate::error::{Error, Result};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// [`HttpTransport`] backed by reqwest, on Tokio. Native targets only.
///
/// Also the transport that provides the realtime feed and the blocking
/// client. Cheap to clone; clones share the connection pool.
#[derive(Debug, Clone)]
pub struct NativeTransport(reqwest::Client);

impl NativeTransport {
    /// A transport with a 10 s connect timeout and a 30 s request timeout.
    /// Fails only if the TLS backend can't initialize.
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self(client))
    }
}

impl Default for NativeTransport {
    /// Like [`new`](Self::new), but panics if the TLS backend can't initialize.
    #[expect(
        clippy::expect_used,
        reason = "only fails if the TLS backend can't initialize"
    )]
    fn default() -> Self {
        Self::new().expect("default transport configuration is valid")
    }
}

/// Use a reqwest client you configured yourself, e.g. with a proxy. Its
/// timeouts apply in place of the defaults.
impl From<reqwest::Client> for NativeTransport {
    fn from(client: reqwest::Client) -> Self {
        Self(client)
    }
}

impl HttpTransport for NativeTransport {
    async fn send(&self, req: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>> {
        let req = reqwest::Request::try_from(req)?;
        let resp = self.0.execute(req).await?;
        let mut out = http::Response::new(Vec::new());
        *out.status_mut() = resp.status();
        *out.headers_mut() = resp.headers().clone();
        *out.body_mut() = resp.bytes().await.map_err(Error::from)?.to_vec();
        Ok(out)
    }
}
