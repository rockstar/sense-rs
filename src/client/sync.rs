//! Blocking API.
//!
//! [`Client`] wraps the async [`crate::Client`] and runs each call on a
//! private Tokio runtime, the same way `reqwest::blocking` does. Don't use
//! it from inside an async runtime; use the async client there.
//!
//! ```no_run
//! use sense::sync::Client;
//!
//! let client = Client::new()?;
//! client.login("me@example.com", "hunter2")?;
//! let monitor = client.default_monitor_id().unwrap();
//! for event in client.realtime(monitor)? {
//!     if let sense::realtime::RealtimeMessage::Update(u) = event?.message {
//!         println!("{} W", u.w);
//!     }
//! }
//! # Ok::<(), sense::Error>(())
//! ```

use std::sync::Arc;

use futures_util::StreamExt;
use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::models::*;
use crate::realtime::RealtimeEvent;

/// Blocking client for the Sense API. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    inner: crate::Client,
    rt: Arc<Runtime>,
}

impl Client {
    /// A client with default settings and no session.
    pub fn new() -> Result<Self> {
        Self::from_async(crate::Client::new())
    }

    /// Wrap an async client, e.g. one built with [`crate::Client::builder`].
    pub fn from_async(inner: crate::Client) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| Error::Decode(format!("failed to start runtime: {e}")))?;
        Ok(Self {
            inner,
            rt: Arc::new(rt),
        })
    }

    /// The underlying async client. It shares this client's session.
    pub fn as_async(&self) -> &crate::Client {
        &self.inner
    }

    fn block<F: Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    pub fn session(&self) -> Option<Session> {
        self.inner.session()
    }

    pub fn set_session(&self, session: Option<Session>) {
        self.inner.set_session(session)
    }

    pub fn default_monitor_id(&self) -> Option<i64> {
        self.inner.default_monitor_id()
    }

    /// See [`crate::Client::login`].
    pub fn login(&self, email: &str, password: &str) -> Result<AuthResponse> {
        self.block(self.inner.login(email, password))
    }

    pub fn login_mfa(&self, mfa_token: &str, totp: &str) -> Result<AuthResponse> {
        self.block(self.inner.login_mfa(mfa_token, totp))
    }

    pub fn renew(&self) -> Result<()> {
        self.block(self.inner.renew())
    }

    pub fn logout(&self) -> Result<()> {
        self.block(self.inner.logout())
    }

    pub fn monitor_overview(&self, monitor_id: i64) -> Result<MonitorOverviewResponse> {
        self.block(self.inner.monitor_overview(monitor_id))
    }

    pub fn monitor_status(&self, monitor_id: i64) -> Result<MonitorStatus> {
        self.block(self.inner.monitor_status(monitor_id))
    }

    pub fn devices(&self, monitor_id: i64) -> Result<DevicesOverview> {
        self.block(self.inner.devices(monitor_id))
    }

    pub fn device(&self, monitor_id: i64, device_id: &str) -> Result<serde_json::Value> {
        self.block(self.inner.device(monitor_id, device_id))
    }

    pub fn trends(&self, monitor_id: i64, scale: Scale, start: Option<&str>) -> Result<Trends> {
        self.block(self.inner.trends(monitor_id, scale, start))
    }

    pub fn usage(
        &self,
        monitor_id: i64,
        granularity: Granularity,
        start: &str,
        frames: u32,
    ) -> Result<Usage> {
        self.block(self.inner.usage(monitor_id, granularity, start, frames))
    }

    pub fn cumulative(&self, monitor_id: i64) -> Result<CumulativeUsage> {
        self.block(self.inner.cumulative(monitor_id))
    }

    pub fn get_json(&self, path: &str, query: &[(&str, String)]) -> Result<serde_json::Value> {
        self.block(self.inner.get_json(path, query))
    }

    /// Open the realtime feed. Iterate it to receive events; each call to
    /// `next` blocks until one arrives.
    pub fn realtime(&self, monitor_id: i64) -> Result<RealtimeFeed> {
        let inner = self.block(self.inner.realtime(monitor_id))?;
        Ok(RealtimeFeed {
            inner,
            rt: self.rt.clone(),
        })
    }
}

/// Blocking realtime feed. An [`Iterator`] of events.
pub struct RealtimeFeed {
    inner: crate::realtime::RealtimeFeed,
    rt: Arc<Runtime>,
}

impl RealtimeFeed {
    /// See [`crate::realtime::RealtimeFeed::start_updates`].
    pub fn start_updates(&mut self) -> Result<()> {
        self.rt.block_on(self.inner.start_updates())
    }

    pub fn close(self) -> Result<()> {
        self.rt.block_on(self.inner.close())
    }
}

impl Iterator for RealtimeFeed {
    type Item = Result<RealtimeEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rt.block_on(self.inner.next())
    }
}
