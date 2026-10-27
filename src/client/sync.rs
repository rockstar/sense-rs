//! Blocking API.
//!
//! [`Client`] wraps the async [`crate::Client`] and runs each call on a
//! private Tokio runtime, the same way `reqwest::blocking` does. Don't use
//! it from inside an async runtime; use the async client there.
//!
//! ```no_run
//! use sense::sync::Client;
//! use sense::Error;
//!
//! let client = Client::new()?;
//! client.login("me@example.com", "hunter2")?;
//! let monitor = client.default_monitor_id().unwrap();
//! for event in client.realtime(monitor)? {
//!     let event = match event {
//!         Ok(event) => event,
//!         // Bad messages and server `error`s don't end the feed.
//!         Err(e @ (Error::Decode(_) | Error::Realtime(_))) => { eprintln!("{e}"); continue; }
//!         Err(e) => return Err(e),
//!     };
//!     if let sense::realtime::RealtimeMessage::Update(u) = event.message {
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
        Self::from_async(crate::Client::builder().build()?)
    }

    /// Wrap an async client, e.g. one built with [`crate::Client::builder`].
    pub fn from_async(inner: crate::Client) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(Error::Runtime)?;
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

    /// See [`crate::Client::session`].
    pub fn session(&self) -> Option<Session> {
        self.inner.session()
    }

    /// See [`crate::Client::set_session`].
    pub fn set_session(&self, session: Option<Session>) {
        self.inner.set_session(session)
    }

    /// See [`crate::Client::default_monitor_id`].
    pub fn default_monitor_id(&self) -> Option<i64> {
        self.inner.default_monitor_id()
    }

    /// See [`crate::Client::login`].
    pub fn login(&self, email: &str, password: &str) -> Result<AuthResponse> {
        self.block(self.inner.login(email, password))
    }

    /// See [`crate::Client::login_mfa`].
    pub fn login_mfa(&self, mfa_token: &str, totp: &str) -> Result<AuthResponse> {
        self.block(self.inner.login_mfa(mfa_token, totp))
    }

    /// See [`crate::Client::renew`].
    pub fn renew(&self) -> Result<()> {
        self.block(self.inner.renew())
    }

    /// See [`crate::Client::logout`].
    pub fn logout(&self) -> Result<()> {
        self.block(self.inner.logout())
    }

    /// See [`crate::Client::monitor_overview`].
    pub fn monitor_overview(&self, monitor_id: i64) -> Result<MonitorOverviewResponse> {
        self.block(self.inner.monitor_overview(monitor_id))
    }

    /// See [`crate::Client::monitor_status`].
    pub fn monitor_status(&self, monitor_id: i64) -> Result<MonitorStatus> {
        self.block(self.inner.monitor_status(monitor_id))
    }

    /// See [`crate::Client::devices`].
    pub fn devices(&self, monitor_id: i64) -> Result<DevicesOverview> {
        self.block(self.inner.devices(monitor_id))
    }

    /// See [`crate::Client::device`].
    pub fn device(&self, monitor_id: i64, device_id: &str) -> Result<serde_json::Value> {
        self.block(self.inner.device(monitor_id, device_id))
    }

    /// See [`crate::Client::trends`].
    pub fn trends(&self, monitor_id: i64, scale: Scale, start: Option<&str>) -> Result<Trends> {
        self.block(self.inner.trends(monitor_id, scale, start))
    }

    /// See [`crate::Client::usage`].
    pub fn usage(
        &self,
        monitor_id: i64,
        granularity: Granularity,
        start: &str,
        frames: u32,
    ) -> Result<Usage> {
        self.block(self.inner.usage(monitor_id, granularity, start, frames))
    }

    /// See [`crate::Client::cumulative`].
    pub fn cumulative(&self, monitor_id: i64) -> Result<CumulativeUsage> {
        self.block(self.inner.cumulative(monitor_id))
    }

    /// See [`crate::Client::get_json`].
    pub fn get_json(&self, path: &str, query: &[(&str, String)]) -> Result<serde_json::Value> {
        self.block(self.inner.get_json(path, query))
    }

    /// Open the realtime feed. Iterate it to receive events; each call to
    /// `next` blocks until one arrives.
    ///
    /// If the feed yields [`Error::Api`] with status 401, call
    /// [`renew`](Self::renew) before reconnecting.
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
    /// See [`crate::realtime::RealtimeFeed::set_idle_timeout`].
    pub fn set_idle_timeout(&mut self, timeout: std::time::Duration) {
        self.inner.set_idle_timeout(timeout)
    }

    /// See [`crate::realtime::RealtimeFeed::start_updates`].
    pub fn start_updates(&mut self) -> Result<()> {
        self.rt.block_on(self.inner.start_updates())
    }

    /// See [`crate::realtime::RealtimeFeed::close`].
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::realtime::RealtimeMessage;
    use rmpv::Value;
    use tokio_tungstenite::tungstenite::{self, Message};

    /// A blocking client whose realtime URL points at a websocket server
    /// running `serve` on its own thread.
    fn client_with_ws_server<F>(serve: F) -> (Client, std::thread::JoinHandle<()>)
    where
        F: FnOnce(tungstenite::WebSocket<std::net::TcpStream>) + Send + 'static,
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            serve(tungstenite::accept(sock).unwrap());
        });
        let inner = crate::Client::builder()
            .realtime_url(url)
            .session(Session {
                access_token: "access".into(),
                refresh_token: "refresh".into(),
                user_id: 7,
                monitor_ids: vec![1],
            })
            .build()
            .unwrap();
        (Client::from_async(inner).unwrap(), server)
    }

    fn command_kind(msg: Message) -> String {
        let Message::Binary(bytes) = msg else {
            panic!("expected a binary frame, got {msg:?}")
        };
        let kind = rmpv::decode::read_value(&mut &bytes[..]).unwrap();
        kind.as_str().unwrap().to_string()
    }

    #[test]
    fn feed_yields_events_then_ends() {
        let (client, server) = client_with_ws_server(|mut ws| {
            let mut hello = Vec::new();
            for v in [
                Value::from("hello"),
                Value::from(1_700_000_000),
                Value::Map(Vec::new()),
            ] {
                rmpv::encode::write_value(&mut hello, &v).unwrap();
            }
            ws.send(Message::Binary(hello.into())).unwrap();
            ws.close(None).unwrap();
            // Drain until the close handshake finishes.
            while ws.read().is_ok() {}
        });

        let events: Vec<_> = client.realtime(1).unwrap().collect();
        server.join().unwrap();

        assert_eq!(events.len(), 1, "{events:?}");
        let ev = events.into_iter().next().unwrap().unwrap();
        assert_eq!(ev.epoch, 1_700_000_000);
        assert!(matches!(ev.message, RealtimeMessage::Hello(_)));
    }

    #[test]
    fn start_updates_and_close_reach_the_server() {
        let (tx, rx) = std::sync::mpsc::channel();
        let (client, server) = client_with_ws_server(move |mut ws| {
            for _ in 0..3 {
                tx.send(ws.read().unwrap()).unwrap();
            }
        });

        let mut feed = client.realtime(1).unwrap();
        feed.start_updates().unwrap();
        feed.close().unwrap();
        server.join().unwrap();

        // `connect` sends the first start command; `start_updates` the second.
        assert_eq!(command_kind(rx.recv().unwrap()), "start_realtime_updates");
        assert_eq!(command_kind(rx.recv().unwrap()), "start_realtime_updates");
        let msg = rx.recv().unwrap();
        assert!(matches!(msg, Message::Close(_)), "{msg:?}");
    }
}
