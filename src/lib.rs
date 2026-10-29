//! Unofficial client for the [Sense](https://sense.com) energy monitor API.
//!
//! The API is undocumented and may change without notice. The default API is
//! async. A blocking API lives in [`sync`].
//!
//! ```no_run
//! use futures_util::StreamExt;
//! use sense::models::Scale;
//! use sense::realtime::RealtimeMessage;
//! use sense::{Client, Error, NativeTransport};
//!
//! # async fn run() -> sense::Result<()> {
//! let client = Client::new(NativeTransport::new()?);
//! match client.login("me@example.com", "hunter2").await {
//!     Ok(_) => {}
//!     Err(Error::MfaRequired { mfa_token }) => {
//!         client.login_mfa(&mfa_token, "123456").await?;
//!     }
//!     Err(e) => return Err(e),
//! }
//!
//! let monitor = client.default_monitor_id().expect("account has a monitor");
//! let trends = client.trends(monitor, Scale::Day, None).await?;
//! println!("today: {:.1} kWh", trends.consumption.map_or(0.0, |c| c.total));
//!
//! let mut feed = client.realtime(monitor).await?;
//! while let Some(event) = feed.next().await {
//!     let event = match event {
//!         Ok(event) => event,
//!         // Bad messages and server `error`s don't end the feed.
//!         Err(e @ (Error::Decode(_) | Error::Realtime(_))) => { eprintln!("{e}"); continue; }
//!         Err(e) => return Err(e),
//!     };
//!     if let RealtimeMessage::Update(u) = event.message {
//!         println!("{} W", u.w);
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! Save [`Client::session`] to avoid logging in every time, and restore it
//! with [`ClientBuilder::session`] or [`Client::set_session`]. Expired access
//! tokens are renewed automatically. A renewal replaces both the access and
//! refresh tokens, so save the session again before exiting.
//!
//! Response types are in [`models`]; live-feed types are in `realtime`.
//!
//! # Platforms
//!
//! A [`Client`] sends requests through an [`HttpTransport`]. Which
//! transports are built in depends on the target, so there are no cargo
//! features to pick:
//!
//! - Native targets: [`NativeTransport`] (reqwest on Tokio). Only
//!   `Client<NativeTransport>` has the realtime feed (`realtime`,
//!   [`Client::realtime`]), and the blocking client (`sync`) is built on it.
//! - `wasm32-wasip2`: `WasiTransport`, via
//!   [wasi-http](https://github.com/WebAssembly/wasi-http) using `wstd`, for
//!   wasm components. REST endpoints only: no realtime feed (wasi has no TLS
//!   sockets for `wss://` yet) and no blocking client. Request timeouts are
//!   left to the host.
//! - Anything else: implement [`HttpTransport`] yourself.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unnameable_types, unreachable_pub)]

mod client;
mod error;
pub mod models;
#[cfg(not(target_family = "wasm"))]
pub mod realtime;
mod transport;

#[cfg(not(target_family = "wasm"))]
pub use client::sync;
pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use transport::HttpTransport;
#[cfg(not(target_family = "wasm"))]
pub use transport::NativeTransport;
#[cfg(all(target_os = "wasi", target_env = "p2"))]
pub use transport::WasiTransport;
