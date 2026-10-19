//! Unofficial client for the [Sense](https://sense.com) energy monitor API.
//!
//! The API is undocumented and may change without notice. The default API is
//! async. A blocking API lives in [`sync`].
//!
//! ```no_run
//! use futures_util::StreamExt;
//! use sense::models::Scale;
//! use sense::realtime::RealtimeMessage;
//! use sense::{Client, Error};
//!
//! # async fn run() -> sense::Result<()> {
//! let client = Client::new();
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
//!     if let RealtimeMessage::Update(u) = event?.message {
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
//! Response types are in [`models`]; live-feed types are in [`realtime`].

#![warn(unnameable_types, unreachable_pub)]

mod client;
mod error;
pub mod models;
pub mod realtime;

pub use client::{Client, ClientBuilder, sync};
pub use error::{Error, Result};
