//! How a [`Client`](crate::Client) sends HTTP requests and opens websockets.

use std::future::Future;

use crate::error::Result;

#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(all(target_os = "wasi", target_env = "p2"))]
mod wasi;

#[cfg(not(target_family = "wasm"))]
pub use native::{NativeTransport, NativeWebSocket};
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

/// One websocket frame to or from the realtime feed.
#[cfg(not(target_family = "wasm"))]
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WebSocketFrame {
    /// A binary frame. The feed's data and commands are all binary.
    Binary(Vec<u8>),
    /// A ping. Sent by the feed to check the connection is alive.
    Ping,
    /// A pong. Received in answer to a ping.
    Pong,
    /// The other side closed the connection.
    Close,
}

/// Opens websockets for the realtime feed of a [`Client`](crate::Client).
///
/// The client builds the handshake request: the `ws://` or `wss://` URL and
/// the feed's own headers (including `Authorization`). The implementation
/// owns the handshake itself, over whichever HTTP version it supports, and
/// sends it through whatever proxy, TLS and timeout settings it has. If the
/// server refuses the websocket, return its response as `Ok(Err(response))`,
/// the same way [`HttpTransport`] treats non-success statuses. Return
/// [`Error::WebSocket`](crate::Error::WebSocket) when no response arrives.
///
/// The socket must answer the server's pings itself and may drop text
/// frames. The feed only sends [`WebSocketFrame::Binary`] and
/// [`WebSocketFrame::Ping`], and closes the socket with
/// [`SinkExt::close`](futures_util::SinkExt::close).
///
/// Built in: [`NativeTransport`]. Implement it yourself for anything else.
#[cfg(not(target_family = "wasm"))]
pub trait WebSocketTransport {
    /// An open websocket.
    type Socket: futures_util::Stream<Item = Result<WebSocketFrame>>
        + futures_util::Sink<WebSocketFrame, Error = crate::Error>
        + Unpin;

    /// Send the handshake `req` and return the open socket, or the server's
    /// response with its whole body if it refused the websocket.
    fn connect(
        &self,
        req: http::Request<()>,
    ) -> impl Future<Output = Result<Result<Self::Socket, http::Response<Vec<u8>>>>>;
}
