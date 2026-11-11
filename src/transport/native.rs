use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Sink, Stream};
use reqwest_websocket::{HandshakeError, Message, Upgrade};

use super::{HttpTransport, WebSocketFrame, WebSocketTransport};
use crate::client::CONNECT_TIMEOUT;
use crate::error::{Error, Result};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// [`HttpTransport`] and [`WebSocketTransport`] backed by reqwest, on Tokio.
/// Native targets only.
///
/// REST calls and the realtime feed both go through the same
/// `reqwest::Client`, so they share its proxy, TLS and timeout settings.
/// Also the transport the blocking client is built on. Cheap to clone;
/// clones share the connection pool.
#[derive(Debug, Clone)]
pub struct NativeTransport(reqwest::Client);

impl NativeTransport {
    /// A transport with a 10 s connect timeout and a 30 s request timeout,
    /// speaking HTTP/1.1 only (see [`From<reqwest::Client>`](#impl-From<Client>-for-NativeTransport)).
    /// Fails only if the TLS backend can't initialize.
    pub fn new() -> Result<Self> {
        let client = reqwest::Client::builder()
            .http1_only()
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
/// timeouts apply in place of the defaults, to the realtime feed's handshake
/// as well as REST calls. The request timeout doesn't cut off an open feed.
///
/// Build it with [`http1_only`](reqwest::ClientBuilder::http1_only) if you
/// use the realtime feed. Websockets need HTTP/1.1, and Sense's servers
/// offer HTTP/2, which reqwest picks when it can. Without it, the feed fails
/// to connect with [`Error::WebSocket`].
impl From<reqwest::Client> for NativeTransport {
    fn from(client: reqwest::Client) -> Self {
        Self(client)
    }
}

impl HttpTransport for NativeTransport {
    async fn send(&self, req: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>> {
        let req = reqwest::Request::try_from(req)?;
        read_response(self.0.execute(req).await?).await
    }
}

async fn read_response(resp: reqwest::Response) -> Result<http::Response<Vec<u8>>> {
    let mut out = http::Response::new(Vec::new());
    *out.status_mut() = resp.status();
    *out.headers_mut() = resp.headers().clone();
    *out.body_mut() = resp.bytes().await.map_err(Error::from)?.to_vec();
    Ok(out)
}

impl WebSocketTransport for NativeTransport {
    type Socket = NativeWebSocket;

    async fn connect(
        &self,
        req: http::Request<()>,
    ) -> Result<Result<NativeWebSocket, http::Response<Vec<u8>>>> {
        let (parts, ()) = req.into_parts();
        let resp = self
            .0
            .get(parts.uri.to_string())
            .headers(parts.headers)
            .upgrade()
            .send()
            .await
            .map_err(ws_error)?;
        if resp.status() != http::StatusCode::SWITCHING_PROTOCOLS {
            return Ok(Err(read_response(resp.into_inner()).await?));
        }
        let ws = resp.into_websocket().await.map_err(ws_error)?;
        Ok(Ok(NativeWebSocket(ws)))
    }
}

fn ws_error(e: reqwest_websocket::Error) -> Error {
    match e {
        reqwest_websocket::Error::Handshake(
            HandshakeError::ServerRespondedWithDifferentVersion,
        ) => Error::WebSocket(
            "the server answered the websocket handshake over HTTP/2; \
                 build the reqwest client with http1_only()"
                .into(),
        ),
        reqwest_websocket::Error::Reqwest(e) => e.into(),
        e => Error::WebSocket(Box::new(e)),
    }
}

/// The open websocket of a [`NativeTransport`].
#[derive(Debug)]
pub struct NativeWebSocket(reqwest_websocket::WebSocket);

impl Stream for NativeWebSocket {
    type Item = Result<WebSocketFrame>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let frame = match std::task::ready!(Pin::new(&mut self.0).poll_next(cx)) {
                None => return Poll::Ready(None),
                Some(Err(e)) => return Poll::Ready(Some(Err(ws_error(e)))),
                Some(Ok(Message::Binary(b))) => WebSocketFrame::Binary(b.into()),
                Some(Ok(Message::Ping(_))) => WebSocketFrame::Ping,
                Some(Ok(Message::Pong(_))) => WebSocketFrame::Pong,
                Some(Ok(Message::Close { .. })) => WebSocketFrame::Close,
                Some(Ok(Message::Text(_))) => continue,
            };
            return Poll::Ready(Some(Ok(frame)));
        }
    }
}

impl Sink<WebSocketFrame> for NativeWebSocket {
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.0).poll_ready(cx).map_err(ws_error)
    }

    fn start_send(mut self: Pin<&mut Self>, frame: WebSocketFrame) -> Result<()> {
        let msg = match frame {
            WebSocketFrame::Binary(b) => Message::Binary(b.into()),
            WebSocketFrame::Ping => Message::Ping(Default::default()),
            WebSocketFrame::Pong => Message::Pong(Default::default()),
            WebSocketFrame::Close => Message::Close {
                code: Default::default(),
                reason: String::new(),
            },
        };
        Pin::new(&mut self.0).start_send(msg).map_err(ws_error)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx).map_err(ws_error)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut self.0).poll_close(cx).map_err(ws_error)
    }
}
