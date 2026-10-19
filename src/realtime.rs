//! The realtime feed at `wss://clientrt.sense.com/monitors/{id}/realtimefeed`.
//!
//! Each binary frame holds three MessagePack values back to back: a message
//! type string, a unix timestamp, and a payload map. Commands go the other
//! way in the same shape.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::time::{Instant, Sleep};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::client::{CONNECT_TIMEOUT, PROTOCOL_VERSION};
use crate::error::{Error, Result};
use crate::models::ConnectionState;

/// How long the feed may be silent before it's treated as dead.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// A live connection to a monitor's realtime feed.
///
/// Implements [`Stream`]; use [`StreamExt::next`] to read events. The server
/// sends [`RealtimeMessage::Update`] about once a second.
///
/// If nothing arrives for half the idle timeout, the feed sends a websocket
/// ping. If nothing arrives for the whole timeout (30 s by default), the
/// connection is presumed dead: the stream yields [`Error::Timeout`] and
/// then ends. Reconnect with [`Client::realtime`](crate::Client::realtime).
///
/// If the server rejects the token mid-feed, the stream yields
/// [`Error::Api`] with status 401. Call [`Client::renew`](crate::Client::renew)
/// before reconnecting, or the new feed will be rejected too.
pub struct RealtimeFeed {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    idle_timeout: Duration,
    last_seen: Instant,
    /// Fires at the ping time, then (after a ping) at the idle deadline.
    timer: Pin<Box<Sleep>>,
    pinged: bool,
    done: bool,
}

impl RealtimeFeed {
    pub(crate) async fn connect(url: &str, access_token: &str) -> Result<Self> {
        let mut req = url.into_client_request()?;
        let headers = req.headers_mut();
        headers.insert(
            "Authorization",
            format!("bearer {access_token}").parse().map_err(|_| {
                Error::InvalidArgument("access token is not a valid header value".into())
            })?,
        );
        headers.insert("Accept", HeaderValue::from_static("application/x-msgpack"));
        headers.insert(
            "X-Sense-Protocol",
            HeaderValue::from_static(PROTOCOL_VERSION),
        );

        let connect = tokio_tungstenite::connect_async(req);
        let ws = match tokio::time::timeout(CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| Error::Timeout(CONNECT_TIMEOUT))?
        {
            Ok((ws, _)) => ws,
            Err(tungstenite::Error::Http(resp)) => {
                return Err(Error::Api {
                    status: resp.status().as_u16(),
                    reason: resp
                        .body()
                        .as_ref()
                        .map(|b| String::from_utf8_lossy(b).into_owned()),
                });
            }
            Err(e) => return Err(e.into()),
        };
        let now = Instant::now();
        let mut feed = RealtimeFeed {
            ws,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            last_seen: now,
            timer: Box::pin(tokio::time::sleep_until(now + DEFAULT_IDLE_TIMEOUT / 2)),
            pinged: false,
            done: false,
        };
        feed.start_updates().await?;
        Ok(feed)
    }

    /// Change how long the feed may be silent before it's treated as dead.
    /// A ping is sent after half of it.
    pub fn set_idle_timeout(&mut self, timeout: Duration) {
        self.idle_timeout = timeout;
        self.mark_alive();
    }

    fn mark_alive(&mut self) {
        self.last_seen = Instant::now();
        self.pinged = false;
        let ping_at = self.last_seen + self.idle_timeout / 2;
        self.timer.as_mut().reset(ping_at);
    }

    /// Ask the server to (re)start sending realtime updates.
    ///
    /// Called on connect. Call it again if updates stop arriving.
    pub async fn start_updates(&mut self) -> Result<()> {
        self.send_command("start_realtime_updates").await
    }

    async fn send_command(&mut self, command: &str) -> Result<()> {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let frame = encode_command(command, epoch);
        self.ws.send(Message::Binary(frame.into())).await?;
        Ok(())
    }

    /// Close the connection.
    pub async fn close(mut self) -> Result<()> {
        self.ws.close(None).await?;
        Ok(())
    }
}

impl Stream for RealtimeFeed {
    type Item = Result<RealtimeEvent>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let msg = match self.ws.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Some(Ok(msg))) => msg,
            };
            // Any frame, including a pong, proves the connection is alive.
            self.mark_alive();
            match msg {
                Message::Binary(bytes) => return Poll::Ready(Some(decode_frame(&bytes))),
                Message::Close(_) => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                // Pings are answered by tungstenite; nothing else is expected.
                _ => continue,
            }
        }

        // Nothing to read. Ping once when quiet, give up when silent too long.
        while self.timer.as_mut().poll(cx).is_ready() {
            if self.pinged {
                self.done = true;
                return Poll::Ready(Some(Err(Error::Timeout(self.idle_timeout))));
            }
            match self.ws.poll_ready_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Ok(())) => {}
            }
            if let Err(e) = self.ws.start_send_unpin(Message::Ping(Default::default())) {
                return Poll::Ready(Some(Err(e.into())));
            }
            // Later reads flush the ping if this can't finish now.
            if let Poll::Ready(Err(e)) = self.ws.poll_flush_unpin(cx) {
                return Poll::Ready(Some(Err(e.into())));
            }
            self.pinged = true;
            let deadline = self.last_seen + self.idle_timeout;
            self.timer.as_mut().reset(deadline);
        }
        Poll::Pending
    }
}

/// One message from the realtime feed.
#[derive(Debug, Clone)]
pub struct RealtimeEvent {
    /// Unix timestamp (seconds) the server attached to the message.
    pub epoch: i64,
    pub message: RealtimeMessage,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RealtimeMessage {
    /// `hello`: sent on connect with the monitor's connection state.
    Hello(Hello),
    /// `realtime_update`: current power readings.
    Update(Box<RealtimeUpdate>),
    /// `data_change`: something changed server side; refetch if you cache it.
    DataChange(DataChange),
    /// `monitor_info`
    MonitorInfo(MonitorInfo),
    /// Any other message type, with its payload as JSON. Known types include
    /// `device_states`, `recent_history`, `new_timeline_event`,
    /// `monitor_connection`, `solar_setup_status` and `new_relay_event`.
    Other {
        kind: String,
        payload: serde_json::Value,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Hello {
    pub online: Option<bool>,
    pub connection_state: Option<ConnectionState>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorInfo {
    pub features: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataChange {
    pub monitor_overview_checksum: Option<String>,
    pub device_data_checksum: Option<String>,
    pub settings_version: Option<i64>,
    pub user_version: Option<i64>,
    pub pending_events: Option<serde_json::Value>,
}

/// Instantaneous power readings. Watts unless noted.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RealtimeUpdate {
    /// Total consumption.
    pub w: f64,
    /// Consumption cost rate.
    pub c: Option<f64>,
    pub solar_w: Option<f64>,
    pub solar_c: Option<f64>,
    pub grid_w: Option<f64>,
    /// Line frequency.
    pub hz: Option<f64>,
    /// Per-leg power.
    #[serde(default)]
    pub channels: Vec<f64>,
    /// Per-leg voltage.
    #[serde(default)]
    pub voltage: Vec<f64>,
    /// Per-leg current.
    #[serde(default)]
    pub amps: Vec<f64>,
    /// Per-leg solar power.
    #[serde(default)]
    pub solar: Vec<f64>,
    /// Devices currently drawing power.
    #[serde(default)]
    pub devices: Vec<RealtimeDevice>,
    #[serde(default)]
    pub deltas: Vec<Delta>,
    pub d_w: Option<f64>,
    pub d_solar_w: Option<f64>,
    pub d_battery_w: Option<f64>,
    pub d_generator_w: Option<f64>,
    pub solar_pct: Option<i64>,
    pub battery_pct: Option<i64>,
    pub generator_pct: Option<i64>,
    pub backup_power: Option<String>,
    pub gen_state: Option<serde_json::Value>,
    pub gen_last_state_time: Option<String>,
    pub aux: Option<serde_json::Value>,
    pub tou_alert: Option<serde_json::Value>,
    pub power_flow: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RealtimeDevice {
    pub id: String,
    pub name: Option<String>,
    pub icon: Option<String>,
    pub w: Option<f64>,
    pub c: Option<f64>,
    /// Always-on watts.
    pub ao_w: Option<f64>,
    /// State details (dimmer intensity, battery charge, ...).
    pub sd: Option<serde_json::Value>,
    #[serde(default)]
    pub attrs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Delta {
    pub device_id: Option<String>,
    pub w: Option<f64>,
    pub channel: Option<i64>,
    pub frame: Option<i64>,
}

#[derive(Deserialize)]
struct ErrorPayload {
    authorized: Option<bool>,
    error_reason: Option<String>,
}

/// A command frame: the command name, a unix timestamp, and an empty map.
fn encode_command(command: &str, epoch: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    for v in [
        rmpv::Value::from(command),
        rmpv::Value::from(epoch),
        rmpv::Value::Map(Vec::new()),
    ] {
        // Writing to a Vec cannot fail.
        let _ = rmpv::encode::write_value(&mut buf, &v);
    }
    buf
}

fn decode_frame(bytes: &[u8]) -> Result<RealtimeEvent> {
    let mut cur = bytes;
    let mut next = || rmpv::decode::read_value(&mut cur).map_err(|e| Error::Decode(e.to_string()));
    let kind = next()?;
    let epoch = next()?;
    let payload = next().unwrap_or(rmpv::Value::Map(Vec::new()));
    let kind = kind
        .as_str()
        .ok_or_else(|| Error::Decode("message type is not a string".into()))?
        .to_string();
    // Accept a float timestamp too.
    let epoch = epoch
        .as_i64()
        .or_else(|| epoch.as_f64().map(|f| f as i64))
        .unwrap_or_default();

    let message = match kind.as_str() {
        "hello" => RealtimeMessage::Hello(from_msgpack(payload)?),
        "realtime_update" => RealtimeMessage::Update(Box::new(from_msgpack(payload)?)),
        "data_change" => RealtimeMessage::DataChange(from_msgpack(payload)?),
        "monitor_info" => RealtimeMessage::MonitorInfo(from_msgpack(payload)?),
        "error" => {
            let e: ErrorPayload = from_msgpack(payload)?;
            // An expired token may show up here rather than as a 401 on the handshake.
            if e.authorized == Some(false) {
                return Err(Error::Api {
                    status: 401,
                    reason: e.error_reason,
                });
            }
            return Err(Error::Realtime(
                e.error_reason.unwrap_or_else(|| "unknown".into()),
            ));
        }
        _ => RealtimeMessage::Other {
            payload: from_msgpack(payload)?,
            kind,
        },
    };
    Ok(RealtimeEvent { epoch, message })
}

fn from_msgpack<T: serde::de::DeserializeOwned>(v: rmpv::Value) -> Result<T> {
    // Via JSON because rmpv's deserializer can't read string-encoded enums.
    let json: serde_json::Value =
        rmpv::ext::from_value(v).map_err(|e| Error::Decode(e.to_string()))?;
    serde_json::from_value(json).map_err(|e| Error::Decode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmpv::Value;

    fn frame(kind: &str, payload: Value) -> Vec<u8> {
        let mut buf = Vec::new();
        for v in [Value::from(kind), Value::from(1_700_000_000), payload] {
            rmpv::encode::write_value(&mut buf, &v).unwrap();
        }
        buf
    }

    fn map(pairs: Vec<(&str, Value)>) -> Value {
        Value::Map(
            pairs
                .into_iter()
                .map(|(k, v)| (Value::from(k), v))
                .collect(),
        )
    }

    /// Starts a websocket server and returns its URL. `serve` gets each
    /// accepted connection after the handshake.
    async fn ws_server<F, Fut>(serve: F) -> String
    where
        F: FnOnce(WebSocketStream<TcpStream>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            serve(tokio_tungstenite::accept_async(sock).await.unwrap()).await;
        });
        url
    }

    const SHORT: Duration = Duration::from_millis(200);

    #[tokio::test]
    async fn dead_connection_times_out_then_ends() {
        // Completes the handshake, then never reads again, so pings go unanswered.
        let url = ws_server(|ws| async move {
            let _hold = ws;
            std::future::pending::<()>().await;
        })
        .await;
        let mut feed = RealtimeFeed::connect(&url, "token").await.unwrap();
        feed.set_idle_timeout(SHORT);

        let started = std::time::Instant::now();
        let err = feed.next().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::Timeout(d) if d == SHORT), "{err:?}");
        assert!(started.elapsed() >= SHORT);
        assert!(
            feed.next().await.is_none(),
            "feed should end after a timeout"
        );
    }

    #[tokio::test]
    async fn quiet_but_alive_connection_stays_open() {
        // Sends no data but keeps reading, so tungstenite answers our pings.
        let url = ws_server(|mut ws| async move { while ws.next().await.is_some() {} }).await;
        let mut feed = RealtimeFeed::connect(&url, "token").await.unwrap();
        feed.set_idle_timeout(SHORT);

        let waited = tokio::time::timeout(SHORT * 4, feed.next()).await;
        assert!(
            waited.is_err(),
            "expected no event and no timeout, got {waited:?}"
        );
    }

    #[tokio::test]
    async fn bad_token_is_an_error_not_a_panic() {
        let err = RealtimeFeed::connect("ws://127.0.0.1:1/", "token\n")
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    }

    #[tokio::test]
    async fn data_is_delivered_then_server_close_ends_feed() {
        let url = ws_server(|mut ws| async move {
            ws.send(Message::Binary(frame("hello", map(vec![])).into()))
                .await
                .unwrap();
            ws.close(None).await.unwrap();
        })
        .await;
        let mut feed = RealtimeFeed::connect(&url, "token").await.unwrap();
        let ev = feed.next().await.unwrap().unwrap();
        assert!(matches!(ev.message, RealtimeMessage::Hello(_)));
        assert!(
            feed.next().await.is_none(),
            "feed should end on server close"
        );
    }

    #[test]
    fn encodes_start_command() {
        let mut expected = vec![0xb6]; // fixstr, 22 bytes
        expected.extend_from_slice(b"start_realtime_updates");
        expected.extend_from_slice(&[0xce, 0x65, 0x53, 0xf1, 0x00]); // uint32 1_700_000_000
        expected.push(0x80); // empty fixmap
        assert_eq!(
            encode_command("start_realtime_updates", 1_700_000_000),
            expected
        );
    }

    #[test]
    fn decodes_realtime_update() {
        let payload = map(vec![
            ("w", Value::from(1234)), // integer on the wire still decodes into f64
            ("solar_w", Value::from(-50.5)),
            ("hz", Value::from(60.0)),
            (
                "voltage",
                Value::Array(vec![Value::from(120.1), Value::from(119.8)]),
            ),
            (
                "devices",
                Value::Array(vec![map(vec![
                    ("id", Value::from("abc")),
                    ("name", Value::from("Fridge")),
                    ("w", Value::from(150.0)),
                ])]),
            ),
            (
                "deltas",
                Value::Array(vec![
                    map(vec![("w", Value::Nil), ("channel", Value::from(1))]),
                    map(vec![("w", Value::from(5.0)), ("frame", Value::from(2))]),
                ]),
            ),
            ("something_new", Value::from(true)),
        ]);
        let ev = decode_frame(&frame("realtime_update", payload)).unwrap();
        assert_eq!(ev.epoch, 1_700_000_000);
        let RealtimeMessage::Update(u) = ev.message else {
            panic!("wrong variant")
        };
        assert_eq!(u.w, 1234.0);
        assert_eq!(u.solar_w, Some(-50.5));
        assert_eq!(u.voltage, vec![120.1, 119.8]);
        assert_eq!(u.devices[0].name.as_deref(), Some("Fridge"));
        assert_eq!(u.deltas[0].w, None);
        assert_eq!(u.deltas[1].channel, None);
    }

    #[test]
    fn decodes_hello() {
        let payload = map(vec![
            ("online", Value::from(true)),
            ("connection_state", Value::from("ONLINE")),
        ]);
        let ev = decode_frame(&frame("hello", payload)).unwrap();
        let RealtimeMessage::Hello(h) = ev.message else {
            panic!("wrong variant")
        };
        assert_eq!(h.online, Some(true));
        assert_eq!(h.connection_state, Some(ConnectionState::Online));
    }

    #[test]
    fn unknown_type_is_other() {
        let ev = decode_frame(&frame("recent_history", map(vec![("x", Value::from(1))]))).unwrap();
        let RealtimeMessage::Other { kind, payload } = ev.message else {
            panic!("wrong variant")
        };
        assert_eq!(kind, "recent_history");
        assert_eq!(payload["x"], 1);
    }

    #[test]
    fn error_message_is_err() {
        let payload = map(vec![("error_reason", Value::from("Overloaded"))]);
        let err = decode_frame(&frame("error", payload)).unwrap_err();
        assert!(matches!(err, Error::Realtime(r) if r == "Overloaded"));
    }

    #[test]
    fn unauthorized_error_message_is_401() {
        let payload = map(vec![
            ("authorized", Value::from(false)),
            ("error_reason", Value::from("Unauthorized")),
        ]);
        let err = decode_frame(&frame("error", payload)).unwrap_err();
        assert!(matches!(
            err,
            Error::Api { status: 401, reason: Some(r) } if r == "Unauthorized"
        ));
    }

    #[test]
    fn float_epoch_is_truncated() {
        let mut buf = Vec::new();
        for v in [Value::from("hello"), Value::from(1700000000.5), map(vec![])] {
            rmpv::encode::write_value(&mut buf, &v).unwrap();
        }
        assert_eq!(decode_frame(&buf).unwrap().epoch, 1700000000);
    }

    #[test]
    fn missing_payload_is_empty_map() {
        let mut buf = Vec::new();
        for v in [Value::from("hello"), Value::from(1700000000)] {
            rmpv::encode::write_value(&mut buf, &v).unwrap();
        }
        let ev = decode_frame(&buf).unwrap();
        assert!(matches!(ev.message, RealtimeMessage::Hello(_)));
    }
}
