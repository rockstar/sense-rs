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

use crate::client::{CONNECT_TIMEOUT, PROTOCOL_VERSION, api_error};
use crate::error::{Error, Result};
use crate::models::{ConnectionState, null_as_default};

/// How long the feed may be silent before it's treated as dead.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// A live connection to a monitor's realtime feed.
///
/// Implements [`Stream`]; use [`StreamExt::next`] to read events. The server
/// sends [`RealtimeMessage::Update`] about once a second.
///
/// If nothing arrives for half the idle timeout, the feed sends a websocket
/// ping. If nothing answers within the other half (30 s timeout by default),
/// the connection is presumed dead: the stream yields [`Error::Timeout`] and
/// then ends. Reconnect with [`Client::realtime`](crate::Client::realtime).
///
/// If the server rejects the token mid-feed, the stream yields
/// [`Error::Api`] with status 401. Call [`Client::renew`](crate::Client::renew)
/// before reconnecting, or the new feed will be rejected too.
pub struct RealtimeFeed {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    idle_timeout: Duration,
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
                return Err(api_error(
                    resp.status(),
                    resp.body().as_deref().unwrap_or_default(),
                ));
            }
            Err(e) => return Err(e.into()),
        };
        let now = Instant::now();
        let mut feed = RealtimeFeed {
            ws,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            timer: Box::pin(tokio::time::sleep_until(now + DEFAULT_IDLE_TIMEOUT / 2)),
            pinged: false,
            done: false,
        };
        feed.start_updates().await?;
        Ok(feed)
    }

    /// Change how long the feed may be silent before it's treated as dead.
    /// A ping is sent after half of it. Very long values (up to
    /// [`Duration::MAX`]) are capped at about two years.
    pub fn set_idle_timeout(&mut self, timeout: Duration) {
        self.idle_timeout = timeout;
        self.mark_alive();
    }

    fn mark_alive(&mut self) {
        self.pinged = false;
        let ping_at = Instant::now() + self.half_timeout();
        self.timer.as_mut().reset(ping_at);
    }

    /// Half the idle timeout, capped so adding it to `Instant::now()` can't
    /// overflow.
    fn half_timeout(&self) -> Duration {
        (self.idle_timeout / 2).min(Duration::from_secs(86_400 * 365))
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
        // Finish sending a ping that couldn't flush last time; read() won't.
        // Errors here also surface from the read below.
        let _ = self.ws.poll_flush_unpin(cx);
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
            if let Poll::Ready(Err(e)) = self.ws.poll_flush_unpin(cx) {
                return Poll::Ready(Some(Err(e.into())));
            }
            self.pinged = true;
            // Count from the ping, not the last frame: a late read must still
            // give the pong time to arrive.
            let deadline = Instant::now() + self.half_timeout();
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
    /// The decoded message.
    pub message: RealtimeMessage,
}

/// The body of a [`RealtimeEvent`].
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
        /// The message type, e.g. `device_states`.
        kind: String,
        /// The message body, as JSON.
        payload: serde_json::Value,
    },
}

/// Payload of [`RealtimeMessage::Hello`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Hello {
    /// Whether the monitor is online.
    pub online: Option<bool>,
    /// The monitor's connection state.
    pub connection_state: Option<ConnectionState>,
}

/// Payload of [`RealtimeMessage::MonitorInfo`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitorInfo {
    /// Enabled features. Meaning not confirmed; the API is undocumented.
    pub features: Option<String>,
}

/// Payload of [`RealtimeMessage::DataChange`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataChange {
    /// New checksum of the monitor overview. Refetch it if this differs from yours.
    pub monitor_overview_checksum: Option<String>,
    /// New checksum of the device list. Refetch it if this differs from yours.
    pub device_data_checksum: Option<String>,
    /// Increments when monitor settings change.
    pub settings_version: Option<i64>,
    /// Increments when user settings change.
    pub user_version: Option<i64>,
    /// Events waiting to be read, as raw JSON.
    pub pending_events: Option<serde_json::Value>,
}

/// Instantaneous power readings. Watts unless noted.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RealtimeUpdate {
    /// Total consumption. `0.0` if the server omits it or sends null.
    #[serde(default, deserialize_with = "null_as_default")]
    pub w: f64,
    /// Consumption cost rate.
    pub c: Option<f64>,
    /// Solar production.
    pub solar_w: Option<f64>,
    /// Solar production value rate.
    pub solar_c: Option<f64>,
    /// Net power from the grid. Negative when exporting.
    pub grid_w: Option<f64>,
    /// Line frequency.
    pub hz: Option<f64>,
    /// Per-leg power.
    #[serde(default, deserialize_with = "null_as_default")]
    pub channels: Vec<f64>,
    /// Per-leg voltage.
    #[serde(default, deserialize_with = "null_as_default")]
    pub voltage: Vec<f64>,
    /// Per-leg current.
    #[serde(default, deserialize_with = "null_as_default")]
    pub amps: Vec<f64>,
    /// Per-leg solar power.
    #[serde(default, deserialize_with = "null_as_default")]
    pub solar: Vec<f64>,
    /// Devices currently drawing power.
    #[serde(default, deserialize_with = "null_as_default")]
    pub devices: Vec<RealtimeDevice>,
    /// Recent changes in power draw.
    #[serde(default, deserialize_with = "null_as_default")]
    pub deltas: Vec<Delta>,
    /// Smoothed total consumption. Meaning not confirmed; the API is undocumented.
    pub d_w: Option<f64>,
    /// Smoothed solar production. Meaning not confirmed; the API is undocumented.
    pub d_solar_w: Option<f64>,
    /// Smoothed battery power. Meaning not confirmed; the API is undocumented.
    pub d_battery_w: Option<f64>,
    /// Smoothed generator power. Meaning not confirmed; the API is undocumented.
    pub d_generator_w: Option<f64>,
    /// Share of consumption covered by solar, as a percentage. Meaning not confirmed; the API is undocumented.
    pub solar_pct: Option<i64>,
    /// Share of consumption covered by battery, as a percentage. Meaning not confirmed; the API is undocumented.
    pub battery_pct: Option<i64>,
    /// Share of consumption covered by generator, as a percentage. Meaning not confirmed; the API is undocumented.
    pub generator_pct: Option<i64>,
    /// Backup power status. Meaning not confirmed; the API is undocumented.
    pub backup_power: Option<String>,
    /// Generator state, as raw JSON.
    pub gen_state: Option<serde_json::Value>,
    /// When the generator state last changed.
    pub gen_last_state_time: Option<String>,
    /// Auxiliary port readings, as raw JSON.
    pub aux: Option<serde_json::Value>,
    /// Time-of-use rate alert, as raw JSON.
    pub tou_alert: Option<serde_json::Value>,
    /// Power flow between grid, solar, battery and home, as raw JSON.
    pub power_flow: Option<serde_json::Value>,
}

/// A device in [`RealtimeUpdate::devices`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RealtimeDevice {
    /// Device ID.
    pub id: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Icon name.
    pub icon: Option<String>,
    /// Current power draw.
    pub w: Option<f64>,
    /// Current cost rate.
    pub c: Option<f64>,
    /// Always-on watts.
    pub ao_w: Option<f64>,
    /// State details (dimmer intensity, battery charge, ...).
    pub sd: Option<serde_json::Value>,
    /// Device attributes. Meaning not confirmed; the API is undocumented.
    #[serde(default, deserialize_with = "null_as_default")]
    pub attrs: Vec<String>,
}

/// A change in power draw, in [`RealtimeUpdate::deltas`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Delta {
    /// Device that changed, if known.
    pub device_id: Option<String>,
    /// Size of the change.
    pub w: Option<f64>,
    /// Leg the change happened on.
    pub channel: Option<i64>,
    /// Frame the change happened in. Meaning not confirmed; the API is undocumented.
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
    let next =
        |cur: &mut &[u8]| rmpv::decode::read_value(cur).map_err(|e| Error::Decode(e.to_string()));
    let kind = next(&mut cur)?;
    let epoch = next(&mut cur)?;
    // Only a missing payload means empty; a truncated one is an error.
    let payload = if cur.is_empty() {
        rmpv::Value::Map(Vec::new())
    } else {
        next(&mut cur)?
    };
    let kind = kind
        .as_str()
        .ok_or_else(|| Error::Decode("message type is not a string".into()))?
        .to_string();
    // Accept a float timestamp too.
    let epoch = epoch
        .as_i64()
        .or_else(|| epoch.as_f64().map(|f| f as i64))
        .ok_or_else(|| Error::Decode("timestamp is not a number".into()))?;

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
    async fn quiet_connection_read_late_stays_open() {
        let url = ws_server(|mut ws| async move { while ws.next().await.is_some() {} }).await;
        let mut feed = RealtimeFeed::connect(&url, "token").await.unwrap();
        feed.set_idle_timeout(SHORT);

        // Nobody polls the feed past the whole idle timeout.
        tokio::time::sleep(SHORT * 2).await;
        let waited = tokio::time::timeout(SHORT * 4, feed.next()).await;
        assert!(
            waited.is_err(),
            "expected no event and no timeout, got {waited:?}"
        );
    }

    #[tokio::test]
    async fn max_idle_timeout_does_not_panic() {
        let url = ws_server(|mut ws| async move { while ws.next().await.is_some() {} }).await;
        let mut feed = RealtimeFeed::connect(&url, "token").await.unwrap();
        feed.set_idle_timeout(Duration::MAX);

        let waited = tokio::time::timeout(SHORT, feed.next()).await;
        assert!(waited.is_err(), "expected no event, got {waited:?}");
    }

    #[tokio::test]
    async fn connect_sends_start_command() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let url = ws_server(|mut ws| async move {
            let _ = tx.send(ws.next().await.unwrap().unwrap());
        })
        .await;
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let _feed = RealtimeFeed::connect(&url, "token").await.unwrap();

        let Message::Binary(bytes) = rx.await.unwrap() else {
            panic!("expected a binary frame")
        };
        let mut cur = &bytes[..];
        let kind = rmpv::decode::read_value(&mut cur).unwrap();
        let epoch = rmpv::decode::read_value(&mut cur).unwrap();
        let payload = rmpv::decode::read_value(&mut cur).unwrap();
        assert_eq!(kind.as_str(), Some("start_realtime_updates"));
        assert!(epoch.as_u64().unwrap() >= before, "{epoch:?}");
        assert_eq!(payload, Value::Map(Vec::new()));
        assert!(cur.is_empty(), "trailing bytes: {cur:?}");
    }

    #[tokio::test]
    async fn close_sends_close_frame() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let url = ws_server(|mut ws| async move {
            // Skip the start command.
            ws.next().await.unwrap().unwrap();
            let _ = tx.send(ws.next().await.unwrap().unwrap());
        })
        .await;
        let feed = RealtimeFeed::connect(&url, "token").await.unwrap();
        feed.close().await.unwrap();

        let msg = rx.await.unwrap();
        assert!(matches!(msg, Message::Close(_)), "{msg:?}");
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
    fn decodes_realtime_update_with_nil_lists() {
        let payload = map(vec![("w", Value::from(1.0)), ("deltas", Value::Nil)]);
        let ev = decode_frame(&frame("realtime_update", payload)).unwrap();
        let RealtimeMessage::Update(u) = ev.message else {
            panic!("wrong variant")
        };
        assert!(u.deltas.is_empty());
    }

    #[test]
    fn decodes_realtime_update_with_nil_w_and_device_without_id() {
        let payload = map(vec![
            ("w", Value::Nil),
            (
                "devices",
                Value::Array(vec![map(vec![("name", Value::from("Fridge"))])]),
            ),
        ]);
        let ev = decode_frame(&frame("realtime_update", payload)).unwrap();
        let RealtimeMessage::Update(u) = ev.message else {
            panic!("wrong variant")
        };
        assert_eq!(u.w, 0.0);
        assert_eq!(u.devices[0].id, None);
        assert_eq!(u.devices[0].name.as_deref(), Some("Fridge"));
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

    #[test]
    fn truncated_payload_is_decode_error() {
        let mut buf = frame("hello", map(vec![("online", Value::from(true))]));
        buf.pop();
        assert!(matches!(decode_frame(&buf), Err(Error::Decode(_))));
    }

    #[test]
    fn non_numeric_epoch_is_decode_error() {
        let mut buf = Vec::new();
        for v in [Value::from("hello"), Value::from("soon"), map(vec![])] {
            rmpv::encode::write_value(&mut buf, &v).unwrap();
        }
        assert!(matches!(decode_frame(&buf), Err(Error::Decode(_))));
    }
}
