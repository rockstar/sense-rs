//! The realtime feed at `wss://clientrt.sense.com/monitors/{id}/realtimefeed`.
//!
//! Each binary frame holds three MessagePack values back to back: a message
//! type string, a unix timestamp, and a payload map. Commands go the other
//! way in the same shape.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::client::PROTOCOL_VERSION;
use crate::error::{Error, Result};
use crate::models::ConnectionState;

/// A live connection to a monitor's realtime feed.
///
/// Implements [`Stream`]; use [`StreamExt::next`] to read events. The server
/// sends [`RealtimeMessage::Update`] about once a second.
pub struct RealtimeFeed {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl RealtimeFeed {
    pub(crate) async fn connect(url: &str, access_token: &str) -> Result<Self> {
        let mut req = url.into_client_request()?;
        let headers = req.headers_mut();
        headers.insert(
            "Authorization",
            format!("bearer {access_token}").parse().unwrap(),
        );
        headers.insert("Accept", "application/x-msgpack".parse().unwrap());
        headers.insert("X-Sense-Protocol", PROTOCOL_VERSION.parse().unwrap());

        let ws = match tokio_tungstenite::connect_async(req).await {
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
        let mut feed = RealtimeFeed { ws };
        feed.start_updates().await?;
        Ok(feed)
    }

    /// Ask the server to (re)start sending realtime updates.
    ///
    /// Called on connect. The Android app re-sends it after periods of
    /// inactivity; do the same if updates stop arriving.
    pub async fn start_updates(&mut self) -> Result<()> {
        self.send_command("start_realtime_updates").await
    }

    async fn send_command(&mut self, command: &str) -> Result<()> {
        let mut buf = Vec::new();
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // Writing to a Vec cannot fail.
        rmp::encode::write_str(&mut buf, command).unwrap();
        rmp::encode::write_uint(&mut buf, epoch).unwrap();
        rmp::encode::write_map_len(&mut buf, 0).unwrap();
        self.ws.send(Message::Binary(buf.into())).await?;
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
        loop {
            let msg = match self.ws.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
                Poll::Ready(Some(Ok(msg))) => msg,
            };
            match msg {
                Message::Binary(bytes) => return Poll::Ready(Some(decode_frame(&bytes))),
                Message::Close(_) => return Poll::Ready(None),
                // Pings are answered by tungstenite; nothing else is expected.
                _ => continue,
            }
        }
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
    pub w: f64,
    pub channel: i64,
    pub frame: i64,
}

#[derive(Deserialize)]
struct ErrorPayload {
    error_reason: Option<String>,
}

fn decode_frame(bytes: &[u8]) -> Result<RealtimeEvent> {
    let mut cur = bytes;
    let mut next = || rmpv::decode::read_value(&mut cur).map_err(|e| Error::Decode(e.to_string()));
    let first = next()?;
    // Accept a single [type, epoch, payload] array too, in case the server wraps it.
    let (kind, epoch, payload) = match first {
        rmpv::Value::Array(mut v) if v.len() == 3 => {
            let payload = v.pop().unwrap();
            let epoch = v.pop().unwrap();
            (v.pop().unwrap(), epoch, payload)
        }
        kind => {
            let epoch = next()?;
            let payload = next().unwrap_or(rmpv::Value::Nil);
            (kind, epoch, payload)
        }
    };
    let kind = kind
        .as_str()
        .ok_or_else(|| Error::Decode(format!("message type is not a string: {kind}")))?
        .to_string();
    let epoch = epoch.as_i64().unwrap_or_default();

    let message = match kind.as_str() {
        "hello" => RealtimeMessage::Hello(from_msgpack(payload)?),
        "realtime_update" => RealtimeMessage::Update(Box::new(from_msgpack(payload)?)),
        "data_change" => RealtimeMessage::DataChange(from_msgpack(payload)?),
        "monitor_info" => RealtimeMessage::MonitorInfo(from_msgpack(payload)?),
        "error" => {
            let e: ErrorPayload = from_msgpack(payload)?;
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
        let payload = map(vec![
            ("authorized", Value::from(false)),
            ("error_reason", Value::from("Unauthorized")),
        ]);
        let err = decode_frame(&frame("error", payload)).unwrap_err();
        assert!(matches!(err, Error::Realtime(r) if r == "Unauthorized"));
    }

    #[test]
    fn decodes_wrapped_array() {
        let mut buf = Vec::new();
        let v = Value::Array(vec![Value::from("hello"), Value::from(5), map(vec![])]);
        rmpv::encode::write_value(&mut buf, &v).unwrap();
        let ev = decode_frame(&buf).unwrap();
        assert_eq!(ev.epoch, 5);
        assert!(matches!(ev.message, RealtimeMessage::Hello(_)));
    }
}
