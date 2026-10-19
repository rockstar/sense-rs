# sense-rs

Unofficial Rust client for the [Sense](https://sense.com) home energy monitor API.
The API is undocumented and may change without notice.

```toml
[dependencies]
sense-rs = "0.1"
```

The crate imports as `sense`. The default API is async (Tokio). A blocking API is in `sense::sync`.
Response types live in `sense::models` and live-feed types in `sense::realtime`.

```rust
use futures_util::StreamExt;
use sense::models::Scale;
use sense::realtime::RealtimeMessage;
use sense::Client;

let client = Client::new();
client.login("me@example.com", "password").await?;   // Err(Error::MfaRequired { .. }) → client.login_mfa(..)
let monitor = client.default_monitor_id().unwrap();

let today = client.trends(monitor, Scale::Day, None).await?;
let devices = client.devices(monitor).await?;

let mut feed = client.realtime(monitor).await?;
while let Some(event) = feed.next().await {
    if let RealtimeMessage::Update(u) = event?.message {
        println!("{} W", u.w);
    }
}
```

```rust
let client = sense::sync::Client::new()?;
client.login("me@example.com", "password")?;
for event in client.realtime(client.default_monitor_id().unwrap())? { /* ... */ }
```

## What's covered

- Auth: `login`, `login_mfa`, `renew`, `logout`. Expired tokens renew automatically on a 401.
  Save `client.session()` and restore it with `set_session` to skip logging in.
  Renewals replace both tokens, so save the session again before exiting.
- Monitors: `monitor_overview`, `monitor_status`
- Devices: `devices`, `device`
- History: `trends`, `usage`, `cumulative`
- Realtime: `realtime` opens the MessagePack websocket feed
- Everything else: `get_json(path, query)` for any GET endpoint

Examples: `SENSE_EMAIL=... SENSE_PASSWORD=... cargo run --example realtime` (or `--example sync`).

## License

MIT. See [LICENSE](LICENSE).
