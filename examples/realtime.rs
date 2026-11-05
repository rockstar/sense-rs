//! Print live power readings.
//!
//! SENSE_EMAIL=... SENSE_PASSWORD=... [SENSE_TOTP=...] cargo run --example realtime
//!
//! The session is saved to `sense-session.json` (or `$SENSE_SESSION`) and reused
//! on the next run, so the login details are only needed the first time.
#![allow(clippy::expect_used)] // examples may panic on missing env vars

use futures_util::StreamExt;
use sense::models::Session;
use sense::realtime::RealtimeMessage;
use sense::{Client, Error, NativeTransport};

#[tokio::main]
async fn main() -> sense::Result<()> {
    let path = std::env::var("SENSE_SESSION").unwrap_or_else(|_| "sense-session.json".into());
    let client = Client::new(NativeTransport::new()?);

    let mut saved: Option<Session> = std::fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok());
    if saved.is_some() {
        client.set_session(saved.clone());
    } else {
        login(&client).await?;
    }
    // Any call can renew the session, which replaces the refresh token. Save after
    // each call, not just on exit: a process that is killed never gets to exit.
    save_if_changed(&client, &mut saved, &path);

    let monitor = client.default_monitor_id().expect("no monitors on account");

    let feed = client.realtime(monitor).await;
    save_if_changed(&client, &mut saved, &path);
    let mut feed = feed?;
    while let Some(event) = feed.next().await {
        // A bad message or a server `error` doesn't end the feed, so keep going.
        // Anything else (timeout, socket closed) means open a new feed.
        let event = match event {
            Ok(event) => event,
            Err(e @ (Error::Decode(_) | Error::Realtime(_))) => {
                eprintln!("{e}");
                continue;
            }
            Err(e) => return Err(e),
        };
        match event.message {
            RealtimeMessage::Update(u) => {
                let on: Vec<_> = u.devices.iter().filter_map(|d| d.name.as_deref()).collect();
                println!(
                    "{:>7.0} W  solar {:>6.0} W  {}",
                    u.w,
                    u.solar_w.unwrap_or(0.0),
                    on.join(", ")
                );
            }
            other => println!("{other:?}"),
        }
    }
    Ok(())
}

async fn login(client: &Client<NativeTransport>) -> sense::Result<()> {
    let email = std::env::var("SENSE_EMAIL").expect("set SENSE_EMAIL");
    let password = std::env::var("SENSE_PASSWORD").expect("set SENSE_PASSWORD");
    match client.login(&email, &password).await {
        Ok(_) => Ok(()),
        Err(Error::MfaRequired { mfa_token }) => {
            let totp = std::env::var("SENSE_TOTP").expect("account uses MFA; set SENSE_TOTP");
            client.login_mfa(&mfa_token, &totp).await.map(drop)
        }
        Err(e) => Err(e),
    }
}

fn save_if_changed(client: &Client<NativeTransport>, saved: &mut Option<Session>, path: &str) {
    let current = client.session();
    if current != *saved {
        let json = serde_json::to_vec(&current).expect("a session always serializes");
        std::fs::write(path, json).expect("write session file");
        *saved = current;
    }
}
