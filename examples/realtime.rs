//! Print live power readings.
//!
//! SENSE_EMAIL=... SENSE_PASSWORD=... [SENSE_TOTP=...] cargo run --example realtime

use futures_util::StreamExt;
use sense::realtime::RealtimeMessage;
use sense::{Client, Error};

#[tokio::main]
async fn main() -> sense::Result<()> {
    let email = std::env::var("SENSE_EMAIL").expect("set SENSE_EMAIL");
    let password = std::env::var("SENSE_PASSWORD").expect("set SENSE_PASSWORD");

    let client = Client::new();
    match client.login(&email, &password).await {
        Ok(_) => {}
        Err(Error::MfaRequired { mfa_token }) => {
            let totp = std::env::var("SENSE_TOTP").expect("account uses MFA; set SENSE_TOTP");
            client.login_mfa(&mfa_token, &totp).await?;
        }
        Err(e) => return Err(e),
    }
    let monitor = client.default_monitor_id().expect("no monitors on account");

    let mut feed = client.realtime(monitor).await?;
    while let Some(event) = feed.next().await {
        match event?.message {
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
