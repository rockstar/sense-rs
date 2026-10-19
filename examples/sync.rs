//! Blocking API: print today's usage and the device list.
//!
//! SENSE_EMAIL=... SENSE_PASSWORD=... [SENSE_TOTP=...] cargo run --example sync
#![allow(clippy::expect_used)] // examples may panic on missing env vars

use sense::Error;
use sense::models::Scale;
use sense::sync::Client;

fn main() -> sense::Result<()> {
    let email = std::env::var("SENSE_EMAIL").expect("set SENSE_EMAIL");
    let password = std::env::var("SENSE_PASSWORD").expect("set SENSE_PASSWORD");

    let client = Client::new()?;
    match client.login(&email, &password) {
        Ok(_) => {}
        Err(Error::MfaRequired { mfa_token }) => {
            let totp = std::env::var("SENSE_TOTP").expect("account uses MFA; set SENSE_TOTP");
            client.login_mfa(&mfa_token, &totp)?;
        }
        Err(e) => return Err(e),
    }
    let monitor = client.default_monitor_id().expect("no monitors on account");

    let today = client.trends(monitor, Scale::Day, None)?;
    if let Some(c) = today.consumption {
        println!("Today: {:.2} kWh", c.total);
    }
    for device in client.devices(monitor)?.devices {
        println!("  {}", device.name);
    }
    Ok(())
}
