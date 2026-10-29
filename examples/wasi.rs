//! Logs in and prints today's usage from a wasm32-wasip2 component.
//!
//! ```sh
//! cargo build --example wasi --target wasm32-wasip2
//! wasmtime run -S http --env SENSE_EMAIL=... --env SENSE_PASSWORD=... \
//!     target/wasm32-wasip2/debug/examples/wasi.wasm
//! ```

#![allow(missing_docs)] // `wstd::main` generates an undocumented fn

#[cfg(all(target_os = "wasi", target_env = "p2"))]
#[wstd::main]
async fn main() -> sense::Result<()> {
    use sense::models::Scale;

    let email = std::env::var("SENSE_EMAIL").unwrap_or_default();
    let password = std::env::var("SENSE_PASSWORD").unwrap_or_default();
    let client = sense::Client::new(sense::WasiTransport::new());
    client.login(&email, &password).await?;
    let Some(monitor) = client.default_monitor_id() else {
        println!("account has no monitor");
        return Ok(());
    };
    let trends = client.trends(monitor, Scale::Day, None).await?;
    println!(
        "today: {:.1} kWh",
        trends.consumption.map_or(0.0, |c| c.total)
    );
    Ok(())
}

#[cfg(not(all(target_os = "wasi", target_env = "p2")))]
fn main() {
    eprintln!("build this example for wasm32-wasip2");
}
