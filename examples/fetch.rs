//! A custom `HttpTransport` built on JavaScript's `fetch`, for
//! wasm32-unknown-unknown. Logs in and prints today's usage under Node.
//!
//! ```sh
//! cargo build --example fetch --target wasm32-unknown-unknown
//! wasm-bindgen --target nodejs --out-dir target/fetch \
//!     target/wasm32-unknown-unknown/debug/examples/fetch.wasm
//! SENSE_EMAIL=... SENSE_PASSWORD=... node target/fetch/fetch.js
//! ```
//!
//! The same transport works in a browser, but `api.sense.com` doesn't send
//! CORS headers, so browsers block its responses.

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
mod fetch {
    use sense::{Error, HttpTransport};
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::JsFuture;

    #[wasm_bindgen]
    extern "C" {
        /// The global `fetch`: browsers, Node 18+, Deno and Bun all have it.
        #[wasm_bindgen(js_name = fetch)]
        fn js_fetch(req: &web_sys::Request) -> js_sys::Promise;
    }

    /// Sends requests with the global `fetch`.
    pub struct FetchTransport;

    impl HttpTransport for FetchTransport {
        async fn send(
            &self,
            req: http::Request<Vec<u8>>,
        ) -> sense::Result<http::Response<Vec<u8>>> {
            send(req)
                .await
                .map_err(|e| Error::Http(format!("{e:?}").into()))
        }
    }

    async fn send(req: http::Request<Vec<u8>>) -> Result<http::Response<Vec<u8>>, JsValue> {
        let (parts, body) = req.into_parts();
        let headers = web_sys::Headers::new()?;
        for (name, value) in &parts.headers {
            let value = value
                .to_str()
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            headers.append(name.as_str(), value)?;
        }
        let init = web_sys::RequestInit::new();
        init.set_method(parts.method.as_str());
        init.set_headers(&headers);
        if !body.is_empty() {
            init.set_body(&js_sys::Uint8Array::from(body.as_slice()));
        }
        let req = web_sys::Request::new_with_str_and_init(&parts.uri.to_string(), &init)?;

        let resp: web_sys::Response = JsFuture::from(js_fetch(&req)).await?.dyn_into()?;
        let mut out = http::Response::builder().status(resp.status());
        for pair in js_sys::try_iter(&resp.headers())?.ok_or("headers aren't iterable")? {
            let pair: js_sys::Array = pair?.dyn_into()?;
            if let (Some(name), Some(value)) = (pair.get(0).as_string(), pair.get(1).as_string()) {
                out = out.header(name, value);
            }
        }
        let body = JsFuture::from(resp.array_buffer()?).await?;
        out.body(js_sys::Uint8Array::new(&body).to_vec())
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Reads an environment variable through Node's `process.env`.
    pub fn env(name: &str) -> Option<String> {
        let env = js_sys::Reflect::get(&js_sys::global(), &"process".into())
            .and_then(|p| js_sys::Reflect::get(&p, &"env".into()))
            .ok()?;
        js_sys::Reflect::get(&env, &name.into()).ok()?.as_string()
    }
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
#[wasm_bindgen::prelude::wasm_bindgen(main)]
async fn main() -> Result<(), wasm_bindgen::JsValue> {
    let line = today().await.map_err(|e| e.to_string())?;
    web_sys::console::log_1(&line.into());
    Ok(())
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
async fn today() -> sense::Result<String> {
    let email = fetch::env("SENSE_EMAIL").unwrap_or_default();
    let password = fetch::env("SENSE_PASSWORD").unwrap_or_default();
    let client = sense::Client::new(fetch::FetchTransport);
    client.login(&email, &password).await?;
    let Some(monitor) = client.default_monitor_id() else {
        return Ok("account has no monitor".into());
    };
    let trends = client
        .trends(monitor, sense::models::Scale::Day, None)
        .await?;
    Ok(format!(
        "today: {:.1} kWh",
        trends.consumption.map_or(0.0, |c| c.total)
    ))
}

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
fn main() {
    eprintln!("build this example for wasm32-unknown-unknown");
}
