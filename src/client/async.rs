use std::fmt::Write;
use std::sync::{Arc, PoisonError, RwLock};

use http::{Method, StatusCode};
use serde::de::DeserializeOwned;

use super::http::Http;
use crate::error::{Error, Result};
use crate::models::*;
#[cfg(feature = "native")]
use crate::realtime::RealtimeFeed;

const DEFAULT_API_URL: &str = "https://api.sense.com/apiservice/api/v1/";
#[cfg(feature = "native")]
const DEFAULT_REALTIME_URL: &str = "wss://clientrt.sense.com/";

/// Async client for the Sense API.
///
/// Cheap to clone; clones share the HTTP connection pool and the session.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    http: Http,
    api_url: String,
    #[cfg(feature = "native")]
    realtime_url: String,
    session: RwLock<Option<Session>>,
    /// Held while renewing, so concurrent 401s trigger only one `/renew`.
    renew_lock: tokio::sync::Mutex<()>,
}

/// Configures a [`Client`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    api_url: String,
    #[cfg(feature = "native")]
    realtime_url: String,
    session: Option<Session>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_API_URL.into(),
            #[cfg(feature = "native")]
            realtime_url: DEFAULT_REALTIME_URL.into(),
            session: None,
        }
    }
}

impl ClientBuilder {
    /// Override the REST base URL (default `https://api.sense.com/apiservice/api/v1/`).
    pub fn api_url(mut self, url: impl Into<String>) -> Self {
        self.api_url = with_trailing_slash(url.into());
        self
    }

    /// Override the realtime websocket base URL (default `wss://clientrt.sense.com/`).
    /// Requires the `native` feature.
    #[cfg(feature = "native")]
    pub fn realtime_url(mut self, url: impl Into<String>) -> Self {
        self.realtime_url = with_trailing_slash(url.into());
        self
    }

    /// Start with a saved session instead of logging in.
    pub fn session(mut self, session: Session) -> Self {
        self.session = Some(session);
        self
    }

    /// Build the client. Fails only if the HTTP client can't be created.
    pub fn build(self) -> Result<Client> {
        Ok(Client {
            inner: Arc::new(Inner {
                http: Http::new()?,
                api_url: self.api_url,
                #[cfg(feature = "native")]
                realtime_url: self.realtime_url,
                session: RwLock::new(self.session),
                renew_lock: tokio::sync::Mutex::new(()),
            }),
        })
    }
}

fn with_trailing_slash(mut url: String) -> String {
    if !url.ends_with('/') {
        url.push('/');
    }
    url
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    /// A client with default settings and no session.
    #[expect(
        clippy::expect_used,
        reason = "only fails if the TLS backend can't initialize"
    )]
    pub fn new() -> Self {
        ClientBuilder::default()
            .build()
            .expect("default client configuration is valid")
    }

    /// A builder for a client with custom settings.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    // ---- session -------------------------------------------------------

    /// The current session, if logged in. Persist it and pass it to
    /// [`set_session`](Self::set_session) later to skip logging in.
    /// Automatic renewals replace both tokens, so persist it again before
    /// exiting.
    pub fn session(&self) -> Option<Session> {
        self.inner
            .session
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replace the current session, e.g. with one saved from
    /// [`session`](Self::session). `None` forgets it without logging out.
    pub fn set_session(&self, session: Option<Session>) {
        *self
            .inner
            .session
            .write()
            .unwrap_or_else(PoisonError::into_inner) = session;
    }

    /// The first monitor on the account, which is the only one for most users.
    pub fn default_monitor_id(&self) -> Option<i64> {
        self.session()?.monitor_ids.first().copied()
    }

    /// Log in with email and password.
    ///
    /// Returns [`Error::MfaRequired`] if the account has MFA enabled; finish
    /// with [`login_mfa`](Self::login_mfa).
    pub async fn login(&self, email: &str, password: &str) -> Result<AuthResponse> {
        let form = [("email", email), ("password", password)];
        let auth: AuthResponse = self
            .send(Method::POST, "authenticate", &[], Some(&form), false)
            .await?;
        self.store_auth(&auth);
        Ok(auth)
    }

    /// Finish an MFA login with the token from [`Error::MfaRequired`] and a TOTP code.
    pub async fn login_mfa(&self, mfa_token: &str, totp: &str) -> Result<AuthResponse> {
        let now = jiff::Timestamp::now()
            .strftime("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let form = [
            ("mfa_token", mfa_token),
            ("totp", totp),
            ("client_time", now.as_str()),
        ];
        let auth: AuthResponse = self
            .send(Method::POST, "authenticate/mfa", &[], Some(&form), false)
            .await?;
        self.store_auth(&auth);
        Ok(auth)
    }

    /// Exchange the refresh token for a new access token.
    ///
    /// Requests that get a 401 call this automatically, so you rarely need it.
    pub async fn renew(&self) -> Result<()> {
        let _guard = self.inner.renew_lock.lock().await;
        self.renew_unlocked().await
    }

    /// Renew after `failed_token` got a 401, unless another task already did.
    async fn renew_after_401(&self, failed_token: &str) -> Result<()> {
        let _guard = self.inner.renew_lock.lock().await;
        if self.access_token()? != failed_token {
            return Ok(()); // renewed while we waited; just retry
        }
        self.renew_unlocked().await
    }

    /// Callers must hold `renew_lock`.
    async fn renew_unlocked(&self) -> Result<()> {
        let session = self.session().ok_or(Error::NotAuthenticated)?;
        let user_id = session.user_id.to_string();
        let form = [
            ("refresh_token", session.refresh_token.as_str()),
            ("user_id", user_id.as_str()),
        ];
        // Goes straight to `request_once`: the retry path in `send_raw` calls us.
        let (status, body) = self
            .request_once(Method::POST, "renew", &[], Some(&form), None)
            .await?;
        if !status.is_success() {
            return Err(api_error(status, &body));
        }
        let tokens: TokenResponse =
            serde_json::from_slice(&body).map_err(|e| Error::Decode(format!("renew: {e}")))?;
        // A login may have replaced the session while we waited; don't graft our tokens onto it.
        if let Some(s) = self
            .inner
            .session
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .filter(|s| s.refresh_token == session.refresh_token)
        {
            s.access_token = tokens.access_token;
            s.refresh_token = tokens.refresh_token;
        }
        Ok(())
    }

    /// End the session on the server and forget it locally.
    pub async fn logout(&self) -> Result<()> {
        let result = self.send_raw(Method::GET, "logout", &[], None, true).await;
        self.set_session(None);
        result.map(drop)
    }

    fn store_auth(&self, auth: &AuthResponse) {
        self.set_session(Some(Session {
            access_token: auth.access_token.clone(),
            refresh_token: auth.refresh_token.clone(),
            user_id: auth.user_id,
            monitor_ids: auth.monitors.iter().map(|m| m.id).collect(),
        }));
    }

    // ---- monitors ------------------------------------------------------

    /// Overview of a monitor: settings, solar config, and device counts.
    pub async fn monitor_overview(&self, monitor_id: i64) -> Result<MonitorOverviewResponse> {
        self.get(&format!("app/monitors/{monitor_id}/overview"), &[])
            .await
    }

    /// Connectivity and signal status of a monitor.
    pub async fn monitor_status(&self, monitor_id: i64) -> Result<MonitorStatus> {
        self.get(&format!("app/monitors/{monitor_id}/status"), &[])
            .await
    }

    // ---- devices -------------------------------------------------------

    /// All devices detected on a monitor.
    pub async fn devices(&self, monitor_id: i64) -> Result<DevicesOverview> {
        self.get(&format!("app/monitors/{monitor_id}/devices/overview"), &[])
            .await
    }

    /// Full details for one device. Returned as raw JSON because the shape
    /// varies a lot by device type.
    pub async fn device(&self, monitor_id: i64, device_id: &str) -> Result<serde_json::Value> {
        let device_id = path_segment(device_id)?;
        self.get(
            &format!("app/monitors/{monitor_id}/devices/{device_id}"),
            &[],
        )
        .await
    }

    // ---- history -------------------------------------------------------

    /// Usage and production totals for the period containing `start`
    /// (an ISO 8601 timestamp), or the current period when `start` is `None`.
    pub async fn trends(
        &self,
        monitor_id: i64,
        scale: Scale,
        start: Option<&str>,
    ) -> Result<Trends> {
        // The API rejects requests without `start`; now is in the current period.
        let start = start.map_or_else(|| jiff::Timestamp::now().to_string(), str::to_string);
        let query = [
            ("monitor_id", monitor_id.to_string()),
            ("scale", scale.to_string()),
            ("start", start),
        ];
        self.get("app/history/trends", &query).await
    }

    /// `frames` samples of `granularity` beginning at `start` (ISO 8601).
    pub async fn usage(
        &self,
        monitor_id: i64,
        granularity: Granularity,
        start: &str,
        frames: u32,
    ) -> Result<Usage> {
        let query = [
            ("monitor_id", monitor_id.to_string()),
            ("granularity", granularity.to_string()),
            ("start", start.to_string()),
            ("frames", frames.to_string()),
        ];
        self.get("app/history/usage", &query).await
    }

    /// Consumption so far in the current billing cycle.
    pub async fn cumulative(&self, monitor_id: i64) -> Result<CumulativeUsage> {
        self.get(
            "app/history/cumulative",
            &[("monitor_id", monitor_id.to_string())],
        )
        .await
    }

    // ---- realtime ------------------------------------------------------

    /// Open the realtime websocket feed for a monitor. Requires the `native`
    /// feature.
    ///
    /// A 401 on the handshake renews the token and retries once. A 401 from
    /// the open feed ([`Error::Api`] with status 401) does not; call
    /// [`renew`](Self::renew) before reconnecting.
    #[cfg(feature = "native")]
    pub async fn realtime(&self, monitor_id: i64) -> Result<RealtimeFeed> {
        let url = format!(
            "{}monitors/{monitor_id}/realtimefeed",
            self.inner.realtime_url
        );
        let token = self.access_token()?;
        match RealtimeFeed::connect(&url, &token).await {
            Err(Error::Api { status: 401, .. }) => {
                self.renew_after_401(&token).await?;
                RealtimeFeed::connect(&url, &self.access_token()?).await
            }
            other => other,
        }
    }

    // ---- escape hatch --------------------------------------------------

    /// GET any endpoint, relative to the API base URL, as raw JSON.
    ///
    /// ```no_run
    /// # async fn f(client: sense::Client) -> sense::Result<()> {
    /// let rate_zones = client.get_json("app/monitors/12345/rate_zones", &[]).await?;
    /// # Ok(()) }
    /// ```
    pub async fn get_json(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<serde_json::Value> {
        self.get(path, query).await
    }

    // ---- plumbing ------------------------------------------------------

    fn access_token(&self) -> Result<String> {
        self.inner
            .session
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|s| s.access_token.clone())
            .ok_or(Error::NotAuthenticated)
    }

    async fn get<T: DeserializeOwned>(&self, path: &str, query: &[(&str, String)]) -> Result<T> {
        self.send(Method::GET, path, query, None, true).await
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        form: Option<&[(&str, &str)]>,
        authed: bool,
    ) -> Result<T> {
        let body = self.send_raw(method, path, query, form, authed).await?;
        serde_json::from_slice(&body).map_err(|e| Error::Decode(format!("{path}: {e}")))
    }

    async fn send_raw(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        form: Option<&[(&str, &str)]>,
        authed: bool,
    ) -> Result<Vec<u8>> {
        let token = if authed {
            Some(self.access_token()?)
        } else {
            None
        };
        let (mut status, mut body) = self
            .request_once(method.clone(), path, query, form, token.as_deref())
            .await?;
        if let (StatusCode::UNAUTHORIZED, Some(sent)) = (status, &token) {
            self.renew_after_401(sent).await?;
            let token = self.access_token()?;
            (status, body) = self
                .request_once(method, path, query, form, Some(&token))
                .await?;
        }
        if status.is_success() {
            Ok(body)
        } else {
            Err(api_error(status, &body))
        }
    }

    async fn request_once(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        form: Option<&[(&str, &str)]>,
        access_token: Option<&str>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let url = format!("{}{path}", self.inner.api_url);
        self.inner
            .http
            .request(method, &url, query, form, access_token)
            .await
    }
}

/// Percent-encodes `s` as a single URL path segment, so `/`, `?` and `#`
/// can't change which endpoint a request goes to.
fn path_segment(s: &str) -> Result<String> {
    // URL parsers treat these as navigation even when percent-encoded.
    if matches!(s, "" | "." | "..") {
        return Err(Error::InvalidArgument(format!(
            "invalid path segment {s:?}"
        )));
    }
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(char::from(b));
        } else {
            // Writing to a String cannot fail.
            let _ = write!(out, "%{b:02X}");
        }
    }
    Ok(out)
}

pub(crate) fn api_error(status: StatusCode, body: &[u8]) -> Error {
    let parsed: Option<ApiErrorBody> = serde_json::from_slice(body).ok();
    if let Some(mfa_token) = parsed.as_ref().and_then(|b| b.mfa_token.clone()) {
        return Error::MfaRequired { mfa_token };
    }
    Error::Api {
        status: status.as_u16(),
        reason: parsed.and_then(|b| b.error_reason.or(b.status)),
    }
}

#[cfg(test)]
#[cfg(feature = "native")]
mod tests {
    use super::*;
    use crate::client::PROTOCOL_VERSION;

    #[test]
    fn mfa_error_is_detected() {
        let body = br#"{"status":"mfa_required","mfa_token":"abc","mfa_type":"totp"}"#;
        assert!(matches!(
            api_error(StatusCode::UNAUTHORIZED, body),
            Error::MfaRequired { mfa_token } if mfa_token == "abc"
        ));
    }

    #[test]
    fn api_error_reason() {
        let body = br#"{"status":"error","error_reason":"Bad email or password"}"#;
        assert!(matches!(
            api_error(StatusCode::UNAUTHORIZED, body),
            Error::Api { status: 401, reason: Some(r) } if r == "Bad email or password"
        ));
    }

    /// One request as the mock server saw it.
    #[derive(Debug)]
    struct Seen {
        line: String,
        auth: Option<String>,
        /// The request line and headers.
        head: String,
        body: String,
    }

    impl Seen {
        fn header(&self, name: &str) -> Option<&str> {
            self.head.lines().find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.eq_ignore_ascii_case(name).then(|| v.trim())
            })
        }
    }

    /// Reads one HTTP request from `sock`.
    async fn read_request(sock: &mut tokio::net::TcpStream) -> Seen {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let n = sock.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let mut seen = Seen {
            line: String::new(),
            auth: None,
            head: String::from_utf8_lossy(&buf[..header_end]).into_owned(),
            body: String::new(),
        };
        seen.line = seen.head.lines().next().unwrap().to_string();
        seen.auth = seen.header("authorization").map(str::to_string);
        let len: usize = seen
            .header("content-length")
            .map_or(0, |v| v.parse().unwrap());
        while buf.len() < header_end + len {
            let n = sock.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
        }
        seen.body = String::from_utf8_lossy(&buf[header_end..]).into_owned();
        seen
    }

    async fn write_response(sock: &mut tokio::net::TcpStream, (status, body): (u16, &str)) {
        use tokio::io::AsyncWriteExt;

        let resp = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(resp.as_bytes()).await.unwrap();
    }

    /// Serves `replies` in order, one per connection, and returns what it saw.
    async fn mock_server(
        replies: Vec<(u16, &'static str)>,
    ) -> (String, tokio::task::JoinHandle<Vec<Seen>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut seen = Vec::new();
            for reply in replies {
                let (mut sock, _) = listener.accept().await.unwrap();
                seen.push(read_request(&mut sock).await);
                write_response(&mut sock, reply).await;
            }
            seen
        });
        (url, handle)
    }

    fn client_for(url: String) -> Client {
        Client::builder()
            .api_url(url)
            .session(Session {
                access_token: "old-access".into(),
                refresh_token: "old-refresh".into(),
                user_id: 7,
                monitor_ids: vec![1],
            })
            .build()
            .unwrap()
    }

    const EXPIRED: (u16, &str) = (401, r#"{"status":"error","error_reason":"expired"}"#);
    const RENEWED: (u16, &str) = (
        200,
        r#"{"access_token":"new-access","refresh_token":"new-refresh"}"#,
    );

    #[test]
    fn path_segment_escapes_separators() {
        assert_eq!(path_segment("abc-1_2.3~").unwrap(), "abc-1_2.3~");
        assert_eq!(path_segment("../users/1").unwrap(), "..%2Fusers%2F1");
        assert_eq!(path_segment("a?b#c d").unwrap(), "a%3Fb%23c%20d");
        assert_eq!(path_segment("é").unwrap(), "%C3%A9");
        for bad in ["", ".", ".."] {
            assert!(
                matches!(path_segment(bad), Err(Error::InvalidArgument(_))),
                "{bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn device_id_stays_in_its_segment() {
        let (url, server) = mock_server(vec![(200, "{}")]).await;
        client_for(url).device(1, "../../users/7?x").await.unwrap();
        let seen = server.await.unwrap();
        assert!(
            seen[0]
                .line
                .starts_with("GET /app/monitors/1/devices/..%2F..%2Fusers%2F7%3Fx "),
            "{}",
            seen[0].line
        );
    }

    /// Plain `#[test]`: the blocking client panics inside a runtime. The
    /// request timeout needs timers, so this also guards `enable_all()`.
    #[test]
    fn blocking_client_makes_requests() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (url, server) = rt.block_on(mock_server(vec![(200, "{}")]));
        let client = crate::sync::Client::from_async(client_for(url)).unwrap();

        client.monitor_status(1).unwrap();

        let seen = rt.block_on(server).unwrap();
        assert!(seen[0].line.starts_with("GET /app/monitors/1/status "));
        assert_eq!(seen[0].auth.as_deref(), Some("bearer old-access"));
    }

    #[tokio::test]
    async fn sends_required_headers() {
        let (url, server) = mock_server(vec![(200, "{}")]).await;
        client_for(url).monitor_status(1).await.unwrap();

        let seen = server.await.unwrap();
        assert_eq!(seen[0].header("x-sense-protocol"), Some(PROTOCOL_VERSION));
        assert_eq!(
            seen[0].header("user-agent"),
            Some(concat!("sense-rs/", env!("CARGO_PKG_VERSION")))
        );
    }

    #[tokio::test]
    async fn realtime_sends_required_headers() {
        let (ws_url, ws) = mock_server(vec![(500, r#"{"status":"error"}"#)]).await;
        let client = Client::builder()
            .realtime_url(ws_url.replacen("http", "ws", 1))
            .session(Session {
                access_token: "old-access".into(),
                refresh_token: "old-refresh".into(),
                user_id: 7,
                monitor_ids: vec![1],
            })
            .build()
            .unwrap();

        let err = client.realtime(1).await.err().unwrap();

        assert!(matches!(err, Error::Api { status: 500, .. }), "{err:?}");
        let seen = ws.await.unwrap();
        assert_eq!(seen[0].header("accept"), Some("application/x-msgpack"));
        assert_eq!(seen[0].header("x-sense-protocol"), Some(PROTOCOL_VERSION));
        assert_eq!(seen[0].auth.as_deref(), Some("bearer old-access"));
    }

    #[tokio::test]
    async fn renews_and_retries_once_on_401() {
        let (url, server) = mock_server(vec![EXPIRED, RENEWED, (200, "{}")]).await;
        let client = client_for(url);

        client.monitor_status(1).await.unwrap();

        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 3);
        assert!(seen[0].line.starts_with("GET /app/monitors/1/status "));
        assert_eq!(seen[0].auth.as_deref(), Some("bearer old-access"));

        assert!(seen[1].line.starts_with("POST /renew "));
        assert_eq!(seen[1].auth, None, "renew must not send the expired token");
        assert!(
            seen[1].body.contains("refresh_token=old-refresh"),
            "{}",
            seen[1].body
        );
        assert!(seen[1].body.contains("user_id=7"), "{}", seen[1].body);

        assert!(seen[2].line.starts_with("GET /app/monitors/1/status "));
        assert_eq!(seen[2].auth.as_deref(), Some("bearer new-access"));

        let session = client.session().unwrap();
        assert_eq!(session.access_token, "new-access");
        assert_eq!(session.refresh_token, "new-refresh");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_401s_renew_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let renews = Arc::new(AtomicUsize::new(0));
        // Holds both expired requests until each has arrived, so both 401s
        // reach the client before either renew can start.
        let both_expired = Arc::new(tokio::sync::Barrier::new(2));
        {
            let renews = renews.clone();
            tokio::spawn(async move {
                loop {
                    let (mut sock, _) = listener.accept().await.unwrap();
                    let (renews, both_expired) = (renews.clone(), both_expired.clone());
                    tokio::spawn(async move {
                        let req = read_request(&mut sock).await;
                        let reply = if req.line.starts_with("POST /renew ") {
                            renews.fetch_add(1, Ordering::SeqCst);
                            RENEWED
                        } else if req.auth.as_deref() == Some("bearer old-access") {
                            both_expired.wait().await;
                            EXPIRED
                        } else {
                            (200, "{}")
                        };
                        write_response(&mut sock, reply).await;
                    });
                }
            });
        }
        let client = client_for(url);

        let (a, b) = tokio::join!(client.monitor_status(1), client.monitor_status(2));

        a.unwrap();
        b.unwrap();
        assert_eq!(renews.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    // The handshake callback's error type is fixed by tungstenite.
    #[allow(clippy::result_large_err)]
    async fn realtime_renews_and_reconnects_on_401_handshake() {
        use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

        let (api_url, api) = mock_server(vec![RENEWED]).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}/", listener.local_addr().unwrap());
        let ws = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let first = read_request(&mut sock).await.auth;
            write_response(&mut sock, EXPIRED).await;

            let (sock, _) = listener.accept().await.unwrap();
            let mut second = None;
            let _ws = tokio_tungstenite::accept_hdr_async(sock, |req: &Request, resp: Response| {
                second = req
                    .headers()
                    .get("authorization")
                    .map(|v| v.to_str().unwrap().to_string());
                Ok(resp)
            })
            .await
            .unwrap();
            (first, second)
        });
        let client = Client::builder()
            .api_url(api_url)
            .realtime_url(ws_url)
            .session(Session {
                access_token: "old-access".into(),
                refresh_token: "old-refresh".into(),
                user_id: 7,
                monitor_ids: vec![1],
            })
            .build()
            .unwrap();

        client.realtime(1).await.unwrap();

        let (first, second) = ws.await.unwrap();
        assert_eq!(first.as_deref(), Some("bearer old-access"));
        assert_eq!(second.as_deref(), Some("bearer new-access"));
        let seen = api.await.unwrap();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].line.starts_with("POST /renew "));
    }

    #[tokio::test]
    async fn renew_does_not_overwrite_a_newer_login() {
        use tokio::sync::Notify;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        // Holds the renew reply until the test has swapped in a new session.
        let (arrived, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        {
            let (arrived, release) = (arrived.clone(), release.clone());
            tokio::spawn(async move {
                let (mut sock, _) = listener.accept().await.unwrap();
                read_request(&mut sock).await;
                arrived.notify_one();
                release.notified().await;
                write_response(&mut sock, RENEWED).await;
            });
        }
        let client = client_for(url);

        let (renewed, ()) = tokio::join!(client.renew(), async {
            arrived.notified().await;
            client.set_session(Some(Session {
                access_token: "login-access".into(),
                refresh_token: "login-refresh".into(),
                user_id: 8,
                monitor_ids: vec![2],
            }));
            release.notify_one();
        });

        renewed.unwrap();
        let session = client.session().unwrap();
        assert_eq!(session.access_token, "login-access");
        assert_eq!(session.refresh_token, "login-refresh");
        assert_eq!(session.user_id, 8);
    }

    #[tokio::test]
    async fn login_with_mfa_stores_session() {
        let (url, server) = mock_server(vec![
            (401, r#"{"status":"mfa_required","mfa_token":"mfa-1"}"#),
            (
                200,
                r#"{"user_id":9,"access_token":"a","refresh_token":"r","monitors":[{"id":42},{"id":43}]}"#,
            ),
        ])
        .await;
        let client = Client::builder().api_url(url).build().unwrap();

        let err = client.login("me@x.com", "pw").await.unwrap_err();
        let Error::MfaRequired { mfa_token } = err else {
            panic!("{err:?}")
        };
        client.login_mfa(&mfa_token, "123456").await.unwrap();

        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 2);
        assert!(seen[0].line.starts_with("POST /authenticate "));
        assert_eq!(seen[0].body, "email=me%40x.com&password=pw");
        assert!(seen[1].line.starts_with("POST /authenticate/mfa "));
        let client_time = seen[1]
            .body
            .strip_prefix("mfa_token=mfa-1&totp=123456&client_time=")
            .unwrap_or_else(|| panic!("{}", seen[1].body))
            .replace("%3A", ":");
        // Whole seconds in UTC, e.g. 2009-02-13T23:31:30Z.
        assert_eq!(client_time.len(), 20, "{client_time}");
        assert!(client_time.ends_with('Z'), "{client_time}");
        let sent: jiff::Timestamp = client_time.parse().unwrap();
        let age = jiff::Timestamp::now().as_second() - sent.as_second();
        assert!((0..60).contains(&age), "{client_time}");
        let session = client.session().unwrap();
        assert_eq!(session.access_token, "a");
        assert_eq!(session.user_id, 9);
        assert_eq!(client.default_monitor_id(), Some(42));
    }

    #[tokio::test]
    async fn bad_password_does_not_renew_old_session() {
        let (url, server) = mock_server(vec![(
            401,
            r#"{"status":"error","error_reason":"Bad email or password"}"#,
        )])
        .await;
        let client = client_for(url);

        let err = client.login("me@x.com", "wrong").await.unwrap_err();

        assert!(matches!(err, Error::Api { status: 401, .. }), "{err:?}");
        let seen = server.await.unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].auth, None, "login must not send the old token");
    }

    #[tokio::test]
    async fn logout_clears_session() {
        let (url, server) = mock_server(vec![(200, "{}")]).await;
        let client = client_for(url);

        client.logout().await.unwrap();

        let seen = server.await.unwrap();
        assert!(seen[0].line.starts_with("GET /logout "), "{}", seen[0].line);
        assert_eq!(seen[0].auth.as_deref(), Some("bearer old-access"));
        assert!(client.session().is_none());
    }

    #[tokio::test]
    async fn logout_clears_session_on_server_error() {
        let (url, server) = mock_server(vec![(500, r#"{"status":"error"}"#)]).await;
        let client = client_for(url);

        let err = client.logout().await.unwrap_err();

        assert!(matches!(err, Error::Api { status: 500, .. }), "{err:?}");
        assert_eq!(server.await.unwrap().len(), 1);
        assert!(client.session().is_none());
    }

    /// The path and query of a request line like `GET /a?b=c HTTP/1.1`.
    fn target(seen: &Seen) -> &str {
        seen.line.split(' ').nth(1).unwrap()
    }

    #[tokio::test]
    async fn history_queries() {
        let (url, server) = mock_server(vec![(200, "{}"); 3]).await;
        let client = client_for(url);

        client
            .trends(1, Scale::Week, Some("2024-01-02T03:04:05Z"))
            .await
            .unwrap();
        client
            .usage(2, Granularity::Hour, "2024-01-02T03:04:05Z", 24)
            .await
            .unwrap();
        client.cumulative(3).await.unwrap();

        let seen = server.await.unwrap();
        assert_eq!(
            target(&seen[0]),
            "/app/history/trends?monitor_id=1&scale=WEEK&start=2024-01-02T03%3A04%3A05Z"
        );
        assert_eq!(
            target(&seen[1]),
            "/app/history/usage?monitor_id=2&granularity=hour&start=2024-01-02T03%3A04%3A05Z&frames=24"
        );
        assert_eq!(target(&seen[2]), "/app/history/cumulative?monitor_id=3");
    }

    #[tokio::test]
    async fn trends_without_start_sends_now() {
        let (url, server) = mock_server(vec![(200, "{}")]).await;
        client_for(url).trends(1, Scale::Day, None).await.unwrap();

        let seen = server.await.unwrap();
        let start = target(&seen[0])
            .strip_prefix("/app/history/trends?monitor_id=1&scale=DAY&start=")
            .unwrap_or_else(|| panic!("{}", seen[0].line))
            .replace("%3A", ":");
        let sent: jiff::Timestamp = start.parse().unwrap();
        let age = jiff::Timestamp::now().as_second() - sent.as_second();
        assert!((0..60).contains(&age), "{start}");
    }

    #[test]
    fn scale_and_granularity_display() {
        let scales = [
            Scale::Day,
            Scale::Week,
            Scale::Month,
            Scale::Year,
            Scale::Cycle,
        ];
        let shown: Vec<_> = scales.iter().map(ToString::to_string).collect();
        assert_eq!(shown, ["DAY", "WEEK", "MONTH", "YEAR", "CYCLE"]);
        let granularities = [Granularity::Second, Granularity::Minute, Granularity::Hour];
        let shown: Vec<_> = granularities.iter().map(ToString::to_string).collect();
        assert_eq!(shown, ["second", "minute", "hour"]);
    }

    #[tokio::test]
    async fn second_401_is_returned_not_retried() {
        let (url, server) = mock_server(vec![EXPIRED, RENEWED, EXPIRED]).await;
        let client = client_for(url);

        let err = client.monitor_status(1).await.unwrap_err();

        assert!(matches!(err, Error::Api { status: 401, .. }), "{err:?}");
        assert_eq!(server.await.unwrap().len(), 3);
    }
}
