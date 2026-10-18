use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;

use super::PROTOCOL_VERSION;
use crate::error::{Error, Result};
use crate::models::*;
use crate::realtime::RealtimeFeed;

const DEFAULT_API_URL: &str = "https://api.sense.com/apiservice/api/v1/";
const DEFAULT_REALTIME_URL: &str = "wss://clientrt.sense.com/";
const USER_AGENT: &str = concat!("sense-rs/", env!("CARGO_PKG_VERSION"));

/// Async client for the Sense API.
///
/// Cheap to clone; clones share the HTTP connection pool and the session.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    api_url: String,
    realtime_url: String,
    session: RwLock<Option<Session>>,
}

/// Configures a [`Client`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    api_url: String,
    realtime_url: String,
    session: Option<Session>,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            api_url: DEFAULT_API_URL.into(),
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
    pub fn realtime_url(mut self, url: impl Into<String>) -> Self {
        self.realtime_url = with_trailing_slash(url.into());
        self
    }

    /// Start with a saved session instead of logging in.
    pub fn session(mut self, session: Session) -> Self {
        self.session = Some(session);
        self
    }

    pub fn build(self) -> Result<Client> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("X-Sense-Protocol", PROTOCOL_VERSION.parse().unwrap());
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .default_headers(headers)
            .build()?;
        Ok(Client {
            inner: Arc::new(Inner {
                http,
                api_url: self.api_url,
                realtime_url: self.realtime_url,
                session: RwLock::new(self.session),
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
    pub fn new() -> Self {
        ClientBuilder::default()
            .build()
            .expect("default client configuration is valid")
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    // ---- session -------------------------------------------------------

    /// The current session, if logged in. Persist it and pass it to
    /// [`set_session`](Self::set_session) later to skip logging in.
    pub fn session(&self) -> Option<Session> {
        self.inner.session.read().unwrap().clone()
    }

    pub fn set_session(&self, session: Option<Session>) {
        *self.inner.session.write().unwrap() = session;
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
        let now = iso8601_now();
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
        if let Some(s) = self.inner.session.write().unwrap().as_mut() {
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

    pub async fn monitor_overview(&self, monitor_id: i64) -> Result<MonitorOverviewResponse> {
        self.get(&format!("app/monitors/{monitor_id}/overview"), &[])
            .await
    }

    pub async fn monitor_status(&self, monitor_id: i64) -> Result<MonitorStatus> {
        self.get(&format!("app/monitors/{monitor_id}/status"), &[])
            .await
    }

    // ---- devices -------------------------------------------------------

    pub async fn devices(&self, monitor_id: i64) -> Result<DevicesOverview> {
        self.get(&format!("app/monitors/{monitor_id}/devices/overview"), &[])
            .await
    }

    /// Full details for one device. Returned as raw JSON because the shape
    /// varies a lot by device type.
    pub async fn device(&self, monitor_id: i64, device_id: &str) -> Result<serde_json::Value> {
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
        let mut query = vec![
            ("monitor_id", monitor_id.to_string()),
            ("scale", scale.to_string()),
        ];
        if let Some(start) = start {
            query.push(("start", start.to_string()));
        }
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

    /// Open the realtime websocket feed for a monitor.
    pub async fn realtime(&self, monitor_id: i64) -> Result<RealtimeFeed> {
        let url = format!(
            "{}monitors/{monitor_id}/realtimefeed",
            self.inner.realtime_url
        );
        match RealtimeFeed::connect(&url, &self.access_token()?).await {
            Err(Error::Api { status: 401, .. }) => {
                self.renew().await?;
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
            .unwrap()
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
        if status == StatusCode::UNAUTHORIZED && authed {
            self.renew().await?;
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
        let mut req = self
            .inner
            .http
            .request(method, format!("{}{path}", self.inner.api_url))
            .query(query);
        if let Some(token) = access_token {
            req = req.header("Authorization", format!("bearer {token}"));
        }
        if let Some(form) = form {
            req = req.form(form);
        }
        let resp = req.send().await?;
        let status = resp.status();
        Ok((status, resp.bytes().await?.to_vec()))
    }
}

fn api_error(status: StatusCode, body: &[u8]) -> Error {
    let parsed: Option<ApiErrorBody> = serde_json::from_slice(body).ok();
    if let Some(mfa_token) = parsed.as_ref().and_then(|b| b.mfa_token.clone()) {
        return Error::MfaRequired { mfa_token };
    }
    Error::Api {
        status: status.as_u16(),
        reason: parsed.and_then(|b| b.error_reason.or(b.status)),
    }
}

/// Current UTC time as `YYYY-MM-DDTHH:MM:SSZ`.
fn iso8601_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil-from-days, from Howard Hinnant's date algorithms.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_shape() {
        let s = iso8601_now();
        assert_eq!(s.len(), 20);
        assert!(s.starts_with("20") && s.ends_with('Z'));
    }

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
}
