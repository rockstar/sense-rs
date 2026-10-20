use std::time::Duration;

use http::{Method, StatusCode};
use reqwest::header::{HeaderMap, HeaderValue};

use super::USER_AGENT;
use crate::client::{CONNECT_TIMEOUT, PROTOCOL_VERSION};
use crate::error::Result;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Http(reqwest::Client);

impl Http {
    pub(crate) fn new() -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Sense-Protocol",
            HeaderValue::from_static(PROTOCOL_VERSION),
        );
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .default_headers(headers)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()?;
        Ok(Self(client))
    }

    pub(crate) async fn request(
        &self,
        method: Method,
        url: &str,
        query: &[(&str, String)],
        form: Option<&[(&str, &str)]>,
        access_token: Option<&str>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let mut req = self.0.request(method, url).query(query);
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
