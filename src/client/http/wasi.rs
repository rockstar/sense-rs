use http::{Method, StatusCode};
use wstd::http::{Client, Request};

use super::USER_AGENT;
use crate::client::PROTOCOL_VERSION;
use crate::error::{Error, Result};

/// No timeouts are set: hosts that don't support wasi-http's timeout
/// options fail every request that sets one. The host's own limits apply.
pub(crate) struct Http(Client);

impl Http {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self(Client::new()))
    }

    pub(crate) async fn request(
        &self,
        method: Method,
        url: &str,
        query: &[(&str, String)],
        form: Option<&[(&str, &str)]>,
        access_token: Option<&str>,
    ) -> Result<(StatusCode, Vec<u8>)> {
        let mut url = url.to_string();
        if !query.is_empty() {
            url.push('?');
            url.push_str(&urlencode(query));
        }
        let mut req = Request::builder()
            .method(method)
            .uri(url)
            .header("User-Agent", USER_AGENT)
            .header("X-Sense-Protocol", PROTOCOL_VERSION);
        if let Some(token) = access_token {
            req = req.header("Authorization", format!("bearer {token}"));
        }
        let body = match form {
            Some(form) => {
                req = req.header("Content-Type", "application/x-www-form-urlencoded");
                urlencode(form)
            }
            None => String::new(),
        };
        let req = req.body(body).map_err(|e| Error::Http(e.into()))?;
        let mut resp = self.0.send(req).await.map_err(Error::Http)?;
        let status = resp.status();
        let body = resp.body_mut().contents().await.map_err(Error::Http)?;
        Ok((status, body.to_vec()))
    }
}

fn urlencode<V: AsRef<str>>(pairs: &[(&str, V)]) -> String {
    form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}
