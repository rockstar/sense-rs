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
        let req = build_request(method, url, query, form, access_token)?;
        let mut resp = self.0.send(req).await.map_err(Error::Http)?;
        let status = resp.status();
        let body = resp.body_mut().contents().await.map_err(Error::Http)?;
        Ok((status, body.to_vec()))
    }
}

fn build_request(
    method: Method,
    url: &str,
    query: &[(&str, String)],
    form: Option<&[(&str, &str)]>,
    access_token: Option<&str>,
) -> Result<Request<String>> {
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
    req.body(body).map_err(|e| Error::Http(e.into()))
}

fn urlencode<V: AsRef<str>>(pairs: &[(&str, V)]) -> String {
    form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn login_form() {
        let req = build_request(
            Method::POST,
            "https://api.sense.com/apiservice/api/v1/authenticate",
            &[],
            Some(&[("email", "a+b@example.com"), ("password", "p&ss= word")]),
            None,
        )
        .unwrap();
        assert_eq!(
            req.uri(),
            "https://api.sense.com/apiservice/api/v1/authenticate"
        );
        assert_eq!(
            req.headers()["Content-Type"],
            "application/x-www-form-urlencoded"
        );
        assert!(req.headers().get("Authorization").is_none());
        assert_eq!(
            req.body(),
            "email=a%2Bb%40example.com&password=p%26ss%3D+word"
        );
    }

    #[test]
    fn query_and_token() {
        let req = build_request(
            Method::GET,
            "https://api.sense.com/apiservice/api/v1/app/history/trends",
            &[
                ("monitor_id", "1".to_string()),
                ("start", "2026-10-23T00:00:00+00:00".to_string()),
            ],
            None,
            Some("tok"),
        )
        .unwrap();
        assert_eq!(
            req.uri(),
            "https://api.sense.com/apiservice/api/v1/app/history/trends?monitor_id=1&start=2026-10-23T00%3A00%3A00%2B00%3A00"
        );
        assert_eq!(req.headers()["Authorization"], "bearer tok");
        assert!(req.headers().get("Content-Type").is_none());
        assert_eq!(req.body(), "");
    }
}
