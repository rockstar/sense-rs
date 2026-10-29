//! Builds the complete HTTP requests handed to an
//! [`HttpTransport`](crate::HttpTransport).

use http::Method;

use super::PROTOCOL_VERSION;
use crate::error::{Error, Result};

const USER_AGENT: &str = concat!("sense-rs/", env!("CARGO_PKG_VERSION"));

pub(crate) fn build_request(
    method: Method,
    url: &str,
    query: &[(&str, String)],
    form: Option<&[(&str, &str)]>,
    access_token: Option<&str>,
) -> Result<http::Request<Vec<u8>>> {
    let mut url = url.to_string();
    if !query.is_empty() {
        url.push('?');
        url.push_str(&urlencode(query));
    }
    let mut req = http::Request::builder()
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
    req.body(body.into_bytes())
        .map_err(|e| Error::InvalidArgument(format!("can't build request: {e}")))
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
            b"email=a%2Bb%40example.com&password=p%26ss%3D+word"
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
        assert_eq!(req.headers()["X-Sense-Protocol"], PROTOCOL_VERSION);
        assert_eq!(req.headers()["User-Agent"], USER_AGENT);
        assert!(req.headers().get("Content-Type").is_none());
        assert!(req.body().is_empty());
    }
}
