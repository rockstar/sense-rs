/// Errors returned by this crate.
///
/// `Debug` output hides the MFA token.
#[derive(thiserror::Error)]
pub enum Error {
    /// Login succeeded but needs a TOTP code. Pass `mfa_token` to `login_mfa`.
    #[error("multi-factor authentication required")]
    MfaRequired { mfa_token: String },

    /// The client has no session. Call `login` or `set_session` first.
    #[error("not authenticated")]
    NotAuthenticated,

    /// An argument can't be used, e.g. a device ID of `..`.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The API returned a non-success status.
    #[error("sense api error ({status}): {}", reason.as_deref().unwrap_or("no reason given"))]
    Api { status: u16, reason: Option<String> },

    /// The realtime feed received nothing, not even a pong, for this long,
    /// or the handshake took too long. Open a new feed.
    #[error("realtime feed timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// The realtime feed sent an `error` message.
    #[error("realtime feed error: {0}")]
    Realtime(String),

    #[error(transparent)]
    Http(#[from] reqwest::Error),

    /// The blocking client couldn't start its Tokio runtime.
    #[error("failed to start runtime: {0}")]
    Runtime(#[source] std::io::Error),

    #[error(transparent)]
    WebSocket(#[from] Box<tokio_tungstenite::tungstenite::Error>),

    #[error("failed to decode response: {0}")]
    Decode(String),
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MfaRequired { .. } => f
                .debug_struct("MfaRequired")
                .field("mfa_token", &"<redacted>")
                .finish(),
            Error::NotAuthenticated => f.write_str("NotAuthenticated"),
            Error::InvalidArgument(s) => f.debug_tuple("InvalidArgument").field(s).finish(),
            Error::Api { status, reason } => f
                .debug_struct("Api")
                .field("status", status)
                .field("reason", reason)
                .finish(),
            Error::Timeout(d) => f.debug_tuple("Timeout").field(d).finish(),
            Error::Realtime(s) => f.debug_tuple("Realtime").field(s).finish(),
            Error::Http(e) => f.debug_tuple("Http").field(e).finish(),
            Error::Runtime(e) => f.debug_tuple("Runtime").field(e).finish(),
            Error::WebSocket(e) => f.debug_tuple("WebSocket").field(e).finish(),
            Error::Decode(s) => f.debug_tuple("Decode").field(s).finish(),
        }
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::WebSocket(Box::new(e))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_hides_mfa_token() {
        let err = Error::MfaRequired {
            mfa_token: "secret-mfa".into(),
        };
        let out = format!("{err:?}");
        assert!(!out.contains("secret"), "{out}");
        assert!(out.contains("MfaRequired"), "{out}");
    }
}
