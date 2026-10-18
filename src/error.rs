/// Errors returned by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Login succeeded but needs a TOTP code. Pass `mfa_token` to `login_mfa`.
    #[error("multi-factor authentication required")]
    MfaRequired { mfa_token: String },

    /// The client has no session. Call `login` or `set_session` first.
    #[error("not authenticated")]
    NotAuthenticated,

    /// The API returned a non-success status.
    #[error("sense api error ({status}): {}", reason.as_deref().unwrap_or("no reason given"))]
    Api { status: u16, reason: Option<String> },

    /// The realtime feed sent an `error` message.
    #[error("realtime feed error: {0}")]
    Realtime(String),

    #[error(transparent)]
    Http(#[from] reqwest::Error),

    #[error(transparent)]
    WebSocket(#[from] Box<tokio_tungstenite::tungstenite::Error>),

    #[error("failed to decode response: {0}")]
    Decode(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::WebSocket(Box::new(e))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
