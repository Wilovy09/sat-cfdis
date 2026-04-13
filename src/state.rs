use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::oneshot;

/// Shared map of pending captcha challenges.
///
/// Key:   session_id (UUID string) — sent to the browser as part of the SSE captcha event.
/// Value: oneshot sender — the SSE handler awaits the receiver; the solve endpoint sends the answer.
pub type CaptchaMap = Mutex<HashMap<String, oneshot::Sender<String>>>;
