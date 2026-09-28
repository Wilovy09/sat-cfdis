//! L18-02: the single verified-session entry point. Before this, seven places (one per
//! module: billing, queue, users, fiel, logs, analytics, plus auth.rs's three Google
//! routes) each carried their own copy of a function that split a JWT on '.', base64-
//! decoded the middle segment and read `id`/`sub` straight out of the JSON -- no signature
//! check at all. Anyone could hand-build a token claiming to be any user, admin included.
//!
//! Two issuers, two keys, both HS256 (confirmed against the Adquiere API's own source --
//! `@nestjs/jwt`'s `JwtModule.register({ secret: process.env.JWT_SECRET, signOptions: {
//! expiresIn: '1h' } })`, no `algorithm` override means the jsonwebtoken/NestJS default,
//! HS256 -- and its session claims shape for /pulso/register and /pulso/sessions:
//! `{id, email, roles, name, phone, is_onboarding_completed, iat, exp}`. Confirmed
//! separately, without printing either value, that Adquiere's real JWT_SECRET and Pulso's
//! own are NOT the same secret -- verifying Adquiere-issued tokens needs its own key,
//! configured here as ADQUIERE_JWT_SECRET, not reused from JWT_SECRET.):
//! - A token Pulso itself signed (Google login, `routes::auth::make_jwt`) verifies against
//!   `cfg.jwt_secret`.
//! - A token the Adquiere API signed (email register/login, forwarded through
//!   `routes::auth::register`/`login`) verifies against `cfg.adquiere_jwt_secret`, if
//!   configured. Unconfigured = that issuer's tokens are rejected outright, not silently
//!   trusted -- an operator who hasn't set the shared secret yet gets every email-login
//!   session bounced with 401, not a security hole.
//!
//! `Validation::new(Algorithm::HS256)`'s defaults require `exp` and check it -- a token
//! with no expiration, or an expired one, is rejected before either secret is even tried
//! twice. This is intentionally the ONLY place in the codebase allowed to decode a bearer
//! token's claims; see `jwt_sub` in routes/auth.rs for the two narrow, declared exceptions
//! (reading the Adquiere API's own response right after login, and Google's id_token from
//! its direct server-to-server exchange) that don't authenticate a request and so don't
//! belong here.

use actix_web::HttpRequest;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;

use crate::{config::Config, errors::AppError};

#[derive(Debug, Deserialize)]
struct SessionClaims {
    #[serde(alias = "sub")]
    id: String,
}

pub fn bearer_token(req: &HttpRequest) -> Option<String> {
    let header = req
        .headers()
        .get(actix_web::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let lower = header.to_lowercase();
    let token = header[lower.find("bearer ")? + 7..].trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

fn try_decode(token: &str, secret: &str) -> Option<String> {
    let validation = Validation::new(Algorithm::HS256);
    decode::<SessionClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .ok()
    .map(|data| data.claims.id)
}

/// Verifies a bearer token's signature and expiration against both known issuers, and
/// returns the authenticated user id. Every 401 from here carries the same text --
/// L18-02 point 3 -- the frontend logs out and redirects to login on any 401 regardless
/// of wording, so there's nothing to lose by not distinguishing "missing" from "invalid"
/// from "expired" and something to gain: it tells an attacker nothing about which check
/// failed.
pub fn require_session(req: &HttpRequest, cfg: &Config) -> Result<String, AppError> {
    let unauthorized = || AppError::unauthorized("Token inválido o expirado");

    let token = bearer_token(req).ok_or_else(unauthorized)?;

    if let Some(id) = try_decode(&token, &cfg.jwt_secret) {
        return Ok(id);
    }
    if let Some(adquiere_secret) = &cfg.adquiere_jwt_secret
        && let Some(id) = try_decode(&token, adquiere_secret)
    {
        return Ok(id);
    }
    Err(unauthorized())
}

#[cfg(test)]
mod l18_02_tests {
    use super::*;
    use actix_web::test::TestRequest;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::json;

    const SECRET_A: &str = "this-is-a-32-character-test-secret-a!!";
    const SECRET_B: &str = "this-is-a-different-test-secret-b!!!!!";

    fn sign(secret: &str, claims: serde_json::Value) -> String {
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn future_exp() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            + 3600
    }

    fn cfg_with_secrets() -> Config {
        let mut cfg = Config::from_env();
        cfg.jwt_secret = SECRET_A.to_string();
        cfg.adquiere_jwt_secret = Some(SECRET_B.to_string());
        cfg
    }

    fn request_with(token: &str) -> HttpRequest {
        TestRequest::default()
            .insert_header(("authorization", format!("Bearer {token}")))
            .to_http_request()
    }

    #[test]
    fn accepts_a_token_signed_with_pulsos_own_secret() {
        let cfg = cfg_with_secrets();
        let token = sign(SECRET_A, json!({"id": "user-1", "exp": future_exp()}));
        assert_eq!(
            require_session(&request_with(&token), &cfg).unwrap(),
            "user-1"
        );
    }

    #[test]
    fn accepts_a_token_signed_with_adquieres_secret() {
        // L18-02: the whole point of trying both keys -- a token from the *other* issuer
        // must work exactly like one from Pulso's own.
        let cfg = cfg_with_secrets();
        let token = sign(SECRET_B, json!({"id": "user-2", "exp": future_exp()}));
        assert_eq!(
            require_session(&request_with(&token), &cfg).unwrap(),
            "user-2"
        );
    }

    #[test]
    fn falls_back_to_sub_when_id_is_absent() {
        let cfg = cfg_with_secrets();
        let token = sign(SECRET_A, json!({"sub": "user-3", "exp": future_exp()}));
        assert_eq!(
            require_session(&request_with(&token), &cfg).unwrap(),
            "user-3"
        );
    }

    #[test]
    fn rejects_a_token_whose_signature_was_altered() {
        // The exact failure mode this whole item closes: before L18-02, this token's
        // claims would have been read and trusted with no signature check at all.
        let cfg = cfg_with_secrets();
        let token = sign(SECRET_A, json!({"id": "attacker", "exp": future_exp()}));
        let mut tampered = token.clone();
        let last = tampered.pop().unwrap();
        tampered.push(if last == 'A' { 'B' } else { 'A' });
        assert!(require_session(&request_with(&tampered), &cfg).is_err());
    }

    #[test]
    fn rejects_a_token_with_no_expiration_at_all() {
        let cfg = cfg_with_secrets();
        let token = sign(SECRET_A, json!({"id": "user-4"}));
        assert!(require_session(&request_with(&token), &cfg).is_err());
    }

    #[test]
    fn rejects_an_expired_token() {
        let cfg = cfg_with_secrets();
        let token = sign(SECRET_A, json!({"id": "user-5", "exp": 1}));
        assert!(require_session(&request_with(&token), &cfg).is_err());
    }

    #[test]
    fn rejects_a_token_from_neither_configured_issuer() {
        let cfg = cfg_with_secrets();
        let token = sign(
            "some-unrelated-secret-nobody-configured",
            json!({"id": "user-6", "exp": future_exp()}),
        );
        assert!(require_session(&request_with(&token), &cfg).is_err());
    }

    #[test]
    fn adquiere_secret_unconfigured_means_that_issuers_tokens_are_rejected_not_trusted() {
        let mut cfg = cfg_with_secrets();
        cfg.adquiere_jwt_secret = None;
        let token = sign(SECRET_B, json!({"id": "user-7", "exp": future_exp()}));
        assert!(require_session(&request_with(&token), &cfg).is_err());
    }

    #[test]
    fn rejects_a_missing_authorization_header() {
        let cfg = cfg_with_secrets();
        let req = TestRequest::default().to_http_request();
        assert!(require_session(&req, &cfg).is_err());
    }
}
