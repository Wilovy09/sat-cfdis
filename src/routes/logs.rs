//! Admin-only access to this process's pm2 log files.
//!
//! GET /api/v1/admin/logs?stream=both&lines=500
//!
//! pm2 splits a process's output into two files, /root/.pm2/logs/<name>-out.log and
//! <name>-error.log -- "out" is stdout (all tracing output lands here, since
//! tracing_subscriber::fmt() writes every level to stdout, not just genuine errors) and
//! "error" is stderr (panics, anything the process writes there directly). `stream` picks
//! which pm2 file to read, not a tracing-level filter.

use actix_web::{HttpRequest, HttpResponse, web};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::{config::Config, errors::AppError};

pub type DbPool = crate::db::DbPool;

const PM2_LOG_DIR: &str = "/root/.pm2/logs";
const DEFAULT_LINES: usize = 500;
const MAX_LINES: usize = 5000;
const TAIL_CHUNK_SIZE: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Admin auth — same pattern as routes/queue.rs (this codebase keeps a private copy per
// admin-only module rather than a shared helper; following that convention here).
// ---------------------------------------------------------------------------

fn bearer_token(req: &HttpRequest) -> Option<String> {
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

fn jwt_user_id(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(payload))
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    json.get("id")
        .or_else(|| json.get("sub"))?
        .as_str()
        .map(|s| s.to_string())
}

/// A per-user admin JWT only means something inside the environment that issued it -- a
/// person who's admin in prod's `public.users`/`user_roles` has a *different* id (and no
/// row at all) in test's, since each environment provisions accounts independently. That
/// makes a personal JWT useless for a tool like adquiere-logs that aggregates logs across
/// environments: whichever environment issued the token, every OTHER environment's DB
/// lookup fails, regardless of whether the person really is an admin everywhere.
///
/// `ADMIN_LOGS_KEY` is the fix: a shared secret configured identically here and in
/// adquiere-logs. It authorizes the aggregator itself (a trusted service, already gating
/// humans at its own login) rather than re-deriving admin status from a token whose
/// identity this environment's DB was never going to recognize. The per-user JWT path
/// below is untouched and still works exactly as before for anyone hitting this endpoint
/// directly with their own token.
fn admin_logs_key_matches(req: &HttpRequest, expected: Option<&str>) -> bool {
    let Some(expected) = expected else {
        return false;
    };
    req.headers()
        .get("x-admin-logs-key")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|got| got == expected)
}

async fn require_admin(req: &HttpRequest, pool: &DbPool, cfg: &Config) -> Result<(), AppError> {
    if admin_logs_key_matches(req, cfg.admin_logs_key.as_deref()) {
        return Ok(());
    }

    let token = bearer_token(req).ok_or_else(|| AppError::unauthorized("Token requerido"))?;
    let user_id = jwt_user_id(&token).ok_or_else(|| AppError::unauthorized("Token inválido"))?;
    let is_admin = crate::db::users::is_user_admin(pool, &user_id)
        .await
        .unwrap_or(false);
    if !is_admin {
        return Err(AppError::forbidden("Acceso denegado"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tail — read the last `n` lines of a file without loading it in full. pm2 log files
// accumulate indefinitely across restarts; a plain read_to_string would re-read the whole
// history on every request once a file grows past a few MB.
// ---------------------------------------------------------------------------

/// Reads at most the last `n` lines from `path`, oldest first (same order as `tail -n`).
/// Reads backward in fixed-size chunks from EOF until `n` newlines are found or the start
/// of the file is reached, instead of loading the whole file into memory.
fn tail_lines(path: &Path, n: usize) -> std::io::Result<Vec<String>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len == 0 || n == 0 {
        return Ok(Vec::new());
    }

    let mut pos = file_len;
    let mut buf: Vec<u8> = Vec::new();
    let mut newline_count = 0usize;

    // "> n" not ">= n": the last line has no trailing newline before EOF, so we need one
    // more newline than requested lines to be sure we've captured a full n-th line.
    while pos > 0 && newline_count <= n {
        let read_size = TAIL_CHUNK_SIZE.min(pos as usize);
        pos -= read_size as u64;
        file.seek(SeekFrom::Start(pos))?;
        let mut chunk = vec![0u8; read_size];
        file.read_exact(&mut chunk)?;
        newline_count += chunk.iter().filter(|&&b| b == b'\n').count();
        chunk.extend_from_slice(&buf);
        buf = chunk;
    }

    let text = String::from_utf8_lossy(&buf);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(n);
    Ok(lines[start..].iter().map(|s| s.to_string()).collect())
}

#[derive(Serialize)]
struct LogFile {
    path: String,
    lines: Vec<String>,
    /// Set instead of failing the whole request -- a missing/unreadable file on one stream
    /// (wrong PM2_APP_NAME, process not running under pm2, permissions) shouldn't hide the
    /// other stream's logs, and the message itself is the fastest way to diagnose which.
    error: Option<String>,
}

fn read_stream(app_name: &str, suffix: &str, n: usize) -> LogFile {
    let path = PathBuf::from(PM2_LOG_DIR).join(format!("{app_name}-{suffix}.log"));
    let path_str = path.display().to_string();
    match tail_lines(&path, n) {
        Ok(lines) => LogFile {
            path: path_str,
            lines,
            error: None,
        },
        Err(e) => LogFile {
            path: path_str,
            lines: Vec::new(),
            error: Some(e.to_string()),
        },
    }
}

#[derive(Deserialize)]
pub struct LogsQuery {
    /// "out" | "error" | "both" -- which pm2 file(s) to read.
    #[serde(default = "default_stream")]
    stream: String,
    #[serde(default = "default_lines")]
    lines: usize,
}

fn default_stream() -> String {
    "both".to_string()
}

fn default_lines() -> usize {
    DEFAULT_LINES
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/logs",
    tag = "Admin",
    params(
        ("stream" = Option<String>, Query, description = "out | error | both (default both)"),
        ("lines" = Option<usize>, Query, description = "Líneas por archivo, desde el final (default 500, máx 5000)"),
    ),
    responses(
        (status = 200, description = "Últimas líneas de los logs de pm2 (stdout/stderr)"),
        (status = 400, description = "stream inválido"),
        (status = 403, description = "Solo administradores"),
    )
)]
#[tracing::instrument(skip(pool, cfg, query))]
pub async fn get_logs(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    query: web::Query<LogsQuery>,
) -> Result<HttpResponse, AppError> {
    require_admin(&req, pool.get_ref(), cfg.get_ref()).await?;

    let n = query.lines.clamp(1, MAX_LINES);
    let stream = query.stream.as_str();
    if !matches!(stream, "out" | "error" | "both") {
        return Err(AppError::bad_request(
            "stream debe ser 'out', 'error' o 'both'",
        ));
    }

    let out =
        (stream == "out" || stream == "both").then(|| read_stream(&cfg.pm2_app_name, "out", n));
    let error =
        (stream == "error" || stream == "both").then(|| read_stream(&cfg.pm2_app_name, "error", n));

    Ok(HttpResponse::Ok().json(serde_json::json!({
        "app_name": cfg.pm2_app_name,
        "lines_requested": n,
        "out": out,
        "error": error,
    })))
}

#[cfg(test)]
mod tail_lines_tests {
    use super::*;
    use std::io::Write;

    fn write_lines(lines: &[&str]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        f.flush().unwrap();
        f
    }

    #[test]
    fn returns_every_line_when_file_has_fewer_than_n() {
        let f = write_lines(&["a", "b", "c"]);
        assert_eq!(tail_lines(f.path(), 10).unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn returns_only_the_last_n_lines_in_original_order() {
        let f = write_lines(&["1", "2", "3", "4", "5"]);
        assert_eq!(tail_lines(f.path(), 2).unwrap(), vec!["4", "5"]);
    }

    #[test]
    fn n_zero_returns_nothing() {
        let f = write_lines(&["a", "b"]);
        assert!(tail_lines(f.path(), 0).unwrap().is_empty());
    }

    #[test]
    fn empty_file_returns_nothing() {
        let f = tempfile::NamedTempFile::new().unwrap();
        assert!(tail_lines(f.path(), 10).unwrap().is_empty());
    }

    #[test]
    fn works_across_a_chunk_boundary() {
        // TAIL_CHUNK_SIZE is 64KiB -- force at least two backward-read iterations so the
        // buffer-prepend logic (not just a single-chunk read) is exercised.
        let lines: Vec<String> = (0..5000).map(|i| format!("line-{i:05}")).collect();
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        let f = write_lines(&refs);
        let tail = tail_lines(f.path(), 3).unwrap();
        assert_eq!(tail, vec!["line-04997", "line-04998", "line-04999"]);
    }

    #[test]
    fn missing_file_is_an_error_not_a_panic() {
        assert!(tail_lines(Path::new("/does/not/exist.log"), 10).is_err());
    }
}

#[cfg(test)]
mod admin_logs_key_tests {
    use super::admin_logs_key_matches;
    use actix_web::test::TestRequest;

    #[test]
    fn no_configured_key_never_matches_even_with_a_header() {
        let req = TestRequest::default()
            .insert_header(("x-admin-logs-key", "anything"))
            .to_http_request();
        assert!(!admin_logs_key_matches(&req, None));
    }

    #[test]
    fn missing_header_does_not_match_a_configured_key() {
        let req = TestRequest::default().to_http_request();
        assert!(!admin_logs_key_matches(&req, Some("secret")));
    }

    #[test]
    fn wrong_header_value_does_not_match() {
        let req = TestRequest::default()
            .insert_header(("x-admin-logs-key", "wrong"))
            .to_http_request();
        assert!(!admin_logs_key_matches(&req, Some("secret")));
    }

    #[test]
    fn exact_match_succeeds() {
        let req = TestRequest::default()
            .insert_header(("x-admin-logs-key", "secret"))
            .to_http_request();
        assert!(admin_logs_key_matches(&req, Some("secret")));
    }
}
