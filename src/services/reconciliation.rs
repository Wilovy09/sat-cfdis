//! Pulso_Propuestas_Integridad_Descarga_RFC.md 3.1/3.2.2/3.4/3.5: whether an RFC's download
//! is complete is decided by evidence, month by month and direction by direction -- the
//! SAT's own count (`list-count`) against what Pulso holds -- not by the status of jobs.
//!
//! The worker here is the "vigilante": every cycle, for each watched RFC (active, verified
//! owner) with no job in flight, it
//! 1. refreshes what Pulso holds per closed month x direction (`db::reconciliation`);
//! 2. acts on months already counted: equal and every XML present -> verified; fewer in
//!    Pulso -> a job targeted at that month x direction, then a fresh count; more in
//!    Pulso -> count again before believing either side. After `MAX_ATTEMPTS` the month is
//!    flagged for internal review and the team is alerted -- the rest of the RFC goes on;
//! 3. counts the next stretch of months that need it, in one SAT session.
//!
//! A month is "closed" `CLOSE_GRACE_DAYS` after it ends (late stamping, up to 72 h, and
//! cancellations settle first); a verified month is counted again every
//! `RECHECK_AFTER_DAYS`. Release is exact: equal to the SAT, no margin (decision UX-5).

use crate::{
    config::Config,
    db::{self, DbPool, reconciliation::MonthRow},
    services::crypto,
};
use aws_sdk_s3::Client as S3Client;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

const POLL_SECS: u64 = 1800;
/// Days after a month ends before its SAT count is taken as final.
pub const CLOSE_GRACE_DAYS: i32 = 5;
/// Age of a verified month's count before it is re-checked (quarterly pass, 3.5).
const RECHECK_AFTER_DAYS: i32 = 90;
/// Targeted jobs / recounts per month x direction before it goes to internal review.
const MAX_ATTEMPTS: i32 = 3;
/// Months counted per SAT session (one `list-count` process, both directions).
const COUNT_CHUNK_MONTHS: usize = 12;
/// Wait after an RFC's last job activity before comparing, so the ETL has loaded it.
const SETTLE_SECS: u64 = 15 * 60;
/// Same idle bound as the download worker: a SAT session silent this long is dead.
const PHP_IDLE_TIMEOUT_SECS: u64 = 300;

/// RFCs a SAT counting session is open for right now. The download worker
/// (`resume_worker`) skips their jobs meanwhile -- two concurrent logins for one RFC make
/// the SAT reset the second session.
pub type CountingRfcs = Arc<Mutex<HashSet<String>>>;

// ---------------------------------------------------------------------------
// SAT count (`list-count`)
// ---------------------------------------------------------------------------

/// The final line of a `list-count` run.
#[derive(Debug, Default)]
pub struct SatCount {
    pub total: i64,
    /// ("YYYY-MM", "emitidos"|"recibidos") -> CFDIs the SAT lists.
    pub by_month: BTreeMap<(String, String), i64>,
    /// Same key -> days the SAT didn't answer; that month's number isn't trustworthy.
    pub error_days: HashMap<(String, String), i64>,
    /// Set when the SAT rejected the login and the count stopped.
    pub auth_error: Option<String>,
}

impl SatCount {
    fn from_line(line: &serde_json::Value) -> Option<Self> {
        line.get("__count__")?;
        let split = |key: &str| {
            let (ym, dir) = key.split_once('|')?;
            Some((ym.to_string(), dir.to_string()))
        };
        let map = |field: &str| -> Vec<((String, String), i64)> {
            line[field]
                .as_object()
                .map(|o| {
                    o.iter()
                        .filter_map(|(k, v)| Some((split(k)?, v.as_i64()?)))
                        .collect()
                })
                .unwrap_or_default()
        };
        Some(Self {
            total: line["total"].as_i64().unwrap_or(0),
            by_month: map("by_month").into_iter().collect(),
            error_days: map("error_days").into_iter().collect(),
            auth_error: line["auth_error"].as_str().map(str::to_string),
        })
    }

    /// Stores every counted month x direction for `rfc` (untrusted ones as attempts only).
    pub async fn record(&self, pool: &DbPool, rfc: &str) {
        if self.auth_error.is_some() {
            return;
        }
        for ((ym, dir), n) in &self.by_month {
            let errors = self
                .error_days
                .get(&(ym.clone(), dir.clone()))
                .copied()
                .unwrap_or(0);
            if let Err(e) = db::reconciliation::record_count(pool, rfc, ym, dir, *n, errors).await {
                tracing::error!(rfc = %rfc, ym = %ym, "Reconciliation: record_count failed: {e}");
            }
        }
    }
}

/// Where a `list-count` run reports progress while it walks the days.
pub struct CountProgress<'a> {
    pub pool: &'a DbPool,
    pub job_id: &'a str,
}

/// Runs `list-count` over `[period_from, period_to]` (only whole calendar months are
/// counted). `login_ok` fires on the scraper's first accepted SAT login. Returns `None`
/// when the process produced no final count (crash, idle timeout, spawn failure).
pub async fn run_list_count(
    cfg: &Config,
    auth: &serde_json::Value,
    period_from: &str,
    period_to: &str,
    dl_type: &str,
    progress: Option<CountProgress<'_>>,
    mut login_ok: Option<tokio::sync::oneshot::Sender<()>>,
) -> Option<SatCount> {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

    let payload = serde_json::json!({
        "command": "list-count",
        "auth":    auth,
        "params": {
            "period_from":   period_from,
            "period_to":     period_to,
            "download_type": dl_type,
        }
    });
    let mut input = serde_json::to_vec(&payload).ok()?;
    input.push(b'\n');

    let mut cmd = tokio::process::Command::new(&cfg.php_bin);
    cmd.arg(&cfg.php_cli_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(ref proxy) = cfg.https_proxy {
        cmd.env("HTTPS_PROXY", proxy).env("https_proxy", proxy);
    }
    let mut child = cmd.spawn().ok()?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(&input).await;
    }
    let mut lines = tokio::io::BufReader::new(child.stdout.take()?).lines();

    let mut result = None;
    loop {
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(PHP_IDLE_TIMEOUT_SECS),
            lines.next_line(),
        )
        .await;
        let line = match next {
            Ok(Ok(Some(line))) => line,
            Ok(Ok(None) | Err(_)) => break,
            Err(_) => {
                tracing::error!("list-count: PHP worker idle timeout, killing");
                let _ = child.start_kill();
                break;
            }
        };
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if val.get("__auth_ok__").is_some() {
            if let Some(tx) = login_ok.take() {
                let _ = tx.send(());
            }
            continue;
        }
        if val.get("__keepalive__").is_some() {
            if let Some(p) = &progress {
                let total_so_far = val["total_so_far"].as_i64().unwrap_or(0);
                let date = val["date"].as_str().unwrap_or("?");
                let _ = db::jobs::update_count_progress(p.pool, p.job_id, total_so_far, date).await;
            }
            continue;
        }
        if let Some(count) = SatCount::from_line(&val) {
            result = Some(count);
            break;
        }
    }
    let _ = child.wait().await;
    result
}

// ---------------------------------------------------------------------------
// Months
// ---------------------------------------------------------------------------

/// (year, month) of a "YYYY-MM" string.
fn parse_ym(ym: &str) -> Option<(i32, u32)> {
    Some((ym.get(0..4)?.parse().ok()?, ym.get(5..7)?.parse().ok()?))
}

fn format_ym(year: i32, month: u32) -> String {
    format!("{year:04}-{month:02}")
}

fn last_day(year: i32, month: u32) -> u32 {
    match month {
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// The latest month that is closed on `today` ("YYYY-MM-DD"): ended at least
/// `CLOSE_GRACE_DAYS` ago.
pub fn last_closed_month(today: &str) -> Option<String> {
    let (y, m) = parse_ym(today)?;
    let day: u32 = today.get(8..10)?.parse().ok()?;
    // The previous month closes on day CLOSE_GRACE_DAYS + 1 of this one.
    let back = if day > CLOSE_GRACE_DAYS.unsigned_abs() {
        1
    } else {
        2
    };
    let abs = i64::from(y) * 12 + i64::from(m) - 1 - back;
    Some(format_ym(
        i32::try_from(abs.div_euclid(12)).ok()?,
        u32::try_from(abs.rem_euclid(12) + 1).ok()?,
    ))
}

/// The months to count next: the oldest due month and those after it, up to
/// `COUNT_CHUNK_MONTHS` (both directions are counted in the same pass anyway).
fn next_count_chunk(rows: &[MonthRow]) -> Option<(String, String)> {
    let mut due: Vec<&str> = rows
        .iter()
        .filter(|r| r.count_due && !r.needs_review)
        .map(|r| r.ym.as_str())
        .collect();
    due.sort_unstable();
    due.dedup();
    let first = *due.first()?;
    let last = *due.get(COUNT_CHUNK_MONTHS.min(due.len()) - 1)?;
    let (y1, m1) = parse_ym(first)?;
    let (y2, m2) = parse_ym(last)?;
    Some((
        format!("{y1:04}-{m1:02}-01 00:00:00"),
        format!("{y2:04}-{m2:02}-{:02} 23:59:59", last_day(y2, m2)),
    ))
}

/// What a counted, unverified month needs next.
#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Verified,
    /// Counts agree but some XML is still missing (xml_redownload's job) -- wait.
    WaitingForXml,
    Resync,
    Recount,
    Review,
    /// Not counted yet (or its last count had unanswered days).
    Uncounted,
}

fn verdict(row: &MonthRow) -> Verdict {
    let Some(sat) = row.sat_count else {
        return if row.recount_attempts >= MAX_ATTEMPTS {
            Verdict::Review
        } else {
            Verdict::Uncounted
        };
    };
    let held = row.pulso_count.unwrap_or(0);
    match held.cmp(&sat) {
        std::cmp::Ordering::Equal if row.pulso_xml_missing.unwrap_or(0) == 0 => Verdict::Verified,
        std::cmp::Ordering::Equal => Verdict::WaitingForXml,
        std::cmp::Ordering::Less if row.resync_attempts < MAX_ATTEMPTS => Verdict::Resync,
        std::cmp::Ordering::Greater if row.recount_attempts < MAX_ATTEMPTS => Verdict::Recount,
        _ => Verdict::Review,
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

pub async fn worker(pool: DbPool, cfg: Arc<Config>, s3: Arc<S3Client>, counting: CountingRfcs) {
    // After the download and gap workers have had their own first pass.
    tokio::time::sleep(std::time::Duration::from_secs(300)).await;
    loop {
        match db::reconciliation::watched_rfcs(&pool).await {
            Ok(rfcs) => {
                for watched in rfcs {
                    if let Err(e) = reconcile_rfc(&pool, &cfg, &s3, &counting, &watched).await {
                        tracing::error!(rfc = %watched.rfc, "Reconciliation error: {e}");
                    }
                }
            }
            Err(e) => tracing::error!("Reconciliation: watched_rfcs failed: {e}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(POLL_SECS)).await;
    }
}

fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    crate::services::gap_detector::ymd_from_epoch_secs(secs)
}

async fn reconcile_rfc(
    pool: &DbPool,
    cfg: &Arc<Config>,
    s3: &Arc<S3Client>,
    counting: &CountingRfcs,
    watched: &db::reconciliation::WatchedRfc,
) -> anyhow::Result<()> {
    let rfc = watched.rfc.as_str();
    let Some(to_ym) = last_closed_month(&today_utc()) else {
        return Ok(());
    };
    let from_ym = format_ym(watched.anio_piso, 1);
    if from_ym > to_ym {
        return Ok(());
    }
    // Data may still be landing: wait until no job is in flight and the last one settled.
    if db::jobs::get_active_for_rfc(pool, rfc).await?.is_some() {
        return Ok(());
    }
    if let Some(last) = db::reconciliation::last_job_activity(pool, rfc).await?
        && last > db::jobs::utc_ago(SETTLE_SECS)
    {
        return Ok(());
    }

    db::reconciliation::sync_rows(pool, rfc, &from_ym, &to_ym).await?;
    let rows = db::reconciliation::rows(
        pool,
        rfc,
        &from_ym,
        &to_ym,
        CLOSE_GRACE_DAYS,
        RECHECK_AFTER_DAYS,
    )
    .await?;

    let mut queued_jobs = false;
    // A count taken before the month closed (e.g. a download job's pre-pass) isn't final
    // yet: it gets counted again instead of being compared.
    for row in rows
        .iter()
        .filter(|r| !r.verified && !r.needs_review && !(r.count_due && r.sat_count.is_some()))
    {
        match verdict(row) {
            Verdict::Verified => {
                db::reconciliation::mark_verified(pool, rfc, &row.ym, &row.direction).await?;
            }
            Verdict::Resync => {
                queue_month_job(pool, cfg, s3, watched, row).await?;
                db::reconciliation::mark_resync_queued(pool, rfc, &row.ym, &row.direction).await?;
                queued_jobs = true;
            }
            Verdict::Recount => {
                db::reconciliation::mark_recount(pool, rfc, &row.ym, &row.direction).await?;
            }
            Verdict::Review => {
                db::reconciliation::mark_needs_review(pool, rfc, &row.ym, &row.direction).await?;
                crate::services::email::alert_team(
                    cfg,
                    &format!("Pulso: {rfc} {} {} no cuadra contra el SAT", row.ym, row.direction),
                    &format!(
                        "El mes {} ({}) del RFC {rfc} no cuadra después de {MAX_ATTEMPTS} intentos automáticos: SAT {:?}, Pulso {:?}, XML faltantes {:?}. Requiere revisión; el resto del RFC sigue su curso.",
                        row.ym, row.direction, row.sat_count, row.pulso_count, row.pulso_xml_missing
                    ),
                )
                .await;
            }
            Verdict::WaitingForXml | Verdict::Uncounted => {}
        }
    }
    // The targeted jobs run first; this RFC is counted again once their data is in.
    if queued_jobs {
        return Ok(());
    }

    let rows = db::reconciliation::rows(
        pool,
        rfc,
        &from_ym,
        &to_ym,
        CLOSE_GRACE_DAYS,
        RECHECK_AFTER_DAYS,
    )
    .await?;
    let Some((period_from, period_to)) = next_count_chunk(&rows) else {
        return Ok(());
    };
    count_months(pool, cfg, s3, counting, watched, &period_from, &period_to).await;
    Ok(())
}

/// The RFC's current credentials as a scraper auth payload: its FIEL when it has one (the
/// owner is verified, so `try_fiel_auth` allows it), else its stored CIEC.
async fn resolve_auth(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    watched: &db::reconciliation::WatchedRfc,
) -> Option<(serde_json::Value, Option<tempfile::TempDir>)> {
    let bucket = cfg.s3_bucket.clone().unwrap_or_default();
    if let Some((fiel, tmp)) = crate::try_fiel_auth(pool, s3, &bucket, &watched.rfc).await {
        return Some((fiel, Some(tmp)));
    }
    match crypto::decrypt(&crypto::load_key(), &watched.clave_enc) {
        Ok(clave) => Some((
            serde_json::json!({ "type": "ciec", "rfc": watched.rfc, "password": clave }),
            None,
        )),
        Err(e) => {
            tracing::error!(rfc = %watched.rfc, "Reconciliation: decrypt failed: {e}");
            None
        }
    }
}

async fn count_months(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    counting: &CountingRfcs,
    watched: &db::reconciliation::WatchedRfc,
    period_from: &str,
    period_to: &str,
) {
    let rfc = watched.rfc.as_str();
    // Claim the RFC first, then re-check for jobs: resume_worker checks the claim before
    // starting a job, so no download can begin for this RFC from here on.
    let claimed = counting
        .lock()
        .map(|mut set| set.insert(rfc.to_string()))
        .unwrap_or(false);
    if !claimed {
        return;
    }
    let busy = !matches!(db::jobs::get_active_for_rfc(pool, rfc).await, Ok(None));
    if !busy && let Some((auth, _fiel_tmp)) = resolve_auth(pool, cfg, s3, watched).await {
        tracing::info!(rfc = %rfc, from = %period_from, to = %period_to, "Reconciliation: counting months against the SAT");
        match run_list_count(cfg, &auth, period_from, period_to, "ambas", None, None).await {
            Some(count) if count.auth_error.is_some() => {
                // The download jobs' own retry/confirmation flow owns credential problems.
                tracing::warn!(rfc = %rfc, code = ?count.auth_error, "Reconciliation: SAT login rejected during count");
            }
            Some(count) => count.record(pool, rfc).await,
            None => tracing::warn!(rfc = %rfc, "Reconciliation: list-count produced no result"),
        }
    }
    if let Ok(mut set) = counting.lock() {
        set.remove(rfc);
    }
}

/// Queues a `list` job for exactly one month x direction that Pulso is short on.
async fn queue_month_job(
    pool: &DbPool,
    cfg: &Config,
    s3: &S3Client,
    watched: &db::reconciliation::WatchedRfc,
    row: &MonthRow,
) -> anyhow::Result<()> {
    let Some((y, m)) = parse_ym(&row.ym) else {
        return Ok(());
    };
    let Some((auth, _fiel_tmp)) = resolve_auth(pool, cfg, s3, watched).await else {
        return Ok(());
    };
    let auth_type = auth["type"].as_str().unwrap_or("ciec").to_string();
    let key = crypto::load_key();
    let auth_enc = crypto::encrypt(&key, &auth.to_string()).map_err(|e| anyhow::anyhow!(e))?;
    let job_id = db::jobs::insert_queued(
        pool,
        "list",
        &watched.rfc,
        &auth_type,
        &auth_enc,
        &row.direction,
        &format!("{y:04}-{m:02}-01 00:00:00"),
        &format!("{y:04}-{m:02}-{:02} 23:59:59", last_day(y, m)),
    )
    .await?;
    tracing::warn!(
        rfc = %watched.rfc, ym = %row.ym, direction = %row.direction, job_id = %job_id,
        sat = ?row.sat_count, pulso = ?row.pulso_count,
        "Reconciliation: Pulso short of the SAT count, queued a job for the month"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Completeness (sync-status)
// ---------------------------------------------------------------------------

/// What the user is told about an RFC's download (UX-1..UX-4).
#[derive(Debug, Serialize)]
pub struct Completeness {
    /// "liberado" | "incompleto" | "requiere_accion"
    pub state: &'static str,
    /// Share of closed month x direction pairs verified against the SAT.
    pub progress_pct: f64,
    /// Years whose every month x direction is verified (UX-2's green pills).
    pub verified_years: Vec<i32>,
    /// The SAT rejected the stored credential and every automatic retry (UX-4).
    pub credential_rejected: bool,
}

impl Completeness {
    /// An RFC whose owner hasn't passed the SAT login yet: nothing verified, no progress.
    pub fn unverified(credential_rejected: bool) -> Self {
        Self::from_rows(&[], "", "", credential_rejected)
    }

    /// `[from_ym, to_ym]` is the range that must be verified: a closed month without a row
    /// yet counts as pending, not as absent.
    fn from_rows(rows: &[MonthRow], from_ym: &str, to_ym: &str, credential_rejected: bool) -> Self {
        let expected = match (parse_ym(from_ym), parse_ym(to_ym)) {
            (Some((fy, fm)), Some((ty, tm))) => {
                let months =
                    (i64::from(ty) * 12 + i64::from(tm)) - (i64::from(fy) * 12 + i64::from(fm)) + 1;
                usize::try_from(months.max(0) * 2).unwrap_or(0)
            }
            _ => 0,
        };
        let total = expected.max(rows.len());
        let verified = rows.iter().filter(|r| r.verified).count();
        let mut per_year: BTreeMap<i32, (usize, usize)> = BTreeMap::new();
        for r in rows {
            if let Some((y, _)) = parse_ym(&r.ym) {
                let e = per_year.entry(y).or_default();
                e.0 += 1;
                e.1 += usize::from(r.verified);
            }
        }
        // A year needs all 12 months closed (December included) and every one verified.
        let last_full_year = parse_ym(to_ym).map(|(y, m)| if m == 12 { y } else { y - 1 });
        let verified_years = per_year
            .into_iter()
            .filter(|&(y, (n, v))| n == 24 && v == n && last_full_year.is_some_and(|l| y <= l))
            .map(|(y, _)| y)
            .collect();
        #[allow(clippy::cast_precision_loss)]
        let progress_pct = if total == 0 {
            0.0
        } else {
            (verified as f64 / total as f64 * 1000.0).round() / 10.0
        };
        let state = if credential_rejected {
            "requiere_accion"
        } else if total > 0 && verified == total {
            "liberado"
        } else {
            "incompleto"
        };
        Self {
            state,
            progress_pct,
            verified_years,
            credential_rejected,
        }
    }
}

/// The completeness of `rfc` as of today. Fails closed to "incompleto" with no progress.
pub async fn completeness(pool: &DbPool, rfc: &str, credential_rejected: bool) -> Completeness {
    let empty = Completeness::unverified(credential_rejected);
    let Some(to_ym) = last_closed_month(&today_utc()) else {
        return empty;
    };
    let Ok(Some(anio_piso)) = db::users::get_anio_piso_for_rfc(pool, rfc).await else {
        return empty;
    };
    let from_ym = format_ym(anio_piso, 1);
    match db::reconciliation::rows(
        pool,
        rfc,
        &from_ym,
        &to_ym,
        CLOSE_GRACE_DAYS,
        RECHECK_AFTER_DAYS,
    )
    .await
    {
        Ok(rows) => Completeness::from_rows(&rows, &from_ym, &to_ym, credential_rejected),
        Err(e) => {
            tracing::error!(rfc = %rfc, "completeness: {e}");
            empty
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ym: &str, sat: Option<i32>, held: i32) -> MonthRow {
        MonthRow {
            ym: ym.to_string(),
            direction: "emitidos".to_string(),
            sat_count: sat,
            pulso_count: Some(held),
            pulso_xml_missing: Some(0),
            verified: false,
            resync_attempts: 0,
            recount_attempts: 0,
            needs_review: false,
            count_due: sat.is_none(),
        }
    }

    #[test]
    fn last_closed_month_waits_for_the_grace_days() {
        assert_eq!(last_closed_month("2026-10-05").as_deref(), Some("2026-08"));
        assert_eq!(last_closed_month("2026-10-06").as_deref(), Some("2026-09"));
        assert_eq!(last_closed_month("2026-01-20").as_deref(), Some("2025-12"));
        assert_eq!(last_closed_month("2026-01-03").as_deref(), Some("2025-11"));
    }

    #[test]
    fn verdicts_follow_the_exact_rule() {
        assert_eq!(verdict(&row("2026-01", Some(10), 10)), Verdict::Verified);
        assert_eq!(verdict(&row("2026-01", Some(10), 9)), Verdict::Resync);
        assert_eq!(verdict(&row("2026-01", Some(10), 11)), Verdict::Recount);
        assert_eq!(verdict(&row("2026-01", None, 0)), Verdict::Uncounted);
        let mut missing_xml = row("2026-01", Some(10), 10);
        missing_xml.pulso_xml_missing = Some(1);
        assert_eq!(verdict(&missing_xml), Verdict::WaitingForXml);
        let mut exhausted = row("2026-01", Some(10), 9);
        exhausted.resync_attempts = MAX_ATTEMPTS;
        assert_eq!(verdict(&exhausted), Verdict::Review);
    }

    #[test]
    fn count_chunk_starts_at_the_oldest_due_month() {
        let rows = [
            row("2024-01", Some(1), 1),
            row("2024-02", None, 0),
            row("2024-03", None, 0),
        ];
        assert_eq!(
            next_count_chunk(&rows),
            Some((
                "2024-02-01 00:00:00".to_string(),
                "2024-03-31 23:59:59".to_string()
            ))
        );
        assert_eq!(next_count_chunk(&rows[..1]), None);
    }

    #[test]
    fn sat_count_line_parses_months_errors_and_auth() {
        let line = serde_json::json!({
            "__count__": true, "total": 7,
            "by_month": {"2024-01|emitidos": 5, "2024-01|recibidos": 2},
            "error_days": {"2024-01|recibidos": 1},
            "auth_error": null
        });
        let c = SatCount::from_line(&line).unwrap();
        assert_eq!(c.total, 7);
        assert_eq!(c.by_month[&("2024-01".into(), "emitidos".into())], 5);
        assert_eq!(c.error_days[&("2024-01".into(), "recibidos".into())], 1);
        assert!(c.auth_error.is_none());
        assert!(SatCount::from_line(&serde_json::json!({"__keepalive__": true})).is_none());
    }

    #[test]
    fn completeness_pills_need_a_whole_verified_year() {
        let mut rows = Vec::new();
        for m in 1..=12 {
            for dir in ["emitidos", "recibidos"] {
                let mut r = row(&format_ym(2024, m), Some(1), 1);
                r.direction = dir.to_string();
                r.verified = true;
                rows.push(r);
            }
        }
        let mut open = row("2025-01", None, 0);
        open.verified = false;
        rows.push(open);
        let c = Completeness::from_rows(&rows, "2024-01", "2025-01", false);
        assert_eq!(c.state, "incompleto");
        assert_eq!(c.verified_years, vec![2024]);
        assert!(c.progress_pct > 90.0 && c.progress_pct < 100.0);
        assert_eq!(
            Completeness::from_rows(&rows[..24], "2024-01", "2024-12", false).state,
            "liberado"
        );
        // A closed month with no row yet is pending, not absent.
        assert_eq!(
            Completeness::from_rows(&rows[..24], "2024-01", "2025-01", false).state,
            "incompleto"
        );
        assert_eq!(
            Completeness::from_rows(&rows[..24], "2024-01", "2024-12", true).state,
            "requiere_accion"
        );
    }
}
