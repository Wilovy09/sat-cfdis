//! Admin-only snapshot of this server's resources, for Heartbeat's "system" view.
//!
//! GET /api/v1/admin/system
//!
//! Same auth as `/api/v1/admin/logs` (`X-Admin-Logs-Key` or an admin JWT). Answers the JSON
//! Heartbeat expects: CPU, memory, swap, disks, the busiest processes and uptime.
//!
//! CPU usage is a difference between two readings, so the sampler keeps one `System`
//! alive between requests: each answer is the average since the previous call (Heartbeat
//! calls every `SYSTEM_INTERVAL_SECS`, 30 s by default), with no sleep inside the request.

use actix_web::{HttpRequest, HttpResponse, web};
use serde::Serialize;
use std::sync::{Mutex, PoisonError};
use sysinfo::{Disks, ProcessRefreshKind, ProcessesToUpdate, System};

use crate::{config::Config, errors::AppError, routes::logs::require_admin};

pub type DbPool = crate::db::DbPool;

/// Heartbeat keeps up to 16 disks and 20 processes.
const MAX_DISKS: usize = 16;
const MAX_PROCESSES: usize = 20;

/// Filesystems that aren't real storage. Snap's `squashfs` images are always 100% "full"
/// and would trip a disk alert; the rest are memory-backed or virtual.
const PSEUDO_FILESYSTEMS: &[&str] = &[
    "squashfs",
    "tmpfs",
    "devtmpfs",
    "overlay",
    "proc",
    "sysfs",
    "cgroup",
    "cgroup2",
    "devpts",
    "efivarfs",
    "fuse.lxcfs",
    "nsfs",
    "ramfs",
    "autofs",
    "tracefs",
    "debugfs",
];

/// The long-lived `System` the CPU and per-process deltas are measured against.
pub struct SystemSampler(Mutex<System>);

impl SystemSampler {
    /// Takes a first reading, so the first request already has a baseline to diff against.
    #[must_use]
    pub fn new() -> Self {
        let mut sys = System::new();
        sys.refresh_cpu_usage();
        sys.refresh_processes_specifics(ProcessesToUpdate::All, true, process_kind());
        Self(Mutex::new(sys))
    }

    fn snapshot(&self) -> SystemSnapshot {
        let mut sys = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        sys.refresh_cpu_usage();
        sys.refresh_memory();
        sys.refresh_processes_specifics(ProcessesToUpdate::All, true, process_kind());

        let load = System::load_average();
        let mut processes: Vec<ProcessInfo> = sys
            .processes()
            .values()
            .map(|p| ProcessInfo {
                pid: p.pid().as_u32(),
                name: p.name().to_string_lossy().into_owned(),
                cpu_pct: round1(p.cpu_usage()),
                memory_bytes: p.memory(),
            })
            .collect();
        // Busiest first, like `top`; memory breaks ties among idle processes.
        processes.sort_by(|a, b| {
            b.cpu_pct
                .total_cmp(&a.cpu_pct)
                .then(b.memory_bytes.cmp(&a.memory_bytes))
        });
        processes.truncate(MAX_PROCESSES);

        SystemSnapshot {
            cpu: CpuInfo {
                usage_pct: round1(sys.global_cpu_usage()),
                cores: sys.cpus().len(),
                load: [load.one, load.five, load.fifteen],
            },
            memory: MemoryInfo {
                total_bytes: sys.total_memory(),
                used_bytes: sys.used_memory(),
                available_bytes: sys.available_memory(),
            },
            swap: SwapInfo {
                total_bytes: sys.total_swap(),
                used_bytes: sys.used_swap(),
            },
            disks: disks(),
            processes,
            uptime_secs: System::uptime(),
        }
    }
}

impl Default for SystemSampler {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory and CPU per process only; threads (`tasks`) would list every worker thread.
fn process_kind() -> ProcessRefreshKind {
    ProcessRefreshKind::nothing().with_memory().with_cpu()
}

fn round1(value: f32) -> f64 {
    (f64::from(value) * 10.0).round() / 10.0
}

/// Real disks, one per device (a device mounted twice is counted once).
fn disks() -> Vec<DiskInfo> {
    let mut seen_devices = Vec::new();
    Disks::new_with_refreshed_list()
        .list()
        .iter()
        .filter(|d| {
            let fs = d.file_system().to_string_lossy();
            d.total_space() > 0 && !PSEUDO_FILESYSTEMS.contains(&fs.as_ref())
        })
        .filter(|d| {
            let device = d.name().to_string_lossy().into_owned();
            if seen_devices.contains(&device) {
                false
            } else {
                seen_devices.push(device);
                true
            }
        })
        .take(MAX_DISKS)
        .map(|d| DiskInfo {
            mount: d.mount_point().display().to_string(),
            total_bytes: d.total_space(),
            used_bytes: d.total_space().saturating_sub(d.available_space()),
        })
        .collect()
}

#[derive(Serialize)]
struct SystemSnapshot {
    cpu: CpuInfo,
    memory: MemoryInfo,
    swap: SwapInfo,
    disks: Vec<DiskInfo>,
    processes: Vec<ProcessInfo>,
    uptime_secs: u64,
}

#[derive(Serialize)]
struct CpuInfo {
    usage_pct: f64,
    cores: usize,
    load: [f64; 3],
}

#[derive(Serialize)]
struct MemoryInfo {
    total_bytes: u64,
    used_bytes: u64,
    available_bytes: u64,
}

#[derive(Serialize)]
struct SwapInfo {
    total_bytes: u64,
    used_bytes: u64,
}

#[derive(Serialize)]
struct DiskInfo {
    mount: String,
    total_bytes: u64,
    used_bytes: u64,
}

#[derive(Serialize)]
struct ProcessInfo {
    pid: u32,
    name: String,
    /// Percent of one core, like `top` (can exceed 100 on multi-core).
    cpu_pct: f64,
    memory_bytes: u64,
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/system",
    tag = "Admin",
    responses(
        (status = 200, description = "CPU, memoria, swap, discos, procesos y uptime del servidor"),
        (status = 403, description = "Solo administradores"),
    )
)]
#[tracing::instrument(skip(req, pool, cfg, sampler))]
pub async fn get_system(
    req: HttpRequest,
    pool: web::Data<DbPool>,
    cfg: web::Data<Config>,
    sampler: web::Data<SystemSampler>,
) -> Result<HttpResponse, AppError> {
    require_admin(&req, pool.get_ref(), cfg.get_ref()).await?;
    // Reading /proc for every process is blocking work: keep it off the async workers.
    let snapshot = web::block(move || sampler.snapshot())
        .await
        .map_err(|e| AppError::internal(format!("No se pudo leer el sistema: {e}")))?;
    Ok(HttpResponse::Ok().json(snapshot))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_has_the_fields_heartbeat_requires() {
        let sampler = SystemSampler::new();
        let json = serde_json::to_value(sampler.snapshot()).unwrap();
        assert!(json["cpu"]["usage_pct"].is_number());
        assert!(json["memory"]["total_bytes"].as_u64().unwrap() > 0);
        assert!(json["memory"]["used_bytes"].is_u64());
        assert!(json["disks"].as_array().unwrap().len() <= MAX_DISKS);
        assert!(json["processes"].as_array().unwrap().len() <= MAX_PROCESSES);
        assert!(json["uptime_secs"].as_u64().unwrap() > 0);
    }

    #[test]
    fn pseudo_filesystems_are_left_out() {
        for disk in disks() {
            assert!(!disk.mount.is_empty());
            assert!(disk.used_bytes <= disk.total_bytes);
        }
    }

    #[test]
    fn round1_keeps_one_decimal() {
        assert!((round1(23.456) - 23.5).abs() < f64::EPSILON);
        assert!((round1(0.04) - 0.0).abs() < f64::EPSILON);
    }
}
