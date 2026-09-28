use actix_web::HttpResponse;
use serde_json::json;

// ---------------------------------------------------------------------------
// XML date extraction helper
// ---------------------------------------------------------------------------

/// Scan raw CFDI XML bytes and extract the path components for storage.
///
/// Returns `(rfc_emisor, rfc_receptor, year, month, day)`.
/// Falls back to `"UNKNOWN"` for RFCs and current UTC date on parse failure.
pub(crate) fn extract_cfdi_path_info(bytes: &[u8]) -> (String, String, u32, u32, u32) {
    // Find first occurrence of `tag` in bytes, then look for `attr="` within the
    // following `window` bytes and return the value up to the closing `"`.
    fn find_attr(bytes: &[u8], tag: &[u8], attr: &[u8], window: usize) -> Option<String> {
        let pos = bytes.windows(tag.len()).position(|w| w == tag)?;
        let region_end = (pos + window).min(bytes.len());
        let region = &bytes[pos..region_end];
        let a = region.windows(attr.len()).position(|w| w == attr)?;
        let val_start = a + attr.len();
        let end = region[val_start..].iter().position(|&b| b == b'"')?;
        std::str::from_utf8(&region[val_start..val_start + end])
            .ok()
            .map(|s| s.to_uppercase())
    }

    let rfc_emisor =
        find_attr(bytes, b"Emisor", b"Rfc=\"", 300).unwrap_or_else(|| "UNKNOWN".into());
    let rfc_receptor =
        find_attr(bytes, b"Receptor", b"Rfc=\"", 300).unwrap_or_else(|| "UNKNOWN".into());

    // Extract Fecha="YYYY-MM-DD
    let fecha_needle = b"Fecha=\"";
    let (year, month, day) = bytes
        .windows(fecha_needle.len())
        .position(|w| w == fecha_needle)
        .and_then(|pos| {
            let s = pos + fecha_needle.len();
            if bytes.len() < s + 10 {
                return None;
            }
            let y = std::str::from_utf8(&bytes[s..s + 4])
                .ok()?
                .parse::<u32>()
                .ok()?;
            let m = std::str::from_utf8(&bytes[s + 5..s + 7])
                .ok()?
                .parse::<u32>()
                .ok()?;
            let d = std::str::from_utf8(&bytes[s + 8..s + 10])
                .ok()?
                .parse::<u32>()
                .ok()?;
            if (1..=12).contains(&m) && (1..=31).contains(&d) {
                Some((y, m, d))
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            let secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let days = secs / 86400;
            let year = 1970u32 + (days / 365) as u32;
            let month = ((days % 365) / 30 + 1).min(12) as u32;
            (year, month, 1)
        });

    (rfc_emisor, rfc_receptor, year, month, day)
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

#[utoipa::path(
    get,
    path = "/health",
    tag = "Health",
    responses((status = 200, description = "Servicio activo"))
)]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(json!({ "status": "ok" }))
}
