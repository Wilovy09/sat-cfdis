use super::auth::Auth;
use serde::Deserialize;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum DownloadType {
    #[default]
    Emitidos,
    Recibidos,
    Ambos,
    #[serde(rename = "ambas")]
    Ambas,
}

impl DownloadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            DownloadType::Emitidos => "emitidos",
            DownloadType::Recibidos => "recibidos",
            DownloadType::Ambos | DownloadType::Ambas => "ambos",
        }
    }
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ResourceType {
    #[default]
    Xml,
    Pdf,
}

impl ResourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResourceType::Xml => "xml",
            ResourceType::Pdf => "pdf",
        }
    }

    pub fn mime_type(&self) -> &'static str {
        match self {
            ResourceType::Xml => "application/xml",
            ResourceType::Pdf => "application/pdf",
        }
    }
}

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ListRequest {
    pub auth: Auth,
    /// Start of the date range, e.g. "2024-01-01"
    pub period_from: Option<String>,
    /// End of the date range, e.g. "2024-01-31"
    pub period_to: Option<String>,
    /// List specific UUIDs instead of a date range
    pub uuids: Option<Vec<String>>,
    #[serde(default)]
    pub download_type: DownloadType,
    /// Filter by voucher state: "vigentes", "cancelados", or "todos" (default)
    pub state: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DownloadRequest {
    pub auth: Auth,
    pub uuids: Vec<String>,
    #[serde(default)]
    pub download_type: DownloadType,
    #[serde(default)]
    pub resource_type: ResourceType,
}
