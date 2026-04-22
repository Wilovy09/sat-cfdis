use base64::Engine as _;
use serde::{Deserialize, Serialize};

/// Authentication credentials sent by the client in each request body.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Auth {
    /// FIEL (Firma Electrónica Avanzada) – no captcha required.
    Fiel {
        /// Base64-encoded DER certificate (.cer file contents)
        certificate: String,
        /// Base64-encoded DER private key (.key file contents)
        private_key: String,
        /// Password used to protect the private key
        password: String,
    },
    /// CIEC (Clave de Identificación Electrónica Confidencial) – requires captcha resolver.
    Ciec {
        rfc: String,
        password: String,
        /// Optional BoxFactura AI API key for captcha resolution (overrides env var)
        captcha_api_key: Option<String>,
    },
}

impl Auth {
    /// Extract the RFC from auth credentials.
    /// CIEC: taken directly from the field.
    /// FIEL: parsed from the certificate's Subject serialNumber (OID 2.5.4.5).
    pub fn rfc(&self) -> Option<String> {
        match self {
            Auth::Ciec { rfc, .. } => Some(rfc.to_uppercase()),
            Auth::Fiel { certificate, .. } => rfc_from_cert_b64(certificate),
        }
    }
}

/// Parse RFC from a base64-encoded DER X.509 certificate.
/// SAT encodes the RFC in the Subject's serialNumber field as "/ RFC123456789".
fn rfc_from_cert_b64(b64: &str) -> Option<String> {
    let der = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;

    // Scan the DER bytes for the UTF-8 string that contains the RFC pattern.
    // SAT puts the RFC in Subject serialNumber: " / XAXX010101000"
    let text = String::from_utf8_lossy(&der);
    for part in text.split('/') {
        let candidate = part.trim();
        // RFC pattern: 3-4 letters + 6 digits + 3 alphanumeric (homoclave)
        if candidate.len() >= 12 && candidate.len() <= 13 {
            let bytes = candidate.as_bytes();
            let letters = bytes
                .iter()
                .take(4)
                .filter(|b| b.is_ascii_alphabetic())
                .count();
            if letters >= 3 {
                return Some(candidate.to_uppercase());
            }
        }
    }
    None
}
