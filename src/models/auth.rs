use serde::Deserialize;

/// Authentication credentials sent by the client in each request body.
#[derive(Debug, Deserialize)]
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
