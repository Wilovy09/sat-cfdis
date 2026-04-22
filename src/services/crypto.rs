//! AES-256-GCM encryption for storing auth credentials in the queue DB.
//!
//! Key source (priority order):
//!   1. `QUEUE_SECRET_KEY` env var — 64 hex chars (32 bytes)
//!   2. `.pulso_queue_key` file in the working directory — auto-generated on first use
//!
//! Ciphertext format: base64( nonce[12] || ciphertext || tag[16] )
//! The nonce is randomly generated per encryption, so the same plaintext
//! produces different ciphertexts — safe to store in the DB.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use base64::Engine as _;

const KEY_FILE: &str = ".pulso_queue_key";

/// Load or generate the 32-byte encryption key.
pub fn load_key() -> [u8; 32] {
    // 1. Env var
    if let Ok(hex_key) = std::env::var("QUEUE_SECRET_KEY") {
        if let Ok(bytes) = hex::decode(hex_key.trim()) {
            if bytes.len() == 32 {
                let mut key = [0u8; 32];
                key.copy_from_slice(&bytes);
                return key;
            }
        }
        tracing::warn!(
            "QUEUE_SECRET_KEY is set but not 64 valid hex chars — falling back to file key"
        );
    }

    // 2. Key file
    if let Ok(content) = std::fs::read_to_string(KEY_FILE) {
        if let Ok(bytes) = hex::decode(content.trim()) {
            if bytes.len() == 32 {
                let mut key = [0u8; 32];
                key.copy_from_slice(&bytes);
                return key;
            }
        }
    }

    // 3. Generate + persist
    use rand::RngCore as _;
    let mut key = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut key);
    let hex_key = hex::encode(key);
    if let Err(e) = std::fs::write(KEY_FILE, &hex_key) {
        tracing::error!("Could not write queue key file {KEY_FILE}: {e}");
    } else {
        tracing::info!("Generated new queue encryption key → {KEY_FILE}");
    }
    key
}

/// Encrypt `plaintext` with the global key.
/// Returns base64-encoded `nonce || ciphertext_with_tag`.
pub fn encrypt(key: &[u8; 32], plaintext: &str) -> Result<String, String> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 12 bytes
    let ct = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| e.to_string())?;

    let mut blob = Vec::with_capacity(12 + ct.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&ct);

    Ok(base64::engine::general_purpose::STANDARD.encode(&blob))
}

/// Decrypt a value produced by [`encrypt`].
pub fn decrypt(key: &[u8; 32], b64: &str) -> Result<String, String> {
    let blob = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| e.to_string())?;

    if blob.len() < 12 {
        return Err("Ciphertext too short".to_string());
    }

    let (nonce_bytes, ct) = blob.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| e.to_string())?;
    let plaintext = cipher
        .decrypt(nonce, ct)
        .map_err(|_| "Decryption failed — wrong key or corrupted data".to_string())?;

    String::from_utf8(plaintext).map_err(|e| e.to_string())
}
