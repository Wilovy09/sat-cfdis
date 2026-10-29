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
        if let Ok(bytes) = hex::decode(hex_key.trim())
            && bytes.len() == 32
        {
            let mut key = [0u8; 32];
            key.copy_from_slice(&bytes);
            return key;
        }
        tracing::warn!(
            "QUEUE_SECRET_KEY is set but not 64 valid hex chars — falling back to file key"
        );
    }

    // 2. Key file
    if let Ok(content) = std::fs::read_to_string(KEY_FILE)
        && let Ok(bytes) = hex::decode(content.trim())
        && bytes.len() == 32
    {
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        return key;
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

/// Marks a stored file as encrypted by [`encrypt_file`]. FIEL files uploaded before files
/// were encrypted have no marker and are returned as-is by [`decrypt_file`].
const FILE_MARKER: &[u8] = b"pulso-enc-v1:";

/// Encrypts a binary file (a FIEL `.cer`/`.key`) for storage.
pub fn encrypt_file(key: &[u8; 32], data: &[u8]) -> Result<Vec<u8>, String> {
    let b64 = base64::engine::general_purpose::STANDARD.encode(data);
    let mut out = FILE_MARKER.to_vec();
    out.extend_from_slice(encrypt(key, &b64)?.as_bytes());
    Ok(out)
}

/// Reverses [`encrypt_file`]; legacy unencrypted files pass through unchanged.
pub fn decrypt_file(key: &[u8; 32], stored: &[u8]) -> Result<Vec<u8>, String> {
    let Some(blob) = stored.strip_prefix(FILE_MARKER) else {
        return Ok(stored.to_vec());
    };
    let blob = std::str::from_utf8(blob).map_err(|e| e.to_string())?;
    base64::engine::general_purpose::STANDARD
        .decode(decrypt(key, blob)?)
        .map_err(|e| e.to_string())
}

/// Opaque, unguessable token for a public URL (32 random bytes, hex).
#[must_use]
pub fn random_token() -> String {
    use rand::RngCore as _;
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// What a public token is looked up by: the token itself is never stored in clear.
#[must_use]
pub fn token_hash(token: &str) -> String {
    use sha2::{Digest as _, Sha256};
    hex::encode(Sha256::digest(token.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [7; 32];

    #[test]
    fn files_round_trip_and_are_not_stored_in_clear() {
        let der = b"\x30\x82\x05\x00 fake der bytes";
        let stored = encrypt_file(&KEY, der).unwrap();
        assert!(stored.starts_with(FILE_MARKER));
        assert!(!stored.windows(der.len()).any(|w| w == der));
        assert_eq!(decrypt_file(&KEY, &stored).unwrap(), der);
    }

    #[test]
    fn legacy_unencrypted_files_pass_through() {
        let der = b"\x30\x82 legacy";
        assert_eq!(decrypt_file(&KEY, der).unwrap(), der);
    }

    #[test]
    fn tokens_are_random_and_hash_deterministically() {
        let a = random_token();
        assert_eq!(a.len(), 64);
        assert_ne!(a, random_token());
        assert_eq!(token_hash(&a), token_hash(&a));
        assert_ne!(token_hash(&a), a);
    }
}
