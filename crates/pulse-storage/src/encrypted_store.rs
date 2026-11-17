use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use argon2::Argon2;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as OsResult, UploadPart,
};
use parking_lot::{Mutex, RwLock};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Result, StorageError};

/// v1 headers: the key wrap is not bound to the object path. Still readable, no longer written.
pub const ENVELOPE_MAGIC_V1: [u8; 8] = *b"PLSEAGM1";
/// v2 headers: the object path is part of the wrap AAD, so objects can't be swapped between paths.
pub const ENVELOPE_MAGIC: [u8; 8] = *b"PLSEAGM2";
pub const HEADER_LEN: usize = 100;
pub const CHUNK_PLAIN_LEN: usize = 65_536;
pub const GCM_TAG_LEN: usize = 16;
pub const CHUNK_CIPHER_LEN: usize = CHUNK_PLAIN_LEN + GCM_TAG_LEN;
pub const KEY_ID_LEN: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ByokEncryptionStatus {
    pub enabled: bool,
    pub algorithm: String,
    pub active_key_id: Option<String>,
    pub key_fingerprint: Option<String>,
    pub keyring_size: usize,
    pub encrypted_objects_count: usize,
    pub ops_encrypted: u64,
    pub ops_decrypted: u64,
}

#[derive(Debug, Clone)]
struct EnvelopeHeader {
    key_id: String,
    wrap_nonce: [u8; 12],
    wrapped_dek: [u8; 48],
    obj_nonce_prefix: [u8; 8],
    plaintext_len: u64,
    path_bound: bool,
}

impl EnvelopeHeader {
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0..8].copy_from_slice(&ENVELOPE_MAGIC);
        let kb = self.key_id.as_bytes();
        let copy_len = kb.len().min(KEY_ID_LEN);
        out[8..8 + copy_len].copy_from_slice(&kb[..copy_len]);
        out[24..36].copy_from_slice(&self.wrap_nonce);
        out[36..84].copy_from_slice(&self.wrapped_dek);
        out[84..92].copy_from_slice(&self.obj_nonce_prefix);
        out[92..100].copy_from_slice(&self.plaintext_len.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN {
            return None;
        }
        let path_bound = match &bytes[0..8] {
            m if m == ENVELOPE_MAGIC => true,
            m if m == ENVELOPE_MAGIC_V1 => false,
            _ => return None,
        };
        let raw_kid = &bytes[8..24];
        let end = raw_kid.iter().position(|&b| b == 0).unwrap_or(KEY_ID_LEN);
        let key_id = String::from_utf8(raw_kid[..end].to_vec()).ok()?;
        let mut wrap_nonce = [0u8; 12];
        wrap_nonce.copy_from_slice(&bytes[24..36]);
        let mut wrapped_dek = [0u8; 48];
        wrapped_dek.copy_from_slice(&bytes[36..84]);
        let mut obj_nonce_prefix = [0u8; 8];
        obj_nonce_prefix.copy_from_slice(&bytes[84..92]);
        let mut len_buf = [0u8; 8];
        len_buf.copy_from_slice(&bytes[92..100]);
        let plaintext_len = u64::from_le_bytes(len_buf);
        Some(Self {
            key_id,
            wrap_nonce,
            wrapped_dek,
            obj_nonce_prefix,
            plaintext_len,
            path_bound,
        })
    }

    fn wrap_aad(&self, path: &str) -> Vec<u8> {
        let mut aad = vec![0u8; 32];
        let kb = self.key_id.as_bytes();
        let copy_len = kb.len().min(KEY_ID_LEN);
        aad[0..copy_len].copy_from_slice(&kb[..copy_len]);
        aad[16..24].copy_from_slice(&self.obj_nonce_prefix);
        aad[24..32].copy_from_slice(&self.plaintext_len.to_le_bytes());
        if self.path_bound {
            aad.extend_from_slice(path.as_bytes());
        }
        aad
    }
}

/// Derives or parses a 256-bit key: accepts 64 hex chars directly, or derives from a >=16-char passphrase using Argon2id.
pub fn parse_or_derive_master_key(raw: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(StorageError::Corrupted(
            "encryption key cannot be empty".to_string(),
        ));
    }
    if trimmed.len() == 64 && trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        let decoded = hex::decode(trimmed)
            .map_err(|e| StorageError::Corrupted(format!("invalid hex KMS key: {e}")))?;
        let mut out = [0u8; 32];
        out.copy_from_slice(&decoded);
        return Ok(out);
    }
    if trimmed.len() < 16 {
        return Err(StorageError::Corrupted(
            "encryption key must be a 64-char hex key or an Argon2id passphrase of >= 16 characters"
                .to_string(),
        ));
    }
    let mut out = [0u8; 32];
    Argon2::default()
        .hash_password_into(trimmed.as_bytes(), salt, &mut out)
        .map_err(|e| StorageError::Corrupted(format!("Argon2id key derivation failed: {e}")))?;
    Ok(out)
}

/// Parses a 64-char hex 256-bit master key or derives via Argon2id using a default cluster salt for env convenience.
pub fn parse_byok_master_key(raw: &str) -> Result<[u8; 32]> {
    let default_salt: [u8; 16] = *b"PULSE_BYOK_SALT1";
    parse_or_derive_master_key(raw, &default_salt)
}

pub fn make_key_id(key: &[u8; 32]) -> String {
    let digest = Sha256::digest(key);
    format!("key_{}", &hex::encode(digest)[..12])
}

#[inline]
fn aead_key(key_bytes: &[u8; 32]) -> OsResult<LessSafeKey> {
    let unbound =
        UnboundKey::new(&AES_256_GCM, key_bytes).map_err(|_| os_err("invalid AES key"))?;
    Ok(LessSafeKey::new(unbound))
}

#[inline]
fn os_err(msg: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "EncryptedObjectStore",
        source: Box::new(std::io::Error::other(msg.into())),
    }
}

fn csprng_bytes<const N: usize>() -> OsResult<[u8; N]> {
    let rng = SystemRandom::new();
    let mut buf = [0u8; N];
    rng.fill(&mut buf)
        .map_err(|_| os_err("CSPRNG failure generating nonce/DEK"))?;
    Ok(buf)
}

/// Encrypts `plaintext` with an ephemeral CSPRNG per-object DEK wrapped under `master_key` (`key_id`) using AES-256-GCM.
fn seal_envelope_bytes(
    plaintext: &[u8],
    key_id: &str,
    master_key: &[u8; 32],
    path: &str,
) -> OsResult<(Vec<u8>, EnvelopeHeader)> {
    if key_id.is_empty() || key_id.len() > KEY_ID_LEN {
        return Err(os_err("key_id must be 1..=16 ASCII bytes"));
    }
    let dek: [u8; 32] = csprng_bytes()?;
    let wrap_nonce: [u8; 12] = csprng_bytes()?;
    let obj_nonce_prefix: [u8; 8] = csprng_bytes()?;
    let plaintext_len = plaintext.len() as u64;

    let mut hdr = EnvelopeHeader {
        key_id: key_id.to_string(),
        wrap_nonce,
        wrapped_dek: [0u8; 48],
        obj_nonce_prefix,
        plaintext_len,
        path_bound: true,
    };
    let wrap_aad = hdr.wrap_aad(path);
    let kek = aead_key(master_key)?;
    let mut dek_buf = dek.to_vec();
    kek.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(wrap_nonce),
        Aad::from(&wrap_aad),
        &mut dek_buf,
    )
    .map_err(|_| os_err("AES-256-GCM DEK wrap failed"))?;
    hdr.wrapped_dek.copy_from_slice(&dek_buf);

    let num_chunks = if plaintext.is_empty() {
        0
    } else {
        plaintext.len().div_ceil(CHUNK_PLAIN_LEN)
    };
    let mut out = Vec::with_capacity(HEADER_LEN + plaintext.len() + num_chunks * GCM_TAG_LEN);
    out.extend_from_slice(&hdr.encode());

    let dek_key = aead_key(&dek)?;
    for c_idx in 0..num_chunks {
        let start = c_idx * CHUNK_PLAIN_LEN;
        let end = ((c_idx + 1) * CHUNK_PLAIN_LEN).min(plaintext.len());
        let c_u32 = u32::try_from(c_idx).map_err(|_| os_err("object exceeds max chunk count"))?;

        let mut chunk_nonce = [0u8; 12];
        chunk_nonce[0..8].copy_from_slice(&obj_nonce_prefix);
        chunk_nonce[8..12].copy_from_slice(&c_u32.to_le_bytes());

        let mut chunk_buf = Vec::with_capacity((end - start) + GCM_TAG_LEN);
        chunk_buf.extend_from_slice(&plaintext[start..end]);
        dek_key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(chunk_nonce),
                Aad::from(&chunk_nonce),
                &mut chunk_buf,
            )
            .map_err(|_| os_err("AES-256-GCM chunk seal failed"))?;
        out.extend_from_slice(&chunk_buf);
    }

    Ok((out, hdr))
}

fn unwrap_dek(
    hdr: &EnvelopeHeader,
    keyring: &DashMap<String, [u8; 32]>,
    path: &str,
) -> OsResult<LessSafeKey> {
    let master_key = keyring
        .get(&hdr.key_id)
        .map(|entry| *entry.value())
        .ok_or_else(|| {
            os_err(format!(
                "decryption key '{}' not loaded in keyring",
                hdr.key_id
            ))
        })?;
    let kek = aead_key(&master_key)?;
    let wrap_aad = hdr.wrap_aad(path);
    let mut dek_buf = hdr.wrapped_dek.to_vec();
    let plain_dek = kek
        .open_in_place(
            Nonce::assume_unique_for_key(hdr.wrap_nonce),
            Aad::from(&wrap_aad),
            &mut dek_buf,
        )
        .map_err(|_| {
            os_err("AES-256-GCM DEK unwrap authentication failed (wrong key or tampered header)")
        })?;
    let mut dek = [0u8; 32];
    dek.copy_from_slice(plain_dek);
    aead_key(&dek)
}

fn decrypt_chunk_range(
    hdr: &EnvelopeHeader,
    dek_key: &LessSafeKey,
    first_chunk: usize,
    phys_chunks: &[u8],
    req_start: u64,
    req_end: u64,
) -> OsResult<Vec<u8>> {
    if req_start >= req_end || hdr.plaintext_len == 0 {
        return Ok(Vec::new());
    }
    let last_chunk = ((req_end - 1) as usize) / CHUNK_PLAIN_LEN;
    let total_chunks = (hdr.plaintext_len as usize).div_ceil(CHUNK_PLAIN_LEN);
    let mut out = Vec::with_capacity((req_end - req_start) as usize);

    let mut cursor = 0usize;
    for c_idx in first_chunk..=last_chunk {
        if c_idx >= total_chunks {
            return Err(os_err("encrypted chunk index out of bounds"));
        }
        let chunk_plain_len = if c_idx + 1 == total_chunks {
            let rem = (hdr.plaintext_len as usize) % CHUNK_PLAIN_LEN;
            if rem == 0 {
                CHUNK_PLAIN_LEN
            } else {
                rem
            }
        } else {
            CHUNK_PLAIN_LEN
        };
        let chunk_phys_len = chunk_plain_len + GCM_TAG_LEN;
        if cursor + chunk_phys_len > phys_chunks.len() {
            return Err(os_err("truncated AES-256-GCM ciphertext chunk"));
        }

        let c_u32 = u32::try_from(c_idx).map_err(|_| os_err("chunk index overflow"))?;
        let mut chunk_nonce = [0u8; 12];
        chunk_nonce[0..8].copy_from_slice(&hdr.obj_nonce_prefix);
        chunk_nonce[8..12].copy_from_slice(&c_u32.to_le_bytes());

        let mut buf = phys_chunks[cursor..cursor + chunk_phys_len].to_vec();
        cursor += chunk_phys_len;

        let plain = dek_key
            .open_in_place(
                Nonce::assume_unique_for_key(chunk_nonce),
                Aad::from(&chunk_nonce),
                &mut buf,
            )
            .map_err(|_| {
                os_err(format!(
                    "AES-256-GCM chunk {c_idx} authentication failed (ciphertext tampered or wrong key)"
                ))
            })?;

        let chunk_global_start = (c_idx * CHUNK_PLAIN_LEN) as u64;
        let slice_start = req_start.saturating_sub(chunk_global_start) as usize;
        let slice_end = (req_end - chunk_global_start).min(plain.len() as u64) as usize;
        if slice_start < slice_end {
            out.extend_from_slice(&plain[slice_start..slice_end]);
        }
    }

    Ok(out)
}

/// Seals a raw byte buffer with an ephemeral 256-bit key (used for encrypting local NVMe `foyer_cache` blocks).
pub fn seal_ephemeral_blob(ephemeral_key: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    let nonce: [u8; 12] = csprng_bytes().map_err(StorageError::ObjectStore)?;
    let key = aead_key(ephemeral_key).map_err(StorageError::ObjectStore)?;
    let mut out = Vec::with_capacity(12 + plaintext.len() + GCM_TAG_LEN);
    out.extend_from_slice(&nonce);
    let mut ct = plaintext.to_vec();
    key.seal_in_place_append_tag(
        Nonce::assume_unique_for_key(nonce),
        Aad::from(b"PULSE_NVME_CACHE_V1"),
        &mut ct,
    )
    .map_err(|_| StorageError::Corrupted("ephemeral cache seal failed".into()))?;
    out.extend_from_slice(&ct);
    Ok(out)
}

pub fn unseal_ephemeral_blob(ephemeral_key: &[u8; 32], sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < 12 + GCM_TAG_LEN {
        return Err(StorageError::Corrupted(
            "ephemeral cache entry truncated".into(),
        ));
    }
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&sealed[..12]);
    let key = aead_key(ephemeral_key).map_err(StorageError::ObjectStore)?;
    let mut ct = sealed[12..].to_vec();
    let pt = key
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(b"PULSE_NVME_CACHE_V1"),
            &mut ct,
        )
        .map_err(|_| {
            StorageError::Corrupted("ephemeral cache entry authentication failed".into())
        })?;
    Ok(pt.to_vec())
}

/// Multi-key AES-256-GCM envelope encrypting ObjectStore covering WALs, manifests, identity logs,
/// `.pvtx` columnar segments, and `.plsr` byte-range session replays.
pub struct EncryptedObjectStore {
    inner: Arc<dyn ObjectStore>,
    keyring: Arc<DashMap<String, [u8; 32]>>,
    active_key_id: RwLock<Option<String>>,
    /// Per-object header cache (`Some(hdr)` = PLSEAGM1 encrypted, `None` = plaintext).
    headers: Arc<DashMap<String, Option<EnvelopeHeader>>>,
    ops_encrypted: Arc<AtomicU64>,
    ops_decrypted: Arc<AtomicU64>,
    ephemeral_cache_key: [u8; 32],
}

impl Debug for EncryptedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedObjectStore")
            .field("active_key_id", &self.active_key_id.read())
            .field("keyring_size", &self.keyring.len())
            .finish()
    }
}

impl Display for EncryptedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EncryptedObjectStore(active={:?}, inner={})",
            self.active_key_id.read(),
            self.inner
        )
    }
}

impl EncryptedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        let keyring = Arc::new(DashMap::new());
        let mut active_key_id = None;

        if let Ok(raw) = std::env::var("PULSE_BYOK_MASTER_KEY") {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                if let Ok(key) = parse_byok_master_key(trimmed) {
                    let kid = make_key_id(&key);
                    keyring.insert(kid.clone(), key);
                    active_key_id = Some(kid);
                }
            }
        }
        let ephemeral_cache_key = csprng_bytes().unwrap_or([0xA5; 32]);

        Self {
            inner,
            keyring,
            active_key_id: RwLock::new(active_key_id),
            headers: Arc::new(DashMap::new()),
            ops_encrypted: Arc::new(AtomicU64::new(0)),
            ops_decrypted: Arc::new(AtomicU64::new(0)),
            ephemeral_cache_key,
        }
    }

    pub fn raw_inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }

    pub fn as_object_store(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
        self.clone()
    }

    /// Registers a 256-bit key under `key_id` into the persistent decryption keyring.
    /// When `make_active` is true, new writes encrypt using `key_id` while older `key_id`s remain readable (`C4`).
    pub fn register_master_key(
        &self,
        key_id: &str,
        key: [u8; 32],
        make_active: bool,
    ) -> Result<()> {
        if key_id.is_empty() || key_id.len() > KEY_ID_LEN {
            return Err(StorageError::Corrupted(format!(
                "key_id must be 1..={KEY_ID_LEN} ASCII bytes"
            )));
        }
        self.keyring.insert(key_id.to_string(), key);
        if make_active {
            *self.active_key_id.write() = Some(key_id.to_string());
        }
        Ok(())
    }

    /// Stops encrypting *new* writes while preserving the decryption keyring so all existing objects stay readable (`C3`).
    pub fn disable_new_encryption(&self) {
        *self.active_key_id.write() = None;
    }

    /// Convenience helper: registers `Some(key)` as active encryption key, or stops encrypting new writes on `None`.
    pub fn set_master_key(&self, key: Option<[u8; 32]>) {
        match key {
            Some(k) => {
                let kid = make_key_id(&k);
                let _ = self.register_master_key(&kid, k, true);
            }
            None => {
                self.disable_new_encryption();
            }
        }
    }

    pub fn is_encryption_enabled(&self) -> bool {
        self.active_key_id.read().is_some()
    }

    pub fn seal_cache_blob(&self, data: &[u8]) -> Result<Vec<u8>> {
        if self.is_encryption_enabled() || !self.keyring.is_empty() {
            seal_ephemeral_blob(&self.ephemeral_cache_key, data)
        } else {
            Ok(data.to_vec())
        }
    }

    pub fn unseal_cache_blob(&self, raw: &[u8]) -> Result<Vec<u8>> {
        if self.is_encryption_enabled() || !self.keyring.is_empty() {
            unseal_ephemeral_blob(&self.ephemeral_cache_key, raw)
        } else {
            Ok(raw.to_vec())
        }
    }

    pub fn status(&self) -> ByokEncryptionStatus {
        let active_id = self.active_key_id.read().clone();
        let fp = active_id.as_ref().and_then(|kid| {
            self.keyring.get(kid).map(|entry| {
                let hash = Sha256::digest(entry.value());
                format!("sha256:{}", &hex::encode(hash)[..12])
            })
        });
        let encrypted_objects_count = self.headers.iter().filter(|e| e.value().is_some()).count();
        ByokEncryptionStatus {
            enabled: active_id.is_some(),
            algorithm: "AES-256-GCM (Chunked 64 KiB Envelope)".to_string(),
            active_key_id: active_id,
            key_fingerprint: fp,
            keyring_size: self.keyring.len(),
            encrypted_objects_count,
            ops_encrypted: self.ops_encrypted.load(Ordering::Relaxed),
            ops_decrypted: self.ops_decrypted.load(Ordering::Relaxed),
        }
    }

    fn active_key_pair(&self) -> Option<(String, [u8; 32])> {
        let kid = self.active_key_id.read().clone()?;
        let key = self.keyring.get(&kid).map(|e| *e.value())?;
        Some((kid, key))
    }

    async fn resolve_header(&self, location: &ObjectPath) -> OsResult<Option<EnvelopeHeader>> {
        let loc_str = location.as_ref();
        if let Some(entry) = self.headers.get(loc_str) {
            return Ok(entry.value().clone());
        }
        let probe = match self.inner.get_range(location, 0..HEADER_LEN as u64).await {
            Ok(b) => b,
            Err(object_store::Error::NotFound { path, source }) => {
                return Err(object_store::Error::NotFound { path, source });
            }
            Err(err) => {
                // Some backends return InvalidRange / Generic when object size < HEADER_LEN.
                if let Ok(meta) = self.inner.head(location).await {
                    if meta.size < HEADER_LEN as u64 {
                        self.headers.insert(loc_str.to_string(), None);
                        return Ok(None);
                    }
                }
                return Err(err);
            }
        };
        let parsed = EnvelopeHeader::decode(&probe);
        self.headers.insert(loc_str.to_string(), parsed.clone());
        Ok(parsed)
    }

    /// With `PULSE_ENCRYPTION_STRICT=1` and a keyring loaded, plaintext objects are refused, so
    /// someone with bucket write access can't swap in an unencrypted object. Only enable it when
    /// the store was encrypted from the start and encryption stays on.
    fn check_plaintext_read(&self, location: &ObjectPath) -> OsResult<()> {
        if !self.keyring.is_empty()
            && std::env::var("PULSE_ENCRYPTION_STRICT").as_deref() == Ok("1")
        {
            return Err(os_err(format!(
                "refusing plaintext object '{location}' (PULSE_ENCRYPTION_STRICT=1)"
            )));
        }
        Ok(())
    }

    /// Path-bound objects can't be byte-copied to a new path; decrypt and re-seal for `to`.
    async fn reseal_copy(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        mode: PutMode,
    ) -> OsResult<bool> {
        let Some(hdr) = self.resolve_header(from).await? else {
            return Ok(false);
        };
        if !hdr.path_bound {
            return Ok(false);
        }
        let plain = self.get(from).await?.bytes().await?;
        let (kid, key) = match self.active_key_pair() {
            Some(pair) => pair,
            None => {
                let key = self
                    .keyring
                    .get(&hdr.key_id)
                    .map(|e| *e.value())
                    .ok_or_else(|| {
                        os_err(format!(
                            "decryption key '{}' not loaded in keyring",
                            hdr.key_id
                        ))
                    })?;
                (hdr.key_id.clone(), key)
            }
        };
        let (sealed, new_hdr) = seal_envelope_bytes(&plain, &kid, &key, to.as_ref())?;
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        self.inner
            .put_opts(to, PutPayload::from_bytes(Bytes::from(sealed)), opts)
            .await?;
        self.headers.insert(to.as_ref().to_string(), Some(new_hdr));
        Ok(true)
    }

    async fn translate_meta(&self, mut meta: ObjectMeta) -> ObjectMeta {
        if meta.size >= HEADER_LEN as u64 {
            if let Ok(Some(hdr)) = self.resolve_header(&meta.location).await {
                meta.size = hdr.plaintext_len;
            }
        }
        meta
    }
}

struct EncryptingMultipartUpload {
    inner: Arc<dyn ObjectStore>,
    location: ObjectPath,
    active_key: Option<(String, [u8; 32])>,
    headers: Arc<DashMap<String, Option<EnvelopeHeader>>>,
    ops_encrypted: Arc<AtomicU64>,
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl Debug for EncryptingMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptingMultipartUpload")
            .field("location", &self.location)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for EncryptingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let mut guard = self.buffer.lock();
        for part in data.as_ref() {
            guard.extend_from_slice(part);
        }
        Box::pin(futures::future::ready(Ok(())))
    }

    async fn complete(&mut self) -> OsResult<PutResult> {
        let raw = std::mem::take(&mut *self.buffer.lock());
        if let Some((kid, key)) = &self.active_key {
            let (sealed, hdr) = seal_envelope_bytes(&raw, kid, key, self.location.as_ref())?;
            let res = self
                .inner
                .put(&self.location, PutPayload::from_bytes(Bytes::from(sealed)))
                .await?;
            self.headers
                .insert(self.location.as_ref().to_string(), Some(hdr));
            self.ops_encrypted.fetch_add(1, Ordering::Relaxed);
            Ok(res)
        } else {
            self.headers
                .insert(self.location.as_ref().to_string(), None);
            self.inner
                .put(&self.location, PutPayload::from_bytes(Bytes::from(raw)))
                .await
        }
    }

    async fn abort(&mut self) -> OsResult<()> {
        self.buffer.lock().clear();
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for EncryptedObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if let Some((kid, key)) = self.active_key_pair() {
            let mut raw = Vec::with_capacity(payload.content_length());
            for part in payload.as_ref() {
                raw.extend_from_slice(part);
            }
            let (sealed, hdr) = seal_envelope_bytes(&raw, &kid, &key, location.as_ref())?;
            let res = self
                .inner
                .put_opts(location, PutPayload::from_bytes(Bytes::from(sealed)), opts)
                .await?;
            self.headers
                .insert(location.as_ref().to_string(), Some(hdr));
            self.ops_encrypted.fetch_add(1, Ordering::Relaxed);
            Ok(res)
        } else {
            let res = self.inner.put_opts(location, payload, opts).await?;
            self.headers.insert(location.as_ref().to_string(), None);
            Ok(res)
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Ok(Box::new(EncryptingMultipartUpload {
            inner: self.inner.clone(),
            location: location.clone(),
            active_key: self.active_key_pair(),
            headers: self.headers.clone(),
            ops_encrypted: self.ops_encrypted.clone(),
            buffer: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(&self, location: &ObjectPath, options: GetOptions) -> OsResult<GetResult> {
        // Fast path: resolve header first so range reads only fetch needed chunks.
        if let Some(req_range) = &options.range {
            let Some(hdr) = self.resolve_header(location).await? else {
                self.check_plaintext_read(location)?;
                return self.inner.get_opts(location, options).await;
            };
            let plain_range = match req_range {
                GetRange::Bounded(r) => r.start..r.end.min(hdr.plaintext_len),
                GetRange::Offset(o) => *o..hdr.plaintext_len,
                GetRange::Suffix(s) => hdr.plaintext_len.saturating_sub(*s)..hdr.plaintext_len,
            };
            let mut meta = self.inner.head(location).await?;
            meta.size = hdr.plaintext_len;
            let decrypted = self.get_range(location, plain_range.clone()).await?;
            let stream: BoxStream<'static, OsResult<Bytes>> =
                Box::pin(futures::stream::once(async move { Ok(decrypted) }));
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range: plain_range,
                attributes: Default::default(),
            });
        }

        let res = self.inner.get_opts(location, options).await?;
        let mut meta = res.meta.clone();
        let attributes = res.attributes.clone();
        let is_head_only = res.range.start == 0 && res.range.end == 0 && meta.size > 0;
        if is_head_only {
            meta = self.translate_meta(meta).await;
            return Ok(GetResult {
                payload: res.payload,
                meta,
                range: 0..0,
                attributes,
            });
        }

        let raw = res.bytes().await?;
        let Some(hdr) = EnvelopeHeader::decode(&raw) else {
            self.check_plaintext_read(location)?;
            self.headers.insert(location.as_ref().to_string(), None);
            let len = raw.len() as u64;
            let stream: BoxStream<'static, OsResult<Bytes>> =
                Box::pin(futures::stream::once(async move { Ok(raw) }));
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta,
                range: 0..len,
                attributes,
            });
        };

        self.headers
            .insert(location.as_ref().to_string(), Some(hdr.clone()));
        let dek = unwrap_dek(&hdr, &self.keyring, location.as_ref())?;
        let decrypted =
            decrypt_chunk_range(&hdr, &dek, 0, &raw[HEADER_LEN..], 0, hdr.plaintext_len)?;
        self.ops_decrypted.fetch_add(1, Ordering::Relaxed);

        meta.size = hdr.plaintext_len;
        let out_range = 0..hdr.plaintext_len;
        let stream: BoxStream<'static, OsResult<Bytes>> = Box::pin(futures::stream::once(
            async move { Ok(Bytes::from(decrypted)) },
        ));
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range: out_range,
            attributes,
        })
    }

    async fn get_range(&self, location: &ObjectPath, range: Range<u64>) -> OsResult<Bytes> {
        let Some(hdr) = self.resolve_header(location).await? else {
            self.check_plaintext_read(location)?;
            return self.inner.get_range(location, range).await;
        };
        let req_start = range.start;
        let req_end = range.end.min(hdr.plaintext_len);
        if req_start >= req_end || hdr.plaintext_len == 0 {
            return Ok(Bytes::new());
        }

        let dek = unwrap_dek(&hdr, &self.keyring, location.as_ref())?;
        let first_chunk = (req_start as usize) / CHUNK_PLAIN_LEN;
        let last_chunk = ((req_end - 1) as usize) / CHUNK_PLAIN_LEN;
        let num_chunks = (hdr.plaintext_len as usize).div_ceil(CHUNK_PLAIN_LEN);
        let tail_plain = {
            let rem = (hdr.plaintext_len as usize) % CHUNK_PLAIN_LEN;
            if rem == 0 {
                CHUNK_PLAIN_LEN
            } else {
                rem
            }
        };
        let total_phys_end = HEADER_LEN
            + (num_chunks.saturating_sub(1)) * CHUNK_CIPHER_LEN
            + (tail_plain + GCM_TAG_LEN);

        let phys_start = (HEADER_LEN + first_chunk * CHUNK_CIPHER_LEN) as u64;
        let phys_end =
            ((HEADER_LEN + (last_chunk + 1) * CHUNK_CIPHER_LEN).min(total_phys_end)) as u64;

        let cipher_slice = self.inner.get_range(location, phys_start..phys_end).await?;
        let decrypted =
            decrypt_chunk_range(&hdr, &dek, first_chunk, &cipher_slice, req_start, req_end)?;
        self.ops_decrypted.fetch_add(1, Ordering::Relaxed);
        Ok(Bytes::from(decrypted))
    }

    async fn head(&self, location: &ObjectPath) -> OsResult<ObjectMeta> {
        let meta = self.inner.head(location).await?;
        Ok(self.translate_meta(meta).await)
    }

    async fn delete(&self, location: &ObjectPath) -> OsResult<()> {
        self.headers.remove(location.as_ref());
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let inner = self.inner.clone();
        let headers = self.headers.clone();
        let stream = self.inner.list(prefix).then(move |res| {
            let inner_c = inner.clone();
            let headers_c = headers.clone();
            async move {
                let mut meta = res?;
                if meta.size >= HEADER_LEN as u64 {
                    let loc_str = meta.location.as_ref().to_string();
                    if let Some(cached) = headers_c.get(&loc_str) {
                        if let Some(hdr) = cached.value() {
                            meta.size = hdr.plaintext_len;
                        }
                    } else if let Ok(probe) = inner_c
                        .get_range(&meta.location, 0..HEADER_LEN as u64)
                        .await
                    {
                        let parsed = EnvelopeHeader::decode(&probe);
                        if let Some(hdr) = &parsed {
                            meta.size = hdr.plaintext_len;
                        }
                        headers_c.insert(loc_str, parsed);
                    }
                }
                Ok(meta)
            }
        });
        Box::pin(stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&ObjectPath>) -> OsResult<ListResult> {
        let mut res = self.inner.list_with_delimiter(prefix).await?;
        for meta in &mut res.objects {
            if meta.size >= HEADER_LEN as u64 {
                *meta = self.translate_meta(meta.clone()).await;
            }
        }
        Ok(res)
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        if self.reseal_copy(from, to, PutMode::Overwrite).await? {
            return Ok(());
        }
        self.inner.copy(from, to).await?;
        if let Some(cached) = self.headers.get(from.as_ref()) {
            self.headers
                .insert(to.as_ref().to_string(), cached.value().clone());
        }
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &ObjectPath, to: &ObjectPath) -> OsResult<()> {
        if self.reseal_copy(from, to, PutMode::Create).await? {
            return Ok(());
        }
        self.inner.copy_if_not_exists(from, to).await?;
        if let Some(cached) = self.headers.get(from.as_ref()) {
            self.headers
                .insert(to.as_ref().to_string(), cached.value().clone());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn path_bound_objects_cannot_be_swapped_but_copy_reseals() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = EncryptedObjectStore::new(inner.clone());
        store.set_master_key(Some([7u8; 32]));
        let (a, b) = (ObjectPath::from("a"), ObjectPath::from("b"));
        store
            .put(&a, PutPayload::from_static(b"secret-a"))
            .await
            .unwrap();
        store
            .put(&b, PutPayload::from_static(b"secret-b"))
            .await
            .unwrap();

        // copy re-seals for the new path and stays readable
        let c = ObjectPath::from("c");
        store.copy(&a, &c).await.unwrap();
        assert_eq!(
            store.get(&c).await.unwrap().bytes().await.unwrap(),
            "secret-a"
        );

        // swapping raw ciphertext from `a` into `b` fails authentication (fresh store, no cache)
        let raw_a = inner.get(&a).await.unwrap().bytes().await.unwrap();
        inner.put(&b, PutPayload::from(raw_a)).await.unwrap();
        let fresh = EncryptedObjectStore::new(inner.clone());
        fresh.set_master_key(Some([7u8; 32]));
        assert!(fresh.get(&b).await.is_err());
        assert!(fresh.get_range(&b, 0..4).await.is_err());
    }
}
