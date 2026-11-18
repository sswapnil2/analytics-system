pub mod dictionary;
pub mod encrypted_store;
pub mod engine;
pub mod error;
pub mod manifest;
pub mod mem_bucket;
pub mod replay_store;
pub mod segment;
pub mod store_factory;
pub mod wal;

use arrow_array::cast::AsArray;
use arrow_array::Array as ArrowArray;

pub use dictionary::{ProjectDictSnapshot, ProjectDictionary, MAX_DICTIONARY_ENTRIES};
pub use encrypted_store::{
    make_key_id, parse_byok_master_key, parse_or_derive_master_key, seal_ephemeral_blob,
    unseal_ephemeral_blob, ByokEncryptionStatus, EncryptedObjectStore,
};
pub use engine::{
    event_uuid_fingerprint, route_bucket, MaintenanceErrors, MaintenanceKind, ProjectStorage,
    StorageConfig, StorageEngine, DEFAULT_NUM_BUCKETS,
};
pub use error::{Result, StorageError};
pub use manifest::{Manifest, ManifestStore, ReaderPinTracker, StorageSnapshot};
pub use mem_bucket::{
    MemBucket, StoredRow, SESSION_FLAG_DEAD_TAP, SESSION_FLAG_ERROR, SESSION_FLAG_RAGE_TAP,
};
pub use replay_store::{validate_replay_identifier, ReplayAppendResult, ReplayStore};
pub use segment::{
    decode_session_index, decode_tier_c_row, tier_c_key_bit, write_vortex_segment,
    InvertedValueTag, SegmentFooterMeta, SegmentInvertedIndex, SegmentMeta, SegmentSessionIndex,
    SessionHeader, TierBColumnEntry, VortexSegmentReader, MAX_INVERTED_INDEX_CARDINALITY,
    MAX_TIER_B_KEYS,
};
pub use store_factory::{
    allow_private_endpoints, build_customer_object_store, build_from_url, build_object_store,
    check_resolved_addrs, is_forbidden_ssrf_ip, validate_object_store_connection,
    validate_outbound_url_ssrf, DurableLocalObjectStore,
};
pub use wal::{decode_wal_frame, decode_wal_frame_with_epoch, BucketWal};

/// Zero-copy string extractor supporting Arrow `Utf8`, `LargeUtf8`, and `Utf8View` arrays returned by Vortex.
#[inline]
pub fn extract_arrow_str(col: &dyn ArrowArray, row_idx: usize) -> Option<&str> {
    if col.is_null(row_idx) {
        return None;
    }
    if let Some(sa) = col.as_string_opt::<i32>() {
        Some(sa.value(row_idx))
    } else if let Some(sa) = col.as_string_opt::<i64>() {
        Some(sa.value(row_idx))
    } else if let Some(sv) = col.as_string_view_opt() {
        Some(sv.value(row_idx))
    } else {
        None
    }
}
