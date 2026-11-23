pub mod dictionary;
pub mod encrypted_store;
pub mod engine;
pub mod erased;
pub mod error;
pub mod hll;
pub mod manifest;
pub mod mem_bucket;
pub mod profile_store;
pub mod replay_store;
pub mod segment;
pub mod snapshot;
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
    closed_date_max_bytes_from_env, closed_date_upload_interval_from_env, cold_merge_idle_from_env,
    cold_merge_interval_from_env, cold_merge_max_input_bytes_from_env, decode_dead_letter,
    encode_dead_letter, event_uuid_fingerprint, list_dead_letters, route_bucket,
    upload_interval_from_env, upload_merge_max_input_bytes_from_env, DeadLetterEntry,
    MaintenanceErrors, MaintenanceKind, PreparedPublishedRefresh, ProjectStorage, StorageConfig,
    StorageEngine, WriterLeases, DEAD_LETTERED_EVENTS, DEAD_LETTER_MAGIC, DEAD_LETTER_VERSION,
    DEFAULT_CLOSED_DATE_MAX_BYTES, DEFAULT_CLOSED_DATE_UPLOAD_SECS, DEFAULT_COLD_MERGE_IDLE_SECS,
    DEFAULT_COLD_MERGE_INTERVAL_SECS, DEFAULT_COLD_MERGE_MAX_INPUT_BYTES,
    DEFAULT_FLUSH_CONCURRENCY, DEFAULT_HOT_SEGMENTS_MAX_MB, DEFAULT_HOT_SEGMENTS_PROJECT_MAX_MB,
    DEFAULT_NUM_BUCKETS, DEFAULT_UPLOAD_INTERVAL_SECS, DEFAULT_UPLOAD_MERGE_MAX_INPUT_BYTES,
};
pub use erased::ErasedUids;
pub use error::{Result, StorageError};
pub use hll::{CompactHll, HllSketch};
pub use manifest::{
    install_restored_manifest, ColdMergeIntent, ColdMergeIntentStore, IdentitySnapshotRef,
    Manifest, ManifestStore, PartitionEntry, PartitionUploadTime, PendingS3Delete,
    PendingS3DeleteKind, ProfileSnapshotRef, PublishedManifest, PublishedManifestStore,
    ReaderPinTracker, SnapshotRef, StorageSnapshot, UploadIntent, UploadIntentBatch,
    UploadIntentStore,
};
pub use mem_bucket::{
    MemBucket, StoredRow, SESSION_FLAG_DEAD_TAP, SESSION_FLAG_ERROR, SESSION_FLAG_RAGE_TAP,
};
pub use profile_store::{
    expand_erased_with_identity, is_profile_erased, ProfileRouter, ProjectProfileStore,
    StoredUserProfile,
};
pub use replay_store::{validate_replay_identifier, ReplayAppendResult, ReplayStore};
pub use segment::{
    decode_session_index, decode_tier_c_row, tier_c_key_bit, write_vortex_segment,
    write_vortex_segment_opts, InvertedValueTag, SegmentFooterMeta, SegmentInvertedIndex,
    SegmentMeta, SegmentSessionIndex, SessionHeader, TierBColumnEntry, VortexSegmentReader,
    MAX_INVERTED_INDEX_CARDINALITY, MAX_TIER_B_KEYS,
};
pub use snapshot::{
    delete_objects_with_prefix, restore_identity_from_snapshot,
    restore_identity_from_snapshot_read_only, restore_profiles_from_snapshot,
    restore_profiles_from_snapshot_read_only, restore_project_from_published,
    CandidateACheckpointFiles, RestoredProjectSnapshot, SlateDbCheckpointSyncState,
};
pub use store_factory::{
    allow_private_endpoints, build_customer_object_store, build_from_url, build_object_store,
    build_object_store_for_inspect, check_resolved_addrs, finalize_local_hot_store_marker,
    is_forbidden_ssrf_ip, is_hot_routed_store, is_local_hot_store_mode, open_hot_store_for_inspect,
    open_hot_store_for_restore, validate_object_store_connection, validate_outbound_url_ssrf,
    with_hot_store, with_hot_store_for_role, DurableLocalObjectStore,
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
