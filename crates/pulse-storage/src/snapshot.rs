//! T1 SlateDB snapshot and restore helpers (Candidate A checkpoint copy & Candidate B logical export).

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use pulse_identity::ProjectIdentityGraph;
use serde::{Deserialize, Serialize};
use slatedb::CheckpointCreateResult;

use crate::error::{Result, StorageError};
use crate::manifest::{
    install_restored_manifest, IdentitySnapshotRef, Manifest, ProfileSnapshotRef, PublishedManifest,
};
use crate::profile_store::ProjectProfileStore;

/// Deletes all objects under `prefix` in `store`.
pub async fn delete_objects_with_prefix(store: &Arc<dyn ObjectStore>, prefix: &str) -> Result<()> {
    let p = ObjectPath::from(prefix);
    let metas: Vec<_> = store.list(Some(&p)).try_collect().await?;
    for meta in metas {
        store.delete(&meta.location).await?;
    }
    Ok(())
}

/// Restored project handles returned by [`restore_project_from_published`].
pub struct RestoredProjectSnapshot {
    pub project_id: String,
    pub published: PublishedManifest,
    pub manifest: Manifest,
    pub identity_graph: Arc<ProjectIdentityGraph>,
    pub profile_store: Option<Arc<ProjectProfileStore>>,
    pub identity_user_count: u64,
}

/// Restores `profiles/{project_id}` from `profile_ref` through `store` (preserving `PLSEAGM2`
/// path-bound encryption), or clears local `profiles/{project_id}` when `profile_ref` is `None`.
pub async fn restore_profiles_from_snapshot(
    project_id: &str,
    store: Arc<dyn ObjectStore>,
    profile_ref: Option<&ProfileSnapshotRef>,
) -> Result<Option<Arc<ProjectProfileStore>>> {
    let Some(pref) = profile_ref else {
        delete_objects_with_prefix(&store, &format!("profiles/{project_id}")).await?;
        return Ok(None);
    };
    let snap_path = ObjectPath::from(pref.object_path.as_str());
    let bytes = store.get(&snap_path).await?.bytes().await?;
    let pstore = ProjectProfileStore::restore_from_export(project_id, store, &bytes).await?;
    Ok(Some(pstore))
}

/// Restores `identity/{project_id}` from `id_ref` through `store` (preserving `PLSEAGM2`
/// path-bound encryption) and asserts `identity_user_count >= high_water_mark`.
pub async fn restore_identity_from_snapshot(
    project_id: &str,
    store: Arc<dyn ObjectStore>,
    id_ref: &IdentitySnapshotRef,
    high_water_mark: u64,
) -> Result<(Arc<ProjectIdentityGraph>, u64)> {
    let snap_path = ObjectPath::from(id_ref.object_path.as_str());
    let bytes = store.get(&snap_path).await?.bytes().await?;
    delete_objects_with_prefix(&store, &format!("identity/{project_id}")).await?;
    let id_graph = ProjectIdentityGraph::restore_from_export(project_id, store, &bytes).await?;
    let identity_user_count = id_graph.user_count() as u64;
    if identity_user_count < high_water_mark {
        let _ = id_graph.close().await;
        return Err(StorageError::HighWaterMarkViolation {
            project_id: project_id.to_string(),
            identity_user_count,
            high_water_mark,
        });
    }
    Ok((id_graph, identity_user_count))
}

/// Restores `identity/{project_id}` into `local_store` (preserving `PLSEAGM2` path binding) from
/// a remote `PublishedManifest`'s `id_ref` in `s3_store`, returning a read-only
/// [`ProjectIdentityGraph`] (`db: None`) after asserting `identity_user_count >= high_water_mark`.
pub async fn restore_identity_from_snapshot_read_only(
    project_id: &str,
    s3_store: &Arc<dyn ObjectStore>,
    local_store: Arc<dyn ObjectStore>,
    id_ref: &IdentitySnapshotRef,
    high_water_mark: u64,
) -> Result<(Arc<ProjectIdentityGraph>, u64)> {
    let snap_path = ObjectPath::from(id_ref.object_path.as_str());
    let bytes = s3_store.get(&snap_path).await?.bytes().await?;
    delete_objects_with_prefix(&local_store, &format!("identity/{project_id}")).await?;
    let id_graph =
        ProjectIdentityGraph::restore_from_export_read_only(project_id, local_store, &bytes)
            .await?;
    let identity_user_count = id_graph.user_count() as u64;
    if identity_user_count < high_water_mark {
        return Err(StorageError::HighWaterMarkViolation {
            project_id: project_id.to_string(),
            identity_user_count,
            high_water_mark,
        });
    }
    Ok((id_graph, identity_user_count))
}

/// Restores `profiles/{project_id}/pub_{publish_seq}` into `local_store` (preserving `PLSEAGM2`
/// path binding) from `profile_ref` in `s3_store`, returning a read-only [`ProjectProfileStore`].
pub async fn restore_profiles_from_snapshot_read_only(
    project_id: &str,
    publish_seq: u64,
    s3_store: &Arc<dyn ObjectStore>,
    local_store: Arc<dyn ObjectStore>,
    profile_ref: Option<&ProfileSnapshotRef>,
) -> Result<Arc<ProjectProfileStore>> {
    let db_prefix = format!("profiles/{project_id}/pub_{publish_seq}");
    let Some(pref) = profile_ref else {
        delete_objects_with_prefix(&local_store, &db_prefix).await?;
        return Ok(ProjectProfileStore::open_read_only_at(
            project_id,
            db_prefix,
            local_store,
        ));
    };
    let snap_path = ObjectPath::from(pref.object_path.as_str());
    let bytes = s3_store.get(&snap_path).await?.bytes().await?;
    ProjectProfileStore::restore_from_export_read_only_at(
        project_id,
        db_prefix,
        local_store,
        &bytes,
    )
    .await
}

/// Restores `identity/`, `profiles/`, and `manifests/` for `project_id` from `published` via
/// `enc_store` (`EncryptedObjectStore` preserving `PLSEAGM2` path binding), asserting
/// `identity_user_count >= published.high_water_mark`.
pub async fn restore_project_from_published(
    project_id: &str,
    published: &PublishedManifest,
    enc_store: Arc<dyn ObjectStore>,
) -> Result<RestoredProjectSnapshot> {
    let (identity_graph, identity_user_count) = restore_identity_from_snapshot(
        project_id,
        enc_store.clone(),
        &published.snapshot_ref.identity,
        published.high_water_mark,
    )
    .await?;

    let profile_store = restore_profiles_from_snapshot(
        project_id,
        enc_store.clone(),
        published.snapshot_ref.profiles.as_ref(),
    )
    .await?;

    let manifest = install_restored_manifest(project_id, enc_store, published).await?;

    Ok(RestoredProjectSnapshot {
        project_id: project_id.to_string(),
        published: published.clone(),
        manifest,
        identity_graph,
        profile_store,
        identity_user_count,
    })
}

// ============================================================================
// Candidate A: SlateDB Checkpoint Copy + Local Incremental Tracking
// ============================================================================

#[derive(Debug, Deserialize)]
struct SlateManifestJson {
    core: SlateCoreJson,
}

#[derive(Debug, Deserialize)]
struct SlateCoreJson {
    #[serde(default)]
    l0: Vec<SlateSstHandleJson>,
    #[serde(default)]
    compacted: Vec<SlateSortedRunJson>,
    next_wal_sst_id: u64,
    last_compacted_wal_sst_id: u64,
}

#[derive(Debug, Deserialize)]
struct SlateSortedRunJson {
    #[serde(default)]
    ssts: Vec<SlateSstHandleJson>,
}

#[derive(Debug, Deserialize)]
struct SlateSstHandleJson {
    id: SlateSstIdJson,
}

#[derive(Debug, Deserialize)]
enum SlateSstIdJson {
    #[allow(dead_code)]
    Wal(u64),
    Compacted(String),
}

/// Exact relative object paths belonging to a Candidate A SlateDB checkpoint snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CandidateACheckpointFiles {
    pub compacted_files: Vec<String>,
    pub wal_files: Vec<String>,
    pub manifest_file: String,
}

/// Local tracking state for Candidate A incremental checkpoint uploads (avoids S3 `LIST`).
#[derive(Debug, Default)]
pub struct SlateDbCheckpointSyncState {
    uploaded_compacted: HashSet<String>,
    uploaded_wals: BTreeSet<u64>,
    last_manifest_file: Option<String>,
}

async fn read_slatedb_manifest_json(
    db_root: &ObjectPath,
    store: Arc<dyn ObjectStore>,
    manifest_id: u64,
) -> Result<SlateManifestJson> {
    let raw = slatedb::admin::read_manifest(db_root, store, Some(manifest_id))
        .await
        .map_err(|e| StorageError::Corrupted(format!("slatedb read_manifest failed: {e}")))?
        .ok_or_else(|| {
            StorageError::Corrupted(format!(
                "slatedb manifest {manifest_id} missing under {db_root}"
            ))
        })?;
    let (_id, manifest): (u64, SlateManifestJson) = serde_json::from_str(&raw)?;
    Ok(manifest)
}

/// Syncs a SlateDB checkpoint from `db_root` (local hot tier) to `snapshot_root` (S3 `published/...`)
/// through `store` (`EncryptedObjectStore`, preserving `PLSEAGM2` path binding):
/// 1. Reads the checkpoint manifest via `slatedb::admin::read_manifest`.
/// 2. Uploads any not-yet-uploaded `compacted/` SSTs.
/// 3. Uploads any not-yet-uploaded WAL SSTs `> last_compacted_wal_sst_id` in ascending order.
/// 4. Uploads the SlateDB manifest file LAST.
/// 5. Deletes superseded WAL SSTs (`<= last_compacted_wal_sst_id`), unreferenced compacted SSTs,
///    and the previous manifest file cleanly from S3.
pub async fn sync_slatedb_checkpoint(
    store: &Arc<dyn ObjectStore>,
    db_root: &str,
    snapshot_root: &str,
    cp: &CheckpointCreateResult,
    sync_state: &mut SlateDbCheckpointSyncState,
) -> Result<CandidateACheckpointFiles> {
    let db_path = ObjectPath::from(db_root);
    let manifest = read_slatedb_manifest_json(&db_path, store.clone(), cp.manifest_id).await?;

    let mut needed_compacted_set = BTreeSet::new();
    for handle in manifest
        .core
        .l0
        .iter()
        .chain(manifest.core.compacted.iter().flat_map(|sr| sr.ssts.iter()))
    {
        if let SlateSstIdJson::Compacted(ulid_str) = &handle.id {
            needed_compacted_set.insert(format!("compacted/{ulid_str}.sst"));
        }
    }
    let compacted_files: Vec<String> = needed_compacted_set.iter().cloned().collect();

    let wal_start = manifest.core.last_compacted_wal_sst_id + 1;
    let wal_end = manifest.core.next_wal_sst_id;
    let mut wal_ids = Vec::new();
    let mut wal_files = Vec::new();
    for wal_id in wal_start..wal_end {
        wal_ids.push(wal_id);
        wal_files.push(format!("wal/{wal_id:020}.sst"));
    }

    let manifest_file = format!("manifest/{:020}.manifest", cp.manifest_id);

    // 1. Upload missing compacted SSTs.
    for rel in &compacted_files {
        if !sync_state.uploaded_compacted.contains(rel) {
            let src = ObjectPath::from(format!("{db_root}/{rel}"));
            let dst = ObjectPath::from(format!("{snapshot_root}/{rel}"));
            store.copy(&src, &dst).await?;
            sync_state.uploaded_compacted.insert(rel.clone());
        }
    }

    // 2. Upload missing WAL SSTs > last_compacted_wal_sst_id in strictly ascending order.
    for (wal_id, rel) in wal_ids.iter().zip(wal_files.iter()) {
        if !sync_state.uploaded_wals.contains(wal_id) {
            let src = ObjectPath::from(format!("{db_root}/{rel}"));
            let dst = ObjectPath::from(format!("{snapshot_root}/{rel}"));
            store.copy(&src, &dst).await?;
            sync_state.uploaded_wals.insert(*wal_id);
        }
    }

    // 3. Upload SlateDB manifest LAST.
    if sync_state.last_manifest_file.as_deref() != Some(manifest_file.as_str()) {
        let src = ObjectPath::from(format!("{db_root}/{manifest_file}"));
        let dst = ObjectPath::from(format!("{snapshot_root}/{manifest_file}"));
        store.copy(&src, &dst).await?;
    }

    // 4. Cleanly delete superseded snapshot objects from S3.
    let stale_wals: Vec<u64> = sync_state
        .uploaded_wals
        .iter()
        .copied()
        .filter(|&id| id <= manifest.core.last_compacted_wal_sst_id)
        .collect();
    for wal_id in stale_wals {
        let rel = format!("wal/{wal_id:020}.sst");
        let dst = ObjectPath::from(format!("{snapshot_root}/{rel}"));
        let _ = store.delete(&dst).await;
        sync_state.uploaded_wals.remove(&wal_id);
    }

    let stale_compacted: Vec<String> = sync_state
        .uploaded_compacted
        .iter()
        .filter(|rel| !needed_compacted_set.contains(*rel))
        .cloned()
        .collect();
    for rel in stale_compacted {
        let dst = ObjectPath::from(format!("{snapshot_root}/{rel}"));
        let _ = store.delete(&dst).await;
        sync_state.uploaded_compacted.remove(&rel);
    }

    if let Some(prev_mf) = sync_state.last_manifest_file.take() {
        if prev_mf != manifest_file {
            let dst = ObjectPath::from(format!("{snapshot_root}/{prev_mf}"));
            let _ = store.delete(&dst).await;
        }
    }
    sync_state.last_manifest_file = Some(manifest_file.clone());

    Ok(CandidateACheckpointFiles {
        compacted_files,
        wal_files,
        manifest_file,
    })
}

/// Restores a Candidate A SlateDB snapshot from `snapshot_root` into `db_root` through `store`
/// (`EncryptedObjectStore` re-sealing each object under its destination `db_root` path).
pub async fn restore_slatedb_checkpoint(
    store: &Arc<dyn ObjectStore>,
    db_root: &str,
    snapshot_root: &str,
    files: &CandidateACheckpointFiles,
) -> Result<()> {
    delete_objects_with_prefix(store, db_root).await?;

    for rel in &files.compacted_files {
        let src = ObjectPath::from(format!("{snapshot_root}/{rel}"));
        let dst = ObjectPath::from(format!("{db_root}/{rel}"));
        store.copy(&src, &dst).await?;
    }
    for rel in &files.wal_files {
        let src = ObjectPath::from(format!("{snapshot_root}/{rel}"));
        let dst = ObjectPath::from(format!("{db_root}/{rel}"));
        store.copy(&src, &dst).await?;
    }
    let mf_src = ObjectPath::from(format!("{snapshot_root}/{}", files.manifest_file));
    let mf_dst = ObjectPath::from(format!("{db_root}/{}", files.manifest_file));
    store.copy(&mf_src, &mf_dst).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encrypted_store::EncryptedObjectStore;
    use crate::profile_store::StoredUserProfile;
    use object_store::memory::InMemory;
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn candidate_a_checkpoint_sync_and_restore_with_byok() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let enc = Arc::new(EncryptedObjectStore::new(raw.clone()));
        enc.set_master_key(Some([42u8; 32]));
        let store: Arc<dyn ObjectStore> = enc;

        let id_graph = ProjectIdentityGraph::open("proj_a", store.clone())
            .await
            .unwrap();
        for i in 0..20 {
            id_graph.get_or_intern(&format!("u_{i}"), (i % 4) as u8);
        }
        id_graph.alias_durable("u_0", "u_1", 0, 1).await.unwrap();

        let cp = id_graph.create_slatedb_checkpoint().await.unwrap();
        let mut sync_state = SlateDbCheckpointSyncState::default();
        let files = sync_slatedb_checkpoint(
            &store,
            "identity/proj_a",
            "published/proj_a/snapshots/identity",
            &cp,
            &mut sync_state,
        )
        .await
        .unwrap();
        id_graph.delete_slatedb_checkpoint(&cp).await.unwrap();
        id_graph.close().await.unwrap();

        // Restore into a fresh store with the same BYOK key.
        let restore_raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let restore_enc = Arc::new(EncryptedObjectStore::new(restore_raw));
        restore_enc.set_master_key(Some([42u8; 32]));
        let restore_store: Arc<dyn ObjectStore> = restore_enc;

        // Copy published snapshot objects into restore_store's S3 namespace first.
        let pub_metas: Vec<_> = store
            .list(Some(&ObjectPath::from(
                "published/proj_a/snapshots/identity",
            )))
            .try_collect()
            .await
            .unwrap();
        for m in pub_metas {
            let bytes = store.get(&m.location).await.unwrap().bytes().await.unwrap();
            restore_store.put(&m.location, bytes.into()).await.unwrap();
        }

        restore_slatedb_checkpoint(
            &restore_store,
            "identity/proj_a",
            "published/proj_a/snapshots/identity",
            &files,
        )
        .await
        .unwrap();

        let restored = ProjectIdentityGraph::open("proj_a", restore_store)
            .await
            .unwrap();
        assert_eq!(restored.user_count(), 20);
        assert_eq!(
            restored.lookup_canonical("u_0"),
            restored.lookup_canonical("u_1")
        );
        restored.close().await.unwrap();
    }

    #[tokio::test]
    async fn candidate_b_export_and_restore_profiles_and_identity() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let enc = Arc::new(EncryptedObjectStore::new(raw));
        enc.set_master_key(Some([9u8; 32]));
        let store: Arc<dyn ObjectStore> = enc;

        let id_graph = ProjectIdentityGraph::open("proj_b", store.clone())
            .await
            .unwrap();
        let u0 = id_graph.get_or_intern("u0", 0);
        let u1 = id_graph.get_or_intern("u1", 1);
        let canon = id_graph.alias_durable("u0", "u1", 0, 1).await.unwrap();

        let pstore = ProjectProfileStore::open("proj_b", store.clone())
            .await
            .unwrap();
        pstore
            .upsert(&StoredUserProfile {
                project_id: "proj_b".into(),
                distinct_id: "u1".into(),
                canonical_uid: canon,
                props: BTreeMap::from([("plan".into(), serde_json::json!("pro"))]),
                total_revenue: 99.0,
                updated_at_ms: 12345,
            })
            .await
            .unwrap();

        let id_exp = id_graph.export_snapshot().await.unwrap();
        let prof_exp = pstore.export_snapshot_bytes().await.unwrap();

        store
            .put(
                &ObjectPath::from("published/proj_b/snapshots/id.pzst"),
                id_exp.payload.into(),
            )
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from("published/proj_b/snapshots/prof.pzst"),
                prof_exp.into(),
            )
            .await
            .unwrap();

        id_graph.close().await.unwrap();
        pstore.close().await.unwrap();

        // Wipe local hot prefixes and restore.
        delete_objects_with_prefix(&store, "identity/proj_b")
            .await
            .unwrap();
        delete_objects_with_prefix(&store, "profiles/proj_b")
            .await
            .unwrap();

        let (restored_id, count) = restore_identity_from_snapshot(
            "proj_b",
            store.clone(),
            &IdentitySnapshotRef {
                object_path: "published/proj_b/snapshots/id.pzst".into(),
                user_count: id_exp.user_count,
                mutation_epoch: id_exp.mutation_epoch,
            },
            id_exp.user_count,
        )
        .await
        .unwrap();
        assert_eq!(count, 2);
        assert_eq!(restored_id.snapshot().resolve(u0), canon);
        assert_eq!(restored_id.snapshot().resolve(u1), canon);

        let restored_prof = restore_profiles_from_snapshot(
            "proj_b",
            store.clone(),
            Some(&ProfileSnapshotRef {
                object_path: "published/proj_b/snapshots/prof.pzst".into(),
                dirty_seq: 1,
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let got = restored_prof
            .get_by_distinct_id("u1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.canonical_uid, canon);
        assert_eq!(got.props.get("plan").and_then(|v| v.as_str()), Some("pro"));

        restored_id.close().await.unwrap();
        restored_prof.close().await.unwrap();
    }
}
