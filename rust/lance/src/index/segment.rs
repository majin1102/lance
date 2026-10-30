// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Independent query execution from an index segment's metadata and location.
//!
//! This experimental boundary supports exact BTree predicates. It does not
//! resolve a table snapshot, remap row IDs, or materialize fragment columns.

use std::sync::Arc;

use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_index::metrics::MetricsCollector;
use lance_index::registry::IndexPluginRegistry;
use lance_index::scalar::btree::BTreeIndexPlugin;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::ScalarIndexPlugin;
use lance_index::scalar::{SargableQuery, ScalarIndex, SearchResult};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry, uri_to_url};
use lance_select::RowAddrTreeMap;
use lance_table::format::IndexMetadata;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

/// How the IDs stored in a segment relate to the fragments being queried.
///
/// The planner supplies this contract. BTree metadata alone does not identify
/// the table's row-ID mode or tell whether subsequent rewrites require remapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SegmentRowIdMode {
    /// IDs already encode physical fragment IDs and row offsets in the selected
    /// snapshot. No stable-ID lookup or fragment-reuse translation is required.
    PhysicalAddresses,
    /// IDs are stable logical row IDs; this demo cannot resolve them to files.
    StableIds,
    /// Stored IDs need translation after a rewrite; unsupported by this demo.
    RequiresRemapping,
}

/// A read task constructed directly from one index segment primitive.
///
/// Storage location and row-ID interpretation are explicit because neither can
/// be recovered safely from [`IndexMetadata`] alone. Construction and execution
/// require no open Dataset or manifest. The caller must keep the referenced
/// index files alive until all readers finish.
///
/// ```
/// # use std::sync::Arc;
/// # use lance::Result;
/// # use lance::index::segment::{IndexSegmentReadTask, SegmentRowIdMode};
/// # use lance_table::format::IndexMetadata;
/// # use object_store::path::Path;
/// # async fn example(metadata: Arc<IndexMetadata>, path: Path) -> Result<()> {
/// let task = IndexSegmentReadTask::from_segment(
///     "s3://example/table", path, metadata, SegmentRowIdMode::PhysicalAddresses,
/// )?;
/// let scanner = task.open(None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct IndexSegmentReadTask {
    storage_uri: String,
    segment_path: Path,
    metadata: Arc<IndexMetadata>,
}

impl IndexSegmentReadTask {
    /// Construct a BTree task from a resolved storage location and raw metadata.
    ///
    /// `storage_uri` selects the object store; `segment_path` is the resolved
    /// segment directory in that store's namespace, not a path relative to the
    /// URI. Any external `base_id` must already have been resolved by the caller.
    /// No table-relative path resolution or metadata I/O occurs here.
    ///
    /// Unknown coverage, missing details, unsupported index types/versions, and
    /// row-ID modes requiring table state are rejected before storage is opened.
    pub fn from_segment(
        storage_uri: &str,
        segment_path: Path,
        metadata: Arc<IndexMetadata>,
        row_id_mode: SegmentRowIdMode,
    ) -> Result<Self> {
        if storage_uri.is_empty() {
            return Err(Error::invalid_input(
                "Index segment task needs a nonempty storage URI",
            ));
        }
        if row_id_mode != SegmentRowIdMode::PhysicalAddresses {
            return Err(Error::not_supported(format!(
                "Index segment {} requires physical row addresses; got {row_id_mode:?}",
                metadata.uuid
            )));
        }
        if metadata.fragment_bitmap.is_none() {
            return Err(Error::invalid_input(format!(
                "Index segment {} is missing fragment coverage",
                metadata.uuid
            )));
        }
        let details = metadata.index_details.as_deref().ok_or_else(|| {
            Error::invalid_input(format!(
                "Index segment {} is missing index details",
                metadata.uuid
            ))
        })?;
        let registry = IndexPluginRegistry::with_default_plugins();
        if !registry.supports_details(details)
            || registry.get_plugin_by_details(details)?.name() != BTreeIndexPlugin.name()
        {
            return Err(Error::not_supported(format!(
                "Index segment {} requires BTree details; got {}",
                metadata.uuid, details.type_url
            )));
        }
        if u32::try_from(metadata.index_version).ok() != Some(BTreeIndexPlugin.version()) {
            return Err(Error::not_supported(format!(
                "Index segment {} has unsupported BTree version {}; expected {}",
                metadata.uuid,
                metadata.index_version,
                BTreeIndexPlugin.version()
            )));
        }
        if metadata.keyed_field().is_none() {
            return Err(Error::not_supported(format!(
                "Index segment {} must have exactly one keyed field",
                metadata.uuid
            )));
        }
        Ok(Self {
            storage_uri: uri_to_url(storage_uri)?.to_string(),
            segment_path,
            metadata,
        })
    }

    /// Metadata for this segment, including its declared coverage and build
    /// version. Coverage is not a live-row or deletion mask for a table snapshot.
    pub fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Open the segment using worker-local storage configuration.
    ///
    /// `None` uses default storage configuration. The scanner owns the runtime
    /// cache lifetime; it does not consult or refresh table metadata.
    pub async fn open(
        &self,
        store_params: Option<ObjectStoreParams>,
    ) -> Result<IndexSegmentScanner> {
        let (store, _) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            &self.storage_uri,
            &store_params.unwrap_or_default(),
        )
        .await?;
        let index_store = LanceIndexStore::new(
            store,
            self.segment_path.clone(),
            Arc::new(LanceCache::no_cache()),
        )
        .with_file_sizes(self.metadata.file_size_map());
        let cache = LanceCache::no_cache();
        let details = self.metadata.index_details.as_deref().ok_or_else(|| {
            Error::invalid_input(format!(
                "Index segment {} is missing index details",
                self.metadata.uuid
            ))
        })?;
        let index = BTreeIndexPlugin
            .load_index(
                Arc::new(index_store),
                details,
                BTreeIndexPlugin.version(),
                None,
                &cache,
            )
            .await?;
        Ok(IndexSegmentScanner {
            index,
            metadata: self.metadata.clone(),
            _cache: cache,
        })
    }
}

/// An open BTree segment scanner producing physical row selections.
#[derive(Debug)]
pub struct IndexSegmentScanner {
    index: Arc<dyn ScalarIndex>,
    metadata: Arc<IndexMetadata>,
    // BTree holds weak cache references, so the scanner must retain their owner.
    _cache: LanceCache,
}

impl IndexSegmentScanner {
    /// Return exact TRUE matches within this segment's declared coverage.
    ///
    /// NULL results do not qualify. Index files may retain rows from fragments
    /// that were removed from their declared coverage; those rows are excluded.
    /// Declared coverage can still contain fragments absent from the selected
    /// table snapshot. The caller must route/intersect against its own snapshot
    /// primitives, handle uncovered fragments, and apply deletions when reading
    /// fragment rows. An older segment build version can therefore be valid.
    ///
    /// No stable-ID lookup, remapping, or whole-table fallback is performed.
    pub async fn search(
        &self,
        query: &SargableQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<RowAddrTreeMap> {
        let SearchResult::Exact(matches) = self.index.search(query, metrics).await? else {
            return Err(Error::not_supported(
                "Index segment scanner requires exact BTree results",
            ));
        };
        let coverage = self.metadata.fragment_bitmap.as_ref().ok_or_else(|| {
            Error::invalid_input(format!(
                "Index segment {} is missing fragment coverage",
                self.metadata.uuid
            ))
        })?;
        let mut rows = matches.true_rows();
        rows.retain_fragments(coverage.iter());
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use lance_index::pbold;
    use rstest::rstest;
    use uuid::Uuid;

    use super::*;

    #[rstest]
    #[case::stable_ids("stable_ids", "physical row addresses", true)]
    #[case::remapping("remapping", "physical row addresses", true)]
    #[case::unknown_coverage("unknown_coverage", "missing fragment coverage", false)]
    #[case::missing_details("missing_details", "missing index details", false)]
    #[case::wrong_type("wrong_type", "requires BTree details", true)]
    #[case::unknown_package("unknown_package", "requires BTree details", true)]
    #[case::future_version("future_version", "unsupported BTree version", true)]
    #[case::negative_version("negative_version", "unsupported BTree version", true)]
    #[case::empty_uri("empty_uri", "storage URI", false)]
    fn invalid_primitives_fail_without_opening_storage(
        #[case] problem: &str,
        #[case] message: &str,
        #[case] unsupported: bool,
    ) {
        let mut metadata = IndexMetadata {
            uuid: Uuid::nil(),
            fields: vec![0],
            covering_fields: vec![],
            name: "key".to_string(),
            dataset_version: 1,
            fragment_bitmap: Some([0].into_iter().collect()),
            index_details: Some(Arc::new(
                prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default()).unwrap(),
            )),
            index_version: BTreeIndexPlugin.version() as i32,
            created_at: None,
            base_id: None,
            files: None,
        };
        let mut uri = "unsupported-store://does-not-exist";
        let mut mode = SegmentRowIdMode::PhysicalAddresses;
        match problem {
            "stable_ids" => mode = SegmentRowIdMode::StableIds,
            "remapping" => mode = SegmentRowIdMode::RequiresRemapping,
            "unknown_coverage" => metadata.fragment_bitmap = None,
            "missing_details" => metadata.index_details = None,
            "wrong_type" => {
                metadata.index_details = Some(Arc::new(
                    prost_types::Any::from_msg(&pbold::BitmapIndexDetails::default()).unwrap(),
                ))
            }
            "unknown_package" => {
                Arc::make_mut(metadata.index_details.as_mut().unwrap()).type_url =
                    "type.googleapis.com/example.BTreeIndexDetails".to_string();
            }
            "future_version" => metadata.index_version += 1,
            "negative_version" => metadata.index_version = -1,
            "empty_uri" => uri = "",
            _ => unreachable!("Unknown test case {problem}"),
        }
        let error = IndexSegmentReadTask::from_segment(
            uri,
            Path::from("segment"),
            Arc::new(metadata),
            mode,
        )
        .unwrap_err();
        assert_eq!(
            matches!(error, Error::NotSupported { .. }),
            unsupported,
            "{error}"
        );
        if !unsupported {
            assert!(matches!(error, Error::InvalidInput { .. }), "{error}");
        }
        assert!(error.to_string().contains(message), "{error}");
    }
}
