// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Independent query execution from an index segment's metadata and location.
//!
//! This experimental boundary supports exact BTree predicates. It does not
//! resolve a table snapshot, remap row IDs, or materialize fragment columns.
//! ANN requires a separate contract for live-row prefilters, partition probing,
//! candidate merging and refinement; these scalar tasks do not implement it.

use std::ops::Bound;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_index::metrics::MetricsCollector;
use lance_index::registry::IndexPluginRegistry;
use lance_index::scalar::btree::BTreeIndexPlugin;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::ScalarIndexPlugin;
use lance_index::scalar::{SargableQuery, ScalarIndex, SearchResult};
use lance_io::object_store::{ObjectStore, ObjectStoreParams, uri_to_url};
use lance_select::RowAddrTreeMap;
use lance_table::format::IndexMetadata;
use object_store::path::Path;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::session::Session;

// Same-build task transport only; this is not a persistent Lance file format.
#[derive(Clone, Message)]
struct TaskEnvelope {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    storage_uri: String,
    #[prost(string, tag = "3")]
    segment_path: String,
    #[prost(message, optional, tag = "4")]
    metadata: Option<lance_table::format::pb::IndexMetadata>,
    #[prost(enumeration = "TaskRowIdMode", tag = "5")]
    row_id_mode: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum TaskRowIdMode {
    Unspecified = 0,
    PhysicalAddresses = 1,
}

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

    /// Encode complete segment metadata for a worker running the same build.
    ///
    /// The task contains no credentials, session or Dataset. The physical row-ID
    /// contract is explicit; a missing contract is never inferred on decode.
    pub fn encode(&self) -> Vec<u8> {
        TaskEnvelope {
            version: 1,
            storage_uri: self.storage_uri.clone(),
            segment_path: self.segment_path.to_string(),
            metadata: Some(self.metadata.as_ref().into()),
            row_id_mode: TaskRowIdMode::PhysicalAddresses as i32,
        }
        .encode_to_vec()
    }

    /// Decode and validate a same-build task without opening storage.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let envelope = TaskEnvelope::decode(bytes)?;
        if envelope.version != 1 {
            return Err(Error::not_supported(format!(
                "Index segment task version {} is unsupported; expected 1",
                envelope.version
            )));
        }
        if envelope.row_id_mode != TaskRowIdMode::PhysicalAddresses as i32 {
            return Err(Error::not_supported(format!(
                "Index segment task requires explicit physical row addresses; got mode {}",
                envelope.row_id_mode
            )));
        }
        let metadata = envelope
            .metadata
            .ok_or_else(|| Error::invalid_input("Index segment task is missing its metadata"))?;
        let path = Path::parse(&envelope.segment_path).map_err(|error| {
            Error::invalid_input(format!(
                "Invalid index segment path {:?}: {error}",
                envelope.segment_path
            ))
        })?;
        Self::from_segment(
            &envelope.storage_uri,
            path,
            Arc::new(IndexMetadata::try_from(metadata)?),
            SegmentRowIdMode::PhysicalAddresses,
        )
    }

    /// Open the segment using worker-local storage configuration.
    ///
    /// `None` uses default storage configuration. The scanner owns the runtime
    /// cache lifetime; it does not consult or refresh table metadata.
    pub async fn open(
        &self,
        store_params: Option<ObjectStoreParams>,
    ) -> Result<IndexSegmentScanner> {
        self.open_with_session(store_params, Arc::new(Session::default()))
            .await
    }

    /// Open with a worker's shared session and worker-local storage credentials.
    ///
    /// Index caches are scoped by storage URI, resolved segment path and UUID.
    /// The scanner retains the cache owner required by the BTree reader.
    pub async fn open_with_session(
        &self,
        store_params: Option<ObjectStoreParams>,
        session: Arc<Session>,
    ) -> Result<IndexSegmentScanner> {
        let (store, _) = ObjectStore::from_uri_and_params(
            session.store_registry(),
            &self.storage_uri,
            &store_params.unwrap_or_default(),
        )
        .await?;
        let index_store = LanceIndexStore::new(
            store,
            self.segment_path.clone(),
            Arc::new(
                session
                    .metadata_cache
                    .for_dataset(&self.storage_uri)
                    .file_metadata_cache(&self.segment_path),
            ),
        )
        .with_file_sizes(self.metadata.file_size_map());
        let cache = session
            .index_cache
            .for_dataset(&self.storage_uri)
            .for_index(&self.metadata.uuid, None)
            .with_key_prefix(self.segment_path.as_ref());
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
    /// Query literals must have the index's key type, including decimal scale
    /// and timestamp timezone. Type mismatches are rejected without coercion.
    pub async fn search(
        &self,
        query: &SargableQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<RowAddrTreeMap> {
        let key_type = self.index.training_data_type().ok_or_else(|| {
            Error::not_supported(format!(
                "Index segment {} does not expose its key type",
                self.metadata.uuid
            ))
        })?;
        // The Dataset planner normally coerces literals before index dispatch.
        // Independent callers bypass it, and the BTree ordering requires matching
        // types (including decimal precision/scale) rather than arbitrary scalars.
        let validate_literal = |value: &ScalarValue| -> Result<()> {
            let actual_type = value.data_type();
            if actual_type != key_type {
                return Err(Error::invalid_input(format!(
                    "Index segment {} query literal type {actual_type} does not match key type {key_type}",
                    self.metadata.uuid
                )));
            }
            Ok(())
        };
        match query {
            SargableQuery::Equals(value) => validate_literal(value)?,
            SargableQuery::Range(lower, upper) => {
                for bound in [lower, upper] {
                    if let Bound::Included(value) | Bound::Excluded(value) = bound {
                        validate_literal(value)?;
                    }
                }
            }
            SargableQuery::IsIn(values) => {
                for value in values {
                    validate_literal(value)?;
                }
            }
            SargableQuery::LikePrefix(value) => {
                validate_literal(value)?;
                if !matches!(key_type, DataType::Utf8 | DataType::LargeUtf8) {
                    return Err(Error::invalid_input(format!(
                        "Index segment {} LIKE prefix requires a string key, got {key_type}",
                        self.metadata.uuid
                    )));
                }
            }
            SargableQuery::IsNull() | SargableQuery::FullTextSearch(_) => {}
        }
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
    use arrow_array::{RecordBatchIterator, record_batch};
    use lance_core::utils::address::RowAddress;
    use lance_index::IndexType;
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::pbold;
    use lance_index::scalar::ScalarIndexParams;
    use rstest::rstest;
    use uuid::Uuid;

    use super::*;
    use crate::Dataset;
    use crate::dataset::WriteParams;
    use crate::index::DatasetIndexExt;

    #[rstest]
    #[case::unknown_version(99, TaskRowIdMode::PhysicalAddresses as i32, "version 99")]
    #[case::missing_row_id_mode(1, TaskRowIdMode::Unspecified as i32, "explicit physical row addresses")]
    #[case::unknown_row_id_mode(1, 99, "mode 99")]
    fn unsupported_transport_is_rejected_before_storage_access(
        #[case] version: u32,
        #[case] row_id_mode: i32,
        #[case] message: &str,
    ) {
        let bytes = TaskEnvelope {
            version,
            row_id_mode,
            ..Default::default()
        }
        .encode_to_vec();
        let error = IndexSegmentReadTask::decode(&bytes).unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }), "{error}");
        assert!(error.to_string().contains(message), "{error}");
    }

    #[tokio::test]
    async fn transported_segment_uses_fresh_shared_session_without_manifest() {
        let temporary = tempfile::tempdir().unwrap();
        let uri = temporary.path().to_str().unwrap();
        let batch = record_batch!(("key", Int32, [Some(7), None, Some(7), Some(9)])).unwrap();
        let input = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(
            input,
            uri,
            Some(WriteParams {
                max_rows_per_file: 2,
                max_rows_per_group: 2,
                enable_stable_row_ids: false,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset
            .create_index(
                &["key"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let metadata = dataset.load_indices().await.unwrap()[0].clone();
        let expected_metadata = lance_table::format::pb::IndexMetadata::from(&metadata);
        let task = IndexSegmentReadTask::from_segment(
            uri,
            dataset.indices_dir().join(metadata.uuid.to_string()),
            Arc::new(metadata),
            SegmentRowIdMode::PhysicalAddresses,
        )
        .unwrap();
        let bytes = task.encode();
        let expected_addresses = dataset
            .fragments()
            .iter()
            .map(|fragment| RowAddress::new_from_parts(fragment.id as u32, 0))
            .collect::<Vec<_>>();
        drop(task);
        drop(dataset);
        std::fs::remove_dir_all(temporary.path().join("_versions")).unwrap();
        let decoded = IndexSegmentReadTask::decode(&bytes).unwrap();
        assert_eq!(
            lance_table::format::pb::IndexMetadata::from(decoded.metadata()),
            expected_metadata,
        );
        let session = Arc::new(Session::default());
        assert_eq!(session.approx_num_items(), 0);
        for _ in 0..2 {
            let scanner = decoded
                .open_with_session(None, session.clone())
                .await
                .unwrap();
            let matches = scanner
                .search(
                    &SargableQuery::Equals(ScalarValue::Int32(Some(7))),
                    &NoOpMetricsCollector,
                )
                .await
                .unwrap();
            assert_eq!(
                matches.row_addrs().unwrap().collect::<Vec<_>>(),
                expected_addresses,
            );
        }
        assert!(session.approx_num_items() > 0);
        let scanner = decoded.open_with_session(None, session).await.unwrap();
        assert_eq!(
            scanner
                .search(
                    &SargableQuery::Equals(ScalarValue::Int32(Some(7))),
                    &NoOpMetricsCollector,
                )
                .await
                .unwrap()
                .row_addrs()
                .unwrap()
                .collect::<Vec<_>>(),
            expected_addresses,
        );
        for query in [
            SargableQuery::Equals(ScalarValue::Utf8(Some("7".into()))),
            SargableQuery::Range(
                Bound::Included(ScalarValue::Utf8(Some("7".into()))),
                Bound::Unbounded,
            ),
            SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::Utf8(Some("9".into()))),
            ),
            SargableQuery::IsIn(vec![
                ScalarValue::Int32(Some(7)),
                ScalarValue::Utf8(Some("9".into())),
            ]),
            SargableQuery::LikePrefix(ScalarValue::Utf8(Some("7".into()))),
        ] {
            let error = scanner
                .search(&query, &NoOpMetricsCollector)
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::InvalidInput { .. }),
                "{query:?}: {error}"
            );
            assert!(
                error.to_string().contains("literal type Utf8"),
                "{query:?}: {error}"
            );
            assert!(
                error.to_string().contains("key type Int32"),
                "{query:?}: {error}"
            );
        }
        let error = scanner
            .search(
                &SargableQuery::LikePrefix(ScalarValue::Int32(Some(7))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput { .. }));
        assert!(error.to_string().contains("requires a string key"));
    }

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
