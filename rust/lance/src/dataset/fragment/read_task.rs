// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Experimental, same-build transport of a fragment read to an independent worker.
//!
//! Planning captures one fragment and its schema. Execution needs storage access,
//! but neither a Dataset nor a manifest. The scheduler must keep referenced data
//! and deletion files alive for the lifetime of the task.

use std::sync::Arc;

use arrow_schema::{DataType, Schema as ArrowSchema};
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema;
use lance_core::{Error, Result};
use lance_file::datatypes::{Fields, FieldsWithMeta};
use lance_file::reader::FileReaderOptions;
use lance_file::version::ConcreteFileVersion;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry, uri_to_url};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_table::format::Fragment;
use lance_table::io::deletion::read_deletion_file;
use object_store::path::Path;
use prost::Message;

use super::current_reader::{CurrentFileReadContext, add_missing_columns};
use super::{FragmentReader, MetadataMode};
use crate::Dataset;

// An experimental task envelope, not a persistent Lance format or a promise of
// compatibility across builds. Existing schema/fragment protobufs preserve IDs.
#[derive(Clone, Message)]
struct TaskEnvelope {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    uri: String,
    #[prost(uint64, tag = "3")]
    snapshot_version: u64,
    #[prost(message, optional, tag = "4")]
    schema: Option<lance_file::format::pb::Schema>,
    #[prost(message, optional, tag = "5")]
    fragment: Option<lance_table::format::pb::DataFragment>,
    #[prost(string, repeated, tag = "6")]
    projection: Vec<String>,
}

/// A self-contained description of a current-format fragment scan or take.
///
/// This demo supports user columns, projection, deletion vectors, and missing
/// columns. It rejects overlays, external bases, dictionary/blob projections,
/// and legacy formats rather than silently returning incomplete data. Stable
/// row IDs are not exposed or resolved; `take` uses physical row offsets.
///
/// ```
/// # use lance::{Dataset, Result};
/// # use lance::dataset::fragment::read_task::FragmentReadTask;
/// # async fn example(dataset: &Dataset) -> Result<()> {
/// let task = FragmentReadTask::try_new(dataset, 0, &["id"])?;
/// let transported = task.encode();
/// // On a worker with no open Dataset:
/// let reader = FragmentReadTask::decode(&transported)?.open(None).await?;
/// let batch = reader.take_as_batch(&[0, 3], None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct FragmentReadTask {
    uri: String,
    snapshot_version: u64,
    schema: Arc<Schema>,
    fragment: Arc<Fragment>,
    projection: Vec<String>,
}

impl FragmentReadTask {
    /// Capture a fragment of this snapshot without retaining the Dataset.
    ///
    /// The projection must contain at least one user column. Unknown fragment
    /// IDs and unsupported features are rejected before transport or file I/O.
    pub fn try_new(dataset: &Dataset, fragment_id: u64, projection: &[&str]) -> Result<Self> {
        let fragment = dataset
            .find_fragment(fragment_id)
            .ok_or_else(|| Error::invalid_input(format!("Unknown fragment ID {fragment_id}")))?;
        let task = Self {
            uri: uri_to_url(dataset.uri())?.to_string(),
            snapshot_version: dataset.version_id(),
            schema: Arc::new(dataset.schema().clone()),
            fragment: Arc::new(fragment.clone()),
            projection: projection.iter().map(|name| (*name).to_owned()).collect(),
        };
        task.validate()?;
        Ok(task)
    }

    /// Encode for a worker running the same build. Credentials are not included.
    pub fn encode(&self) -> Vec<u8> {
        let fields: FieldsWithMeta = self.schema.as_ref().into();
        TaskEnvelope {
            version: 1,
            uri: self.uri.clone(),
            snapshot_version: self.snapshot_version,
            schema: Some(lance_file::format::pb::Schema {
                fields: fields.fields.0,
                metadata: fields.metadata,
            }),
            fragment: Some(self.fragment.as_ref().into()),
            projection: self.projection.clone(),
        }
        .encode_to_vec()
    }

    /// Decode and validate the task without opening storage or a Dataset.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let envelope = TaskEnvelope::decode(bytes)?;
        if envelope.version != 1 {
            return Err(Error::not_supported(format!(
                "Fragment task version {} is unsupported; expected 1",
                envelope.version
            )));
        }
        let schema = envelope
            .schema
            .ok_or_else(|| Error::invalid_input("Fragment task is missing its schema"))?;
        let fragment = envelope
            .fragment
            .ok_or_else(|| Error::invalid_input("Fragment task is missing its fragment"))?;
        let task = Self {
            uri: envelope.uri,
            snapshot_version: envelope.snapshot_version,
            schema: Arc::new(Schema::try_from(FieldsWithMeta {
                fields: Fields(schema.fields),
                metadata: schema.metadata,
            })?),
            fragment: Arc::new(Fragment::try_from(fragment)?),
            projection: envelope.projection,
        };
        task.validate()?;
        Ok(task)
    }

    /// The physical fragment ID used when routing index row addresses.
    pub fn fragment_id(&self) -> u64 {
        self.fragment.id
    }

    /// The snapshot captured by the planner, for task consistency checks.
    /// This value does not pin files or trigger a manifest lookup.
    pub fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    fn validate(&self) -> Result<()> {
        if self.uri.is_empty() || self.projection.is_empty() {
            return Err(Error::invalid_input(
                "Fragment task needs a URI and nonempty projection",
            ));
        }
        self.schema.validate()?;
        let projection = self.schema.project(&self.projection)?;
        for field in projection.fields_pre_order() {
            if field.id < 0
                || field.is_blob()
                || matches!(field.data_type(), DataType::Dictionary(_, _))
            {
                return Err(Error::not_supported(format!(
                    "Fragment task does not support system, blob or dictionary field {}",
                    field.name
                )));
            }
        }
        if self.fragment.id > u32::MAX as u64 {
            return Err(Error::invalid_input(format!(
                "Fragment ID {} exceeds u32",
                self.fragment.id
            )));
        }
        match self.fragment.physical_rows {
            Some(rows) if rows > 0 && rows <= u32::MAX as usize => {}
            rows => {
                return Err(Error::not_supported(format!(
                    "Fragment {} needs a known nonzero physical row count fitting u32, got {rows:?}",
                    self.fragment.id
                )));
            }
        }
        if !self.fragment.overlays.is_empty() {
            return Err(Error::not_supported(
                "Fragment tasks do not yet support overlays",
            ));
        }
        if self
            .fragment
            .deletion_file
            .as_ref()
            .is_some_and(|file| file.base_id.is_some())
        {
            return Err(Error::not_supported(
                "Fragment tasks do not yet support external deletion bases",
            ));
        }
        for file in &self.fragment.files {
            if file.base_id.is_some() {
                return Err(Error::not_supported(
                    "Fragment tasks do not yet support external data bases",
                ));
            }
            if !matches!(
                file.file_version()?,
                ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3
            ) {
                return Err(Error::not_supported(format!(
                    "Fragment task requires format 2.2 or 2.3, file {} has {}.{}",
                    file.path, file.file_major_version, file.file_minor_version
                )));
            }
            file.validate(&Path::from(self.uri.as_str()))?;
        }
        Ok(())
    }

    /// Open the captured files using worker-local storage configuration.
    ///
    /// `None` selects default storage configuration. No manifest is loaded and
    /// no table-wide cache or fragment list is allocated. Missing files fail
    /// normally; task execution never refreshes to a newer snapshot.
    pub async fn open(&self, store_params: Option<ObjectStoreParams>) -> Result<FragmentReader> {
        let (store, base) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            &self.uri,
            &store_params.unwrap_or_default(),
        )
        .await?;
        let projection = self.schema.project(&self.projection)?;
        // Validation guarantees the range and presence of this count.
        let physical_rows = self
            .fragment
            .physical_rows
            .ok_or_else(|| Error::invalid_input("Fragment task is missing physical_rows"))?;
        let deletion_vec = if let Some(file) = &self.fragment.deletion_file {
            let deletions = read_deletion_file(self.fragment.id, file, &base, &store).await?;
            if deletions
                .iter()
                .any(|offset| offset as usize >= physical_rows)
                || file
                    .num_deleted_rows
                    .is_some_and(|count| count != deletions.len())
            {
                return Err(Error::corrupt_file(
                    base,
                    format!(
                        "Invalid deletion vector for fragment {} with {physical_rows} physical rows",
                        self.fragment.id
                    ),
                ));
            }
            Some(Arc::new(deletions))
        } else {
            None
        };
        let num_rows = physical_rows - deletion_vec.as_ref().map_or(0, |deletions| deletions.len());
        let scheduler = ScanScheduler::new(store.clone(), SchedulerConfig::max_bandwidth(&store));
        let mut readers = Vec::with_capacity(self.fragment.files.len() + 1);
        for file in &self.fragment.files {
            let Some(context) = CurrentFileReadContext::try_new(
                &self.schema,
                file,
                Some(&projection),
                Some(physical_rows),
            )?
            else {
                continue;
            };
            let path = base.clone().join("data").join(file.path.as_str());
            let file_scheduler = scheduler
                .open_file_with_priority(&path, 0, &file.file_size_bytes)
                .await?;
            readers.push(
                context
                    .open(
                        file_scheduler,
                        0,
                        LanceCache::no_cache(),
                        FileReaderOptions::default(),
                        MetadataMode::LazyAllowed,
                    )
                    .await?,
            );
        }
        add_missing_columns(&mut readers, &projection, physical_rows as u32);
        FragmentReader::try_new(
            self.fragment.id as usize,
            deletion_vec,
            None,
            readers,
            ArrowSchema::from(&projection),
            num_rows,
            physical_rows,
            self.fragment.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow::compute::concat_batches;
    use arrow_array::{RecordBatchIterator, record_batch};
    use futures::{StreamExt, TryStreamExt};
    use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
    use rstest::rstest;

    use super::*;
    use crate::dataset::{NewColumnTransform, WriteParams};

    #[rstest]
    #[case::physical_ids(false)]
    #[case::stable_ids_user_columns_only(true)]
    #[tokio::test]
    async fn task_reads_without_manifest_or_unrelated_fragment(#[case] stable_ids: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let uri = temporary.path().to_str().unwrap();
        let batch = record_batch!(
            ("id", Int32, [0, 1, 2, 3, 4, 5]),
            (
                "value",
                Utf8,
                [Some("a"), None, Some("c"), None, Some("e"), Some("f")]
            )
        )
        .unwrap();
        let input = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(
            input,
            uri,
            Some(WriteParams {
                max_rows_per_file: 2,
                max_rows_per_group: 2,
                enable_stable_row_ids: stable_ids,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.delete("id = 2").await.unwrap();
        let mut reference = dataset.scan();
        reference.project(&["value", "id"]).unwrap();
        reference.filter("id < 4").unwrap();
        let expected = reference.try_into_batch().await.unwrap();
        let tasks = dataset.fragments()[..2]
            .iter()
            .map(|fragment| {
                FragmentReadTask::try_new(&dataset, fragment.id, &["value", "id"])
                    .unwrap()
                    .encode()
            })
            .collect::<Vec<_>>();
        let unrelated = dataset.fragments()[2].files[0].path.clone();
        drop(reference);
        drop(dataset);
        std::fs::remove_dir_all(temporary.path().join("_versions")).unwrap();
        std::fs::remove_file(temporary.path().join("data").join(unrelated)).unwrap();
        assert!(Dataset::open(uri).await.is_err());
        let mut actual = Vec::new();
        for encoded in tasks {
            let task = FragmentReadTask::decode(&encoded).unwrap();
            let reader = task.open(None).await.unwrap();
            actual.extend(
                reader
                    .read_all(2)
                    .await
                    .unwrap()
                    .buffered(2)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(
            concat_batches(&expected.schema(), actual.iter()).unwrap(),
            expected
        );
    }

    #[test]
    fn unsupported_task_version_is_rejected_before_storage_access() {
        let bytes = TaskEnvelope {
            version: 2,
            ..Default::default()
        }
        .encode_to_vec();
        let error = FragmentReadTask::decode(&bytes).unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }));
        assert!(error.to_string().contains("version 2"));
    }

    #[tokio::test]
    async fn task_merges_column_files_and_synthesizes_missing_nulls() {
        let temporary = tempfile::tempdir().unwrap();
        let uri = temporary.path().to_str().unwrap();
        let batch = record_batch!(("id", Int32, [0, 1])).unwrap();
        let input = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(input, uri, None).await.unwrap();
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("doubled".into(), "id * 2".into())]),
                None,
                None,
            )
            .await
            .unwrap();
        dataset
            .add_columns(
                NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![
                    arrow_schema::Field::new("missing", DataType::Int32, true),
                ]))),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(dataset.fragments()[0].files.len(), 2);
        let projection = ["missing", "doubled", "id"];
        let expected = dataset
            .scan()
            .project(&projection)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(expected["missing"].null_count(), 2);
        let bytes = FragmentReadTask::try_new(&dataset, dataset.fragments()[0].id, &projection)
            .unwrap()
            .encode();
        drop(dataset);
        std::fs::remove_dir_all(temporary.path().join("_versions")).unwrap();
        let reader = FragmentReadTask::decode(&bytes)
            .unwrap()
            .open(None)
            .await
            .unwrap();
        assert_eq!(reader.take_as_batch(&[0, 1], None).await.unwrap(), expected);
    }

    #[rstest]
    #[case::external_data("external_data", "external data bases")]
    #[case::external_deletions("external_deletions", "external deletion bases")]
    #[case::overlays("overlays", "support overlays")]
    #[case::legacy("legacy", "requires format 2.2 or 2.3")]
    #[case::dictionary("dictionary", "dictionary field id")]
    #[case::blob("blob", "blob or dictionary field id")]
    #[tokio::test]
    async fn unsupported_fragment_features_are_rejected_on_decode(
        #[case] feature: &str,
        #[case] expected_message: &str,
    ) {
        let batch = record_batch!(("id", Int32, [0, 1])).unwrap();
        let input = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
        let mut dataset = Dataset::write(input, "memory://", None).await.unwrap();
        dataset.delete("id = 1").await.unwrap();
        let mut task =
            FragmentReadTask::try_new(&dataset, dataset.fragments()[0].id, &["id"]).unwrap();
        let fragment = Arc::make_mut(&mut task.fragment);
        match feature {
            "external_data" => fragment.files[0].base_id = Some(3),
            "external_deletions" => fragment.deletion_file.as_mut().unwrap().base_id = Some(3),
            "overlays" => fragment.overlays.push(DataOverlayFile {
                data_file: fragment.files[0].clone(),
                coverage: OverlayCoverage::Shared(Arc::new([0].into_iter().collect())),
                committed_version: task.snapshot_version,
            }),
            "legacy" => {
                fragment.files[0].file_major_version = 0;
                fragment.files[0].file_minor_version = 2;
            }
            "dictionary" => {
                Arc::make_mut(&mut task.schema).fields[0].logical_type =
                    (&DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)))
                        .try_into()
                        .unwrap();
            }
            "blob" => {
                let field = &mut Arc::make_mut(&mut task.schema).fields[0];
                field.logical_type = (&DataType::LargeBinary).try_into().unwrap();
                field
                    .metadata
                    .insert(lance_arrow::BLOB_META_KEY.into(), "true".into());
            }
            _ => unreachable!("Unknown test case {feature}"),
        }
        let error = FragmentReadTask::decode(&task.encode()).unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }), "{error}");
        assert!(error.to_string().contains(expected_message), "{error}");
    }
}
