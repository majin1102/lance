// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::prelude::*;
use deepsize::DeepSizeOf;
use lance_file::datatypes::{populate_schema_dictionary, Fields, FieldsWithMeta};
use lance_file::reader::FileReader;
use lance_file::version::{LanceFileVersion, LEGACY_FORMAT_VERSION};
use lance_io::traits::{ProtoStruct, Reader};
use object_store::path::Path;
use prost::Message;
use prost_types::Timestamp;

use super::Fragment;
use crate::feature_flags::{has_deprecated_v2_feature_flag, FLAG_MOVE_STABLE_ROW_IDS};
use crate::format::pb;
use lance_core::cache::LanceCache;
use lance_core::datatypes::{Schema, StorageClass};
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_io::utils::read_struct;
use snafu::location;

/// Manifest of a dataset
///
///  * Schema
///  * Version
///  * Fragments.
///  * Indices.
#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct Manifest {
    /// Dataset schema.
    pub schema: Schema,

    /// Local schema, only containing fields with the default storage class (not blobs)
    pub local_schema: Schema,

    /// Dataset version
    pub version: u64,

    /// Version of the writer library that wrote this manifest.
    pub writer_version: Option<WriterVersion>,

    /// Fragments, the pieces to build the dataset.
    ///
    /// This list is stored in order, sorted by fragment id.  However, the fragment id
    /// sequence may have gaps.
    pub fragments: Arc<Vec<Fragment>>,

    /// The file position of the version aux data.
    pub version_aux_data: usize,

    /// The file position of the index metadata.
    pub index_section: Option<usize>,

    /// The creation timestamp with nanosecond resolution as 128-bit integer
    pub timestamp_nanos: u128,

    /// An optional string tag for this version
    pub tag: Option<String>,

    /// The reader flags
    pub reader_feature_flags: u64,

    /// The writer flags
    pub writer_feature_flags: u64,

    /// The max fragment id used so far  
    /// None means never set, Some(0) means max ID used so far is 0
    pub max_fragment_id: Option<u32>,

    /// The path to the transaction file, relative to the root of the dataset
    pub transaction_file: Option<String>,

    /// Precomputed logic offset of each fragment
    /// accelerating the fragment search using offset ranges.
    fragment_offsets: Vec<usize>,

    /// The max row id used so far.
    pub next_row_id: u64,

    /// The storage format of the data files.
    pub data_storage_format: DataStorageFormat,

    /// Table configuration.
    pub config: HashMap<String, String>,

    /// Blob dataset version
    pub blob_dataset_version: Option<u64>,
}

// We use the most significant bit to indicate that a transaction is detached
pub const DETACHED_VERSION_MASK: u64 = 0x8000_0000_0000_0000;

pub fn is_detached_version(version: u64) -> bool {
    version & DETACHED_VERSION_MASK != 0
}

fn compute_fragment_offsets(fragments: &[Fragment]) -> Vec<usize> {
    fragments
        .iter()
        .map(|f| f.num_rows().unwrap_or_default())
        .chain([0]) // Make the last offset to be the full-length of the dataset.
        .scan(0_usize, |offset, len| {
            let start = *offset;
            *offset += len;
            Some(start)
        })
        .collect()
}

impl Manifest {
    pub fn new(
        schema: Schema,
        fragments: Arc<Vec<Fragment>>,
        data_storage_format: DataStorageFormat,
        blob_dataset_version: Option<u64>,
    ) -> Self {
        let fragment_offsets = compute_fragment_offsets(&fragments);
        let local_schema = schema.retain_storage_class(StorageClass::Default);

        Self {
            schema,
            local_schema,
            version: 1,
            writer_version: Some(WriterVersion::default()),
            fragments,
            version_aux_data: 0,
            index_section: None,
            timestamp_nanos: 0,
            tag: None,
            reader_feature_flags: 0,
            writer_feature_flags: 0,
            max_fragment_id: None,
            transaction_file: None,
            fragment_offsets,
            next_row_id: 0,
            data_storage_format,
            config: HashMap::new(),
            blob_dataset_version,
        }
    }

    pub fn new_from_previous(
        previous: &Self,
        schema: Schema,
        fragments: Arc<Vec<Fragment>>,
        new_blob_version: Option<u64>,
    ) -> Self {
        let fragment_offsets = compute_fragment_offsets(&fragments);
        let local_schema = schema.retain_storage_class(StorageClass::Default);

        let blob_dataset_version = new_blob_version.or(previous.blob_dataset_version);

        Self {
            schema,
            local_schema,
            version: previous.version + 1,
            writer_version: Some(WriterVersion::default()),
            fragments,
            version_aux_data: 0,
            index_section: None, // Caller should update index if they want to keep them.
            timestamp_nanos: 0,  // This will be set on commit
            tag: None,
            reader_feature_flags: 0, // These will be set on commit
            writer_feature_flags: 0, // These will be set on commit
            max_fragment_id: previous.max_fragment_id,
            transaction_file: None,
            fragment_offsets,
            next_row_id: previous.next_row_id,
            data_storage_format: previous.data_storage_format.clone(),
            config: previous.config.clone(),
            blob_dataset_version,
        }
    }

    /// Return the `timestamp_nanos` value as a Utc DateTime
    pub fn timestamp(&self) -> DateTime<Utc> {
        let nanos = self.timestamp_nanos % 1_000_000_000;
        let seconds = ((self.timestamp_nanos - nanos) / 1_000_000_000) as i64;
        Utc.from_utc_datetime(
            &DateTime::from_timestamp(seconds, nanos as u32)
                .unwrap_or_default()
                .naive_utc(),
        )
    }

    /// Set the `timestamp_nanos` value from a Utc DateTime
    pub fn set_timestamp(&mut self, nanos: u128) {
        self.timestamp_nanos = nanos;
    }

    /// Set the `config` from an iterator
    pub fn update_config(&mut self, upsert_values: impl IntoIterator<Item = (String, String)>) {
        self.config.extend(upsert_values);
    }

    /// Delete `config` keys using a slice of keys
    pub fn delete_config_keys(&mut self, delete_keys: &[&str]) {
        self.config
            .retain(|key, _| !delete_keys.contains(&key.as_str()));
    }

    /// Replaces the schema metadata with the given key-value pairs.
    pub fn replace_schema_metadata(&mut self, new_metadata: HashMap<String, String>) {
        self.schema.metadata = new_metadata;
    }

    /// Replaces the metadata of the field with the given id with the given key-value pairs.
    ///
    /// If the field does not exist in the schema, this is a no-op.
    pub fn replace_field_metadata(
        &mut self,
        field_id: i32,
        new_metadata: HashMap<String, String>,
    ) -> Result<()> {
        if let Some(field) = self.schema.field_by_id_mut(field_id) {
            field.metadata = new_metadata;
            Ok(())
        } else {
            Err(Error::invalid_input(
                format!(
                    "Field with id {} does not exist for replace_field_metadata",
                    field_id
                ),
                location!(),
            ))
        }
    }

    /// Check the current fragment list and update the high water mark
    pub fn update_max_fragment_id(&mut self) {
        // If there are no fragments, don't update max_fragment_id
        if self.fragments.is_empty() {
            return;
        }

        let max_fragment_id = self
            .fragments
            .iter()
            .map(|f| f.id)
            .max()
            .unwrap() // Safe because we checked fragments is not empty
            .try_into()
            .unwrap();

        match self.max_fragment_id {
            None => {
                // First time being set
                self.max_fragment_id = Some(max_fragment_id);
            }
            Some(current_max) => {
                // Only update if the computed max is greater than current
                // This preserves the high water mark even when fragments are deleted
                if max_fragment_id > current_max {
                    self.max_fragment_id = Some(max_fragment_id);
                }
            }
        }
    }

    /// Return the max fragment id.
    /// Note this does not support recycling of fragment ids.
    ///
    /// This will return None if there are no fragments and max_fragment_id was never set.
    pub fn max_fragment_id(&self) -> Option<u64> {
        if let Some(max_id) = self.max_fragment_id {
            // Return the stored high water mark
            Some(max_id.into())
        } else {
            // Not yet set, compute from fragment list
            self.fragments.iter().map(|f| f.id).max()
        }
    }

    /// Get the max used field id
    ///
    /// This is different than [Schema::max_field_id] because it also considers
    /// the field ids in the data files that have been dropped from the schema.
    pub fn max_field_id(&self) -> i32 {
        let schema_max_id = self.schema.max_field_id().unwrap_or(-1);
        let fragment_max_id = self
            .fragments
            .iter()
            .flat_map(|f| f.files.iter().flat_map(|file| file.fields.as_slice()))
            .max()
            .copied();
        let fragment_max_id = fragment_max_id.unwrap_or(-1);
        schema_max_id.max(fragment_max_id)
    }

    /// Return the fragments that are newer than the given manifest.
    /// Note this does not support recycling of fragment ids.
    pub fn fragments_since(&self, since: &Self) -> Result<Vec<Fragment>> {
        if since.version >= self.version {
            return Err(Error::io(
                format!(
                    "fragments_since: given version {} is newer than manifest version {}",
                    since.version, self.version
                ),
                location!(),
            ));
        }
        let start = since.max_fragment_id();
        Ok(self
            .fragments
            .iter()
            .filter(|&f| start.map(|s| f.id > s).unwrap_or(true))
            .cloned()
            .collect())
    }

    /// Find the fragments that contain the rows, identified by the offset range.
    ///
    /// Note that the offsets are the logical offsets of rows, not row IDs.
    ///
    ///
    /// Parameters
    /// ----------
    /// range: Range<usize>
    ///     Offset range
    ///
    /// Returns
    /// -------
    /// Vec<(usize, Fragment)>
    ///    A vector of `(starting_offset_of_fragment, fragment)` pairs.
    ///
    pub fn fragments_by_offset_range(&self, range: Range<usize>) -> Vec<(usize, &Fragment)> {
        let start = range.start;
        let end = range.end;
        let idx = self
            .fragment_offsets
            .binary_search(&start)
            .unwrap_or_else(|idx| idx - 1);

        let mut fragments = vec![];
        for i in idx..self.fragments.len() {
            if self.fragment_offsets[i] >= end
                || self.fragment_offsets[i] + self.fragments[i].num_rows().unwrap_or_default()
                    <= start
            {
                break;
            }
            fragments.push((self.fragment_offsets[i], &self.fragments[i]));
        }

        fragments
    }

    /// Whether the dataset uses move-stable row ids.
    pub fn uses_move_stable_row_ids(&self) -> bool {
        self.reader_feature_flags & FLAG_MOVE_STABLE_ROW_IDS != 0
    }

    /// Creates a serialized copy of the manifest, suitable for IPC or temp storage
    /// and can be used to create a dataset
    pub fn serialized(&self) -> Vec<u8> {
        let pb_manifest: pb::Manifest = self.into();
        pb_manifest.encode_to_vec()
    }

    pub fn should_use_legacy_format(&self) -> bool {
        self.data_storage_format.version == LEGACY_FORMAT_VERSION
    }
}

#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct WriterVersion {
    pub library: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct DataStorageFormat {
    pub file_format: String,
    pub version: String,
}

const LANCE_FORMAT_NAME: &str = "lance";

impl DataStorageFormat {
    pub fn new(version: LanceFileVersion) -> Self {
        Self {
            file_format: LANCE_FORMAT_NAME.to_string(),
            version: version.resolve().to_string(),
        }
    }

    pub fn lance_file_version(&self) -> Result<LanceFileVersion> {
        self.version.parse::<LanceFileVersion>()
    }
}

impl Default for DataStorageFormat {
    fn default() -> Self {
        Self::new(LanceFileVersion::default())
    }
}

impl From<pb::manifest::DataStorageFormat> for DataStorageFormat {
    fn from(pb: pb::manifest::DataStorageFormat) -> Self {
        Self {
            file_format: pb.file_format,
            version: pb.version,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionPart {
    Major,
    Minor,
    Patch,
}

impl WriterVersion {
    /// Try to parse the version string as a semver string. Returns None if
    /// not successful.
    pub fn semver(&self) -> Option<(u32, u32, u32, Option<&str>)> {
        let mut parts = self.version.split('.');
        let major = parts.next().unwrap_or("0").parse().ok()?;
        let minor = parts.next().unwrap_or("0").parse().ok()?;
        let patch = parts.next().unwrap_or("0").parse().ok()?;
        let tag = parts.next();
        Some((major, minor, patch, tag))
    }

    pub fn semver_or_panic(&self) -> (u32, u32, u32, Option<&str>) {
        self.semver()
            .unwrap_or_else(|| panic!("Invalid writer version: {}", self.version))
    }

    /// Return true if self is older than the given major/minor/patch
    pub fn older_than(&self, major: u32, minor: u32, patch: u32) -> bool {
        let version = self.semver_or_panic();
        (version.0, version.1, version.2) < (major, minor, patch)
    }

    pub fn bump(&self, part: VersionPart, keep_tag: bool) -> Self {
        let parts = self.semver_or_panic();
        let tag = if keep_tag { parts.3 } else { None };
        let new_parts = match part {
            VersionPart::Major => (parts.0 + 1, parts.1, parts.2, tag),
            VersionPart::Minor => (parts.0, parts.1 + 1, parts.2, tag),
            VersionPart::Patch => (parts.0, parts.1, parts.2 + 1, tag),
        };
        let new_version = if let Some(tag) = tag {
            format!("{}.{}.{}.{}", new_parts.0, new_parts.1, new_parts.2, tag)
        } else {
            format!("{}.{}.{}", new_parts.0, new_parts.1, new_parts.2)
        };
        Self {
            library: self.library.clone(),
            version: new_version,
        }
    }
}

impl Default for WriterVersion {
    #[cfg(not(test))]
    fn default() -> Self {
        Self {
            library: "lance".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    // Unit tests always run as if they are in the next version.
    #[cfg(test)]
    fn default() -> Self {
        Self {
            library: "lance".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
        .bump(VersionPart::Patch, true)
    }
}

impl ProtoStruct for Manifest {
    type Proto = pb::Manifest;
}

impl TryFrom<pb::Manifest> for Manifest {
    type Error = Error;

    fn try_from(p: pb::Manifest) -> Result<Self> {
        let timestamp_nanos = p.timestamp.map(|ts| {
            let sec = ts.seconds as u128 * 1e9 as u128;
            let nanos = ts.nanos as u128;
            sec + nanos
        });
        // We only use the writer version if it is fully set.
        let writer_version = match p.writer_version {
            Some(pb::manifest::WriterVersion { library, version }) => {
                Some(WriterVersion { library, version })
            }
            _ => None,
        };
        let fragments = Arc::new(
            p.fragments
                .into_iter()
                .map(Fragment::try_from)
                .collect::<Result<Vec<_>>>()?,
        );
        let fragment_offsets = compute_fragment_offsets(fragments.as_slice());
        let fields_with_meta = FieldsWithMeta {
            fields: Fields(p.fields),
            metadata: p.metadata,
        };

        if FLAG_MOVE_STABLE_ROW_IDS & p.reader_feature_flags != 0
            && !fragments.iter().all(|frag| frag.row_id_meta.is_some())
        {
            return Err(Error::Internal {
                message: "All fragments must have row ids".into(),
                location: location!(),
            });
        }

        let data_storage_format = match p.data_format {
            None => {
                if let Some(inferred_version) = Fragment::try_infer_version(fragments.as_ref())? {
                    // If there are fragments, they are a better indicator
                    DataStorageFormat::new(inferred_version)
                } else {
                    // No fragments to inspect, best we can do is look at writer flags
                    if has_deprecated_v2_feature_flag(p.writer_feature_flags) {
                        DataStorageFormat::new(LanceFileVersion::Stable)
                    } else {
                        DataStorageFormat::new(LanceFileVersion::Legacy)
                    }
                }
            }
            Some(format) => DataStorageFormat::from(format),
        };

        let schema = Schema::from(fields_with_meta);
        let local_schema = schema.retain_storage_class(StorageClass::Default);

        Ok(Self {
            schema,
            local_schema,
            version: p.version,
            writer_version,
            version_aux_data: p.version_aux_data as usize,
            index_section: p.index_section.map(|i| i as usize),
            timestamp_nanos: timestamp_nanos.unwrap_or(0),
            tag: if p.tag.is_empty() { None } else { Some(p.tag) },
            reader_feature_flags: p.reader_feature_flags,
            writer_feature_flags: p.writer_feature_flags,
            max_fragment_id: p.max_fragment_id,
            fragments,
            transaction_file: if p.transaction_file.is_empty() {
                None
            } else {
                Some(p.transaction_file)
            },
            fragment_offsets,
            next_row_id: p.next_row_id,
            data_storage_format,
            config: p.config,
            blob_dataset_version: if p.blob_dataset_version == 0 {
                None
            } else {
                Some(p.blob_dataset_version)
            },
        })
    }
}

impl From<&Manifest> for pb::Manifest {
    fn from(m: &Manifest) -> Self {
        let timestamp_nanos = if m.timestamp_nanos == 0 {
            None
        } else {
            let nanos = m.timestamp_nanos % 1e9 as u128;
            let seconds = ((m.timestamp_nanos - nanos) / 1e9 as u128) as i64;
            Some(Timestamp {
                seconds,
                nanos: nanos as i32,
            })
        };
        let fields_with_meta: FieldsWithMeta = (&m.schema).into();
        Self {
            fields: fields_with_meta.fields.0,
            version: m.version,
            writer_version: m
                .writer_version
                .as_ref()
                .map(|wv| pb::manifest::WriterVersion {
                    library: wv.library.clone(),
                    version: wv.version.clone(),
                }),
            fragments: m.fragments.iter().map(pb::DataFragment::from).collect(),
            metadata: fields_with_meta.metadata,
            version_aux_data: m.version_aux_data as u64,
            index_section: m.index_section.map(|i| i as u64),
            timestamp: timestamp_nanos,
            tag: m.tag.clone().unwrap_or_default(),
            reader_feature_flags: m.reader_feature_flags,
            writer_feature_flags: m.writer_feature_flags,
            max_fragment_id: m.max_fragment_id,
            transaction_file: m.transaction_file.clone().unwrap_or_default(),
            next_row_id: m.next_row_id,
            data_format: Some(pb::manifest::DataStorageFormat {
                file_format: m.data_storage_format.file_format.clone(),
                version: m.data_storage_format.version.clone(),
            }),
            config: m.config.clone(),
            blob_dataset_version: m.blob_dataset_version.unwrap_or_default(),
        }
    }
}

#[async_trait]
pub trait SelfDescribingFileReader {
    /// Open a file reader without any cached schema
    ///
    /// In this case the schema will first need to be loaded
    /// from the file itself.
    ///
    /// When loading files from a dataset it is preferable to use
    /// the fragment reader to avoid this overhead.
    async fn try_new_self_described(
        object_store: &ObjectStore,
        path: &Path,
        cache: Option<&LanceCache>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let reader = object_store.open(path).await?;
        Self::try_new_self_described_from_reader(reader.into(), cache).await
    }

    async fn try_new_self_described_from_reader(
        reader: Arc<dyn Reader>,
        cache: Option<&LanceCache>,
    ) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait]
impl SelfDescribingFileReader for FileReader {
    async fn try_new_self_described_from_reader(
        reader: Arc<dyn Reader>,
        cache: Option<&LanceCache>,
    ) -> Result<Self> {
        let metadata = Self::read_metadata(reader.as_ref(), cache).await?;
        let manifest_position = metadata.manifest_position.ok_or(Error::Internal {
            message: format!(
                "Attempt to open file at {} as self-describing but it did not contain a manifest",
                reader.path(),
            ),
            location: location!(),
        })?;
        let mut manifest: Manifest = read_struct(reader.as_ref(), manifest_position).await?;
        if manifest.should_use_legacy_format() {
            populate_schema_dictionary(&mut manifest.schema, reader.as_ref()).await?;
        }
        let schema = manifest.schema;
        let max_field_id = schema.max_field_id().unwrap_or_default();
        Self::try_new_from_reader(
            reader.path(),
            reader.clone(),
            Some(metadata),
            schema,
            0,
            0,
            max_field_id,
            cache,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::format::DataFile;

    use super::*;

    use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
    use lance_core::datatypes::Field;

    #[test]
    fn test_writer_version() {
        let wv = WriterVersion::default();
        assert_eq!(wv.library, "lance");
        let parts = wv.semver().unwrap();
        assert_eq!(
            parts,
            (
                env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap(),
                env!("CARGO_PKG_VERSION_MINOR").parse().unwrap(),
                // Unit tests run against (major,minor,patch + 1)
                env!("CARGO_PKG_VERSION_PATCH").parse::<u32>().unwrap() + 1,
                None
            )
        );
        assert_eq!(
            format!("{}.{}.{}", parts.0, parts.1, parts.2 - 1),
            env!("CARGO_PKG_VERSION")
        );
        for part in &[VersionPart::Major, VersionPart::Minor, VersionPart::Patch] {
            let bumped = wv.bump(*part, false);
            let bumped_parts = bumped.semver_or_panic();
            assert!(wv.older_than(bumped_parts.0, bumped_parts.1, bumped_parts.2));
        }
    }

    #[test]
    fn test_fragments_by_offset_range() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "a",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let fragments = vec![
            Fragment::with_file_legacy(0, "path1", &schema, Some(10)),
            Fragment::with_file_legacy(1, "path2", &schema, Some(15)),
            Fragment::with_file_legacy(2, "path3", &schema, Some(20)),
        ];
        let manifest = Manifest::new(
            schema,
            Arc::new(fragments),
            DataStorageFormat::default(),
            /*blob_dataset_version= */ None,
        );

        let actual = manifest.fragments_by_offset_range(0..10);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].0, 0);
        assert_eq!(actual[0].1.id, 0);

        let actual = manifest.fragments_by_offset_range(5..15);
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0].0, 0);
        assert_eq!(actual[0].1.id, 0);
        assert_eq!(actual[1].0, 10);
        assert_eq!(actual[1].1.id, 1);

        let actual = manifest.fragments_by_offset_range(15..50);
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0].0, 10);
        assert_eq!(actual[0].1.id, 1);
        assert_eq!(actual[1].0, 25);
        assert_eq!(actual[1].1.id, 2);

        // Out of range
        let actual = manifest.fragments_by_offset_range(45..100);
        assert!(actual.is_empty());

        assert!(manifest.fragments_by_offset_range(200..400).is_empty());
    }

    #[test]
    fn test_max_field_id() {
        // Validate that max field id handles varying field ids by fragment.
        let mut field0 =
            Field::try_from(ArrowField::new("a", arrow_schema::DataType::Int64, false)).unwrap();
        field0.set_id(-1, &mut 0);
        let mut field2 =
            Field::try_from(ArrowField::new("b", arrow_schema::DataType::Int64, false)).unwrap();
        field2.set_id(-1, &mut 2);

        let schema = Schema {
            fields: vec![field0, field2],
            metadata: Default::default(),
        };
        let fragments = vec![
            Fragment {
                id: 0,
                files: vec![DataFile::new_legacy_from_fields("path1", vec![0, 1, 2])],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
            },
            Fragment {
                id: 1,
                files: vec![
                    DataFile::new_legacy_from_fields("path2", vec![0, 1, 43]),
                    DataFile::new_legacy_from_fields("path3", vec![2]),
                ],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
            },
        ];

        let manifest = Manifest::new(
            schema,
            Arc::new(fragments),
            DataStorageFormat::default(),
            /*blob_dataset_version= */ None,
        );

        assert_eq!(manifest.max_field_id(), 43);
    }

    #[test]
    fn test_config() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "a",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let fragments = vec![
            Fragment::with_file_legacy(0, "path1", &schema, Some(10)),
            Fragment::with_file_legacy(1, "path2", &schema, Some(15)),
            Fragment::with_file_legacy(2, "path3", &schema, Some(20)),
        ];
        let mut manifest = Manifest::new(
            schema,
            Arc::new(fragments),
            DataStorageFormat::default(),
            /*blob_dataset_version= */ None,
        );

        let mut config = manifest.config.clone();
        config.insert("lance.test".to_string(), "value".to_string());
        config.insert("other-key".to_string(), "other-value".to_string());

        manifest.update_config(config.clone());
        assert_eq!(manifest.config, config.clone());

        config.remove("other-key");
        manifest.delete_config_keys(&["other-key"]);
        assert_eq!(manifest.config, config);
    }
}
