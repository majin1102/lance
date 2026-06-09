// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Version Archive module
//!
//! This module provides version archive functionality for preserving version metadata
//! when manifests are cleaned up.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::stream::StreamExt;
use lance_core::datatypes::Schema as LanceSchema;
use lance_core::{Error, Result};
use lance_file::previous::reader::FileReader as PreviousFileReader;
use lance_file::previous::writer::{
    FileWriter as PreviousFileWriter, FileWriterOptions as PreviousFileWriterOptions,
};
use lance_io::object_store::ObjectStore;
use lance_table::format::{ManifestSummary, SelfDescribingFileReader};
use lance_table::io::manifest::ManifestDescribing;
use object_store::path::Path;

pub const ARCHIVE_DIR: &str = "_archive";
const VERSION_ARCHIVE_FILE_SUFFIX: &str = ".lance";
const METADATA_KEY_DATASET_CREATED: &str = "lance:archive:dataset_created";
const METADATA_KEY_CREATED_AT: &str = "lance:archive:created_at";

/// Generate archive filename for a given version
fn archive_filename(version: u64) -> String {
    format!("{:020}{}", version, VERSION_ARCHIVE_FILE_SUFFIX)
}

/// Private helper to get the Arrow schema for VersionArchiveEntry
fn version_archive_entry_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp_millis", DataType::Int64, false),
        Field::new("total_fragments", DataType::UInt64, false),
        Field::new("total_data_files", DataType::UInt64, false),
        Field::new("total_files_size", DataType::UInt64, false),
        Field::new("total_deletion_files", DataType::UInt64, false),
        Field::new("total_data_file_rows", DataType::UInt64, false),
        Field::new("total_deletion_file_rows", DataType::UInt64, false),
        Field::new("total_rows", DataType::UInt64, false),
        Field::new("is_tagged", DataType::Boolean, false),
        Field::new("transaction_uuid", DataType::Utf8, true),
        Field::new("read_version", DataType::UInt64, true),
        Field::new("operation_type", DataType::Utf8, true),
        Field::new("transaction_properties", DataType::Utf8, true),
    ]))
}

/// Private helper to get the Lance schema for VersionArchiveEntry
fn version_archive_entry_lance_schema() -> Result<LanceSchema> {
    let arrow_schema = version_archive_entry_schema();
    LanceSchema::try_from(arrow_schema.as_ref())
        .map_err(|e| Error::invalid_input(format!("Failed to create Lance schema: {}", e)))
}

/// Convert a slice of VersionArchiveEntry to an Arrow RecordBatch
///
/// This function serializes version archive entries into a columnar format
/// suitable for storage in Lance files. Transaction properties are
/// serialized to JSON strings with a size limit of 10KB.
///
/// # Arguments
/// * `entries` - Slice of version archive entries to convert
///
/// # Returns
/// * `Ok(RecordBatch)` - The converted record batch with all version data
/// * `Err(Error)` - If conversion fails (e.g., serialization error)
///
/// # Performance
/// This function uses a single-pass algorithm that pre-allocates all vectors
/// for optimal cache locality and minimal memory allocations.
///
/// # Errors
/// - Fails if any entry's transaction properties cannot be serialized to JSON
/// - Fails if the record batch cannot be constructed from the arrays
fn version_archive_entries_to_record_batch(entries: &[VersionArchiveEntry]) -> Result<RecordBatch> {
    let schema = version_archive_entry_schema();
    let capacity = entries.len();

    // Pre-allocate all vectors with known capacity for better performance
    let mut versions = Vec::with_capacity(capacity);
    let mut timestamps = Vec::with_capacity(capacity);
    let mut total_fragments = Vec::with_capacity(capacity);
    let mut total_data_files = Vec::with_capacity(capacity);
    let mut total_files_size = Vec::with_capacity(capacity);
    let mut total_deletion_files = Vec::with_capacity(capacity);
    let mut total_data_file_rows = Vec::with_capacity(capacity);
    let mut total_deletion_file_rows = Vec::with_capacity(capacity);
    let mut total_rows = Vec::with_capacity(capacity);
    let mut is_tagged = Vec::with_capacity(capacity);
    let mut transaction_uuid = Vec::with_capacity(capacity);
    let mut read_version = Vec::with_capacity(capacity);
    let mut operation_type = Vec::with_capacity(capacity);
    let mut transaction_properties = Vec::with_capacity(capacity);

    // Single pass through entries - more cache friendly
    for s in entries {
        versions.push(Some(s.version));
        timestamps.push(Some(s.timestamp_millis));
        total_fragments.push(Some(s.manifest_summary.total_fragments));
        total_data_files.push(Some(s.manifest_summary.total_data_files));
        total_files_size.push(Some(s.manifest_summary.total_files_size));
        total_deletion_files.push(Some(s.manifest_summary.total_deletion_files));
        total_data_file_rows.push(Some(s.manifest_summary.total_data_file_rows));
        total_deletion_file_rows.push(Some(s.manifest_summary.total_deletion_file_rows));
        total_rows.push(Some(s.manifest_summary.total_rows));
        is_tagged.push(Some(s.is_tagged));
        transaction_uuid.push(s.transaction_uuid.as_deref());
        read_version.push(s.read_version);
        operation_type.push(s.operation_type.as_deref());

        // Handle transaction_properties with size limit
        if s.transaction_properties.is_empty() {
            transaction_properties.push(None);
        } else {
            match serde_json::to_string(&s.transaction_properties) {
                Ok(json) => {
                    const MAX_TRANSACTION_PROPERTIES_SIZE: usize = 10 * 1024;
                    if json.len() > MAX_TRANSACTION_PROPERTIES_SIZE {
                        tracing::warn!(
                            "Transaction properties size ({}) exceeds limit ({}), truncating",
                            json.len(),
                            MAX_TRANSACTION_PROPERTIES_SIZE
                        );
                        // Keep the preview on a UTF-8 boundary so metadata with
                        // non-ASCII values cannot panic during cleanup.
                        let mut preview_len = MAX_TRANSACTION_PROPERTIES_SIZE.min(json.len());
                        while !json.is_char_boundary(preview_len) {
                            preview_len -= 1;
                        }
                        let preview = serde_json::to_string(&json[..preview_len]).map_err(|e| {
                            Error::invalid_input(format!(
                                "Failed to encode truncated transaction properties preview: {}",
                                e
                            ))
                        })?;
                        let wrapped = format!(
                            "{{\"_truncated\":true,\"_size\":{},\"_data\":{}}}",
                            json.len(),
                            preview
                        );
                        transaction_properties.push(Some(wrapped));
                    } else {
                        transaction_properties.push(Some(json));
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to serialize transaction_properties: {}", e);
                    transaction_properties.push(None);
                }
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(versions)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(UInt64Array::from(total_fragments)),
            Arc::new(UInt64Array::from(total_data_files)),
            Arc::new(UInt64Array::from(total_files_size)),
            Arc::new(UInt64Array::from(total_deletion_files)),
            Arc::new(UInt64Array::from(total_data_file_rows)),
            Arc::new(UInt64Array::from(total_deletion_file_rows)),
            Arc::new(UInt64Array::from(total_rows)),
            Arc::new(BooleanArray::from(is_tagged)),
            Arc::new(StringArray::from(transaction_uuid)),
            Arc::new(UInt64Array::from(read_version)),
            Arc::new(StringArray::from(operation_type)),
            Arc::new(StringArray::from(transaction_properties)),
        ],
    )
    .map_err(|e| Error::invalid_input(format!("Failed to create RecordBatch: {}", e)))
}

/// Macro to extract and downcast a column from a RecordBatch
macro_rules! get_column {
    ($batch:expr, $name:expr, $type:ty, $type_str:expr) => {
        $batch
            .column_by_name($name)
            .ok_or_else(|| Error::invalid_input(concat!($name, " column not found")))?
            .as_any()
            .downcast_ref::<$type>()
            .ok_or_else(|| Error::invalid_input(concat!($name, " column is not ", $type_str)))?
    };
}

/// Convert an Arrow RecordBatch to a Vec of VersionArchiveEntry
///
/// This function deserializes a record batch (read from a Lance archive file)
/// back into version archive entries. It handles JSON deserialization of transaction
/// properties and provides fallback error handling for corrupted data.
///
/// # Arguments
/// * `batch` - The Arrow record batch to convert
///
/// # Returns
/// * `Ok(Vec<VersionArchiveEntry>)` - The converted version archive entries
/// * `Err(Error)` - If conversion fails (e.g., column not found, type mismatch)
///
/// # Error Handling
/// - If a column is missing or has the wrong type, returns an error
/// - If transaction properties JSON cannot be parsed, preserves the raw value
///   in a fallback HashMap for debugging purposes
///
/// # Performance
/// This function uses column-by-name access for clarity and maintains
/// the order of entries as they appear in the record batch.
fn record_batch_to_version_archive_entries(
    batch: &RecordBatch,
) -> Result<Vec<VersionArchiveEntry>> {
    let mut entries = Vec::with_capacity(batch.num_rows());

    let version_col = get_column!(batch, "version", UInt64Array, "UInt64");
    let timestamp_col = get_column!(batch, "timestamp_millis", Int64Array, "Int64");
    let total_fragments_col = get_column!(batch, "total_fragments", UInt64Array, "UInt64");
    let total_data_files_col = get_column!(batch, "total_data_files", UInt64Array, "UInt64");
    let total_files_size_col = get_column!(batch, "total_files_size", UInt64Array, "UInt64");
    let total_deletion_files_col =
        get_column!(batch, "total_deletion_files", UInt64Array, "UInt64");
    let total_data_file_rows_col =
        get_column!(batch, "total_data_file_rows", UInt64Array, "UInt64");
    let total_deletion_file_rows_col =
        get_column!(batch, "total_deletion_file_rows", UInt64Array, "UInt64");
    let total_rows_col = get_column!(batch, "total_rows", UInt64Array, "UInt64");
    let is_tagged_col = get_column!(batch, "is_tagged", BooleanArray, "Boolean");
    let transaction_uuid_col = get_column!(batch, "transaction_uuid", StringArray, "String");
    let read_version_col = get_column!(batch, "read_version", UInt64Array, "UInt64");
    let operation_type_col = get_column!(batch, "operation_type", StringArray, "String");
    let transaction_properties_col =
        get_column!(batch, "transaction_properties", StringArray, "String");

    for i in 0..batch.num_rows() {
        let transaction_properties: HashMap<String, String> = if transaction_properties_col
            .is_valid(i)
        {
            match serde_json::from_str(transaction_properties_col.value(i)) {
                Ok(props) => props,
                Err(e) => {
                    let raw_value = transaction_properties_col.value(i);
                    tracing::warn!(
                        "Failed to parse transaction_properties: {}, raw value: {}",
                        e,
                        raw_value
                    );
                    // Preserve raw value for debugging
                    let mut fallback = HashMap::with_capacity(1);
                    fallback.insert("transaction_properties".to_string(), raw_value.to_string());
                    fallback
                }
            }
        } else {
            HashMap::new()
        };

        entries.push(VersionArchiveEntry {
            version: version_col.value(i),
            timestamp_millis: timestamp_col.value(i),
            manifest_summary: ManifestSummary {
                total_fragments: total_fragments_col.value(i),
                total_data_files: total_data_files_col.value(i),
                total_files_size: total_files_size_col.value(i),
                total_deletion_files: total_deletion_files_col.value(i),
                total_data_file_rows: total_data_file_rows_col.value(i),
                total_deletion_file_rows: total_deletion_file_rows_col.value(i),
                total_rows: total_rows_col.value(i),
            },
            is_tagged: is_tagged_col.value(i),
            transaction_uuid: transaction_uuid_col
                .is_valid(i)
                .then(|| transaction_uuid_col.value(i).to_string()),
            read_version: read_version_col
                .is_valid(i)
                .then(|| read_version_col.value(i)),
            operation_type: operation_type_col
                .is_valid(i)
                .then(|| operation_type_col.value(i).to_string()),
            transaction_properties,
        });
    }

    Ok(entries)
}

/// Archive configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionArchiveConfig {
    /// Whether VersionArchive is enabled
    pub enabled: bool,

    /// Maximum number of version entries to retain in a single archive file
    pub max_entries: usize,

    /// Maximum number of archive files to retain
    pub max_archive_files: usize,
}

impl Default for VersionArchiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10000,
            max_archive_files: 2,
        }
    }
}

impl VersionArchiveConfig {
    /// Read configuration from manifest config
    ///
    /// Ensures that `max_entries` and `max_archive_files` are at least 1
    /// to prevent data loss during archive finalization.
    pub fn from_config(config: &HashMap<String, String>) -> Self {
        let max_entries = config
            .get("lance.version_archive.max_entries")
            .and_then(|v| v.parse().ok())
            .unwrap_or(10000)
            .max(1);

        let max_archive_files = config
            .get("lance.version_archive.max_archive_files")
            .and_then(|v| v.parse().ok())
            .unwrap_or(2)
            .max(1);

        Self {
            enabled: config
                .get("lance.version_archive.enabled")
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            max_entries,
            max_archive_files,
        }
    }
}

/// Version archive entry with flattened manifest statistics
///
/// This structure contains metadata about a specific version of the dataset,
/// including statistics about fragments, data files, deletion files, and rows.
/// It is used for archive preservation when manifests are cleaned up.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionArchiveEntry {
    /// The version number of this dataset version
    pub version: u64,
    /// Timestamp when this version was created (milliseconds since epoch)
    pub timestamp_millis: i64,
    /// Summary statistics about the manifest for this version
    pub manifest_summary: ManifestSummary,
    /// Whether this version is tagged (should be retained)
    pub is_tagged: bool,
    /// Unique identifier for the transaction that created this version
    pub transaction_uuid: Option<String>,
    /// The version that was read when this transaction was committed
    pub read_version: Option<u64>,
    /// The type of operation that created this version (e.g., "append", "overwrite")
    pub operation_type: Option<String>,
    /// Additional properties from the transaction
    pub transaction_properties: HashMap<String, String>,
}

/// Version archive with persistence capability
///
/// This structure maintains version metadata for dataset versions, preserving
/// information about versions even after their manifests have been cleaned up.
/// Archives are stored in Lance format and support automatic cleanup of
/// old archive files based on configuration.
#[derive(Debug, Clone)]
pub struct VersionArchive {
    /// List of version archive entries stored in this archive
    pub versions: Vec<VersionArchiveEntry>,
    /// The highest version number in this archive
    pub latest_version_number: u64,
    /// Timestamp when the dataset was created (milliseconds since epoch)
    pub dataset_created_millis: i64,
    /// Timestamp when this archive was created (milliseconds since epoch)
    pub created_at_millis: i64,
    /// Configuration for archive behavior
    config: VersionArchiveConfig,
    /// Base path for the dataset
    base: Path,
    /// Object store for persistence
    object_store: Arc<ObjectStore>,
}

impl VersionArchive {
    pub fn archive_dir(&self) -> Path {
        self.base.clone().join(ARCHIVE_DIR)
    }

    async fn list_archive_files(
        object_store: &ObjectStore,
        archive_dir: &Path,
    ) -> Result<Vec<(u64, Path)>> {
        let mut archives = Vec::new();
        let mut stream = object_store.list(Some(archive_dir.clone()));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if let Some(filename) = meta.location.filename()
                && let Some(version) = filename
                    .strip_suffix(VERSION_ARCHIVE_FILE_SUFFIX)
                    .and_then(|s| s.parse::<u64>().ok())
            {
                archives.push((version, meta.location));
            }
        }

        archives.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(archives)
    }

    /// Private helper to write an archive in Lance format
    async fn write_archive(&self) -> Result<()> {
        let archive_dir = self.archive_dir();
        let filename = archive_filename(self.latest_version_number);
        let path = archive_dir.join(filename);

        let batch = version_archive_entries_to_record_batch(&self.versions)?;
        let mut lance_schema = version_archive_entry_lance_schema()?;
        lance_schema.metadata.insert(
            METADATA_KEY_DATASET_CREATED.to_string(),
            self.dataset_created_millis.to_string(),
        );
        lance_schema.metadata.insert(
            METADATA_KEY_CREATED_AT.to_string(),
            self.created_at_millis.to_string(),
        );

        let options = PreviousFileWriterOptions::default();
        let mut writer = PreviousFileWriter::<ManifestDescribing>::try_new(
            &self.object_store,
            &path,
            lance_schema,
            &options,
        )
        .await?;

        writer.write(&[batch]).await?;
        writer.finish().await?;

        Ok(())
    }

    /// Private helper to read an archive from Lance format
    async fn read_archive(
        base: &Path,
        object_store: Arc<ObjectStore>,
        path: &Path,
        config: VersionArchiveConfig,
    ) -> Result<Self> {
        let reader = object_store.open(path).await?;
        let reader =
            PreviousFileReader::try_new_self_described_from_reader(reader.into(), None).await?;
        let num_batches = reader.num_batches();

        let mut all_entries = Vec::new();
        for i in 0..num_batches {
            let batch = reader
                .read_batch(
                    i as i32,
                    lance_io::ReadBatchParams::RangeFull,
                    reader.schema(),
                )
                .await?;
            all_entries.extend(record_batch_to_version_archive_entries(&batch)?);
        }

        let latest_version_number = all_entries.iter().map(|s| s.version).max().unwrap_or(0);

        let schema = reader.schema();
        let dataset_created_millis = Self::parse_metadata(
            &schema.metadata,
            METADATA_KEY_DATASET_CREATED,
            0i64,
            path,
            "i64",
        );
        let created_at_millis = Self::parse_metadata(
            &schema.metadata,
            METADATA_KEY_CREATED_AT,
            chrono::Utc::now().timestamp_millis(),
            path,
            "i64",
        );

        Ok(Self {
            versions: all_entries,
            latest_version_number,
            dataset_created_millis,
            created_at_millis,
            config,
            base: base.clone(),
            object_store,
        })
    }

    /// Load archive files from storage
    ///
    /// Attempts to load archive files in order (newest first) and returns
    /// the first successfully loaded archive.
    async fn load_from_files(
        base: &Path,
        object_store: Arc<ObjectStore>,
        config: VersionArchiveConfig,
    ) -> Result<Option<Self>> {
        let archive_dir = base.clone().join(ARCHIVE_DIR);
        let archives = Self::list_archive_files(&object_store, &archive_dir).await?;

        for (_, path) in archives {
            match Self::read_archive(base, object_store.clone(), &path, config).await {
                Ok(archive) => return Ok(Some(archive)),
                Err(e) => {
                    tracing::warn!("Failed to load archive file {}: {}", path, e);
                }
            }
        }
        Ok(None)
    }

    /// Load the latest archive from storage, or create a new empty one
    ///
    /// Tries to load from the newest archive file. If corrupted, tries older files.
    /// If no valid archive exists, creates a new empty one.
    pub async fn load_or_new(
        base: Path,
        object_store: Arc<ObjectStore>,
        config: VersionArchiveConfig,
    ) -> Result<Self> {
        if let Some(archive) = Self::load_from_files(&base, object_store.clone(), config).await? {
            return Ok(archive);
        }

        Ok(Self {
            versions: Vec::new(),
            latest_version_number: 0,
            dataset_created_millis: 0,
            created_at_millis: chrono::Utc::now().timestamp_millis(),
            config,
            base,
            object_store,
        })
    }

    /// Load the latest archive from storage
    pub async fn load_latest(
        base: Path,
        object_store: Arc<ObjectStore>,
        config: VersionArchiveConfig,
    ) -> Result<Option<Self>> {
        Self::load_from_files(&base, object_store, config).await
    }

    /// Scan all retained archive files.
    ///
    /// Entries are deduplicated by version.  If the same version exists in
    /// multiple archive files then the newest archive file wins.
    pub async fn scan(
        base: Path,
        object_store: Arc<ObjectStore>,
        config: VersionArchiveConfig,
    ) -> Result<Vec<VersionArchiveEntry>> {
        let archive_dir = base.clone().join(ARCHIVE_DIR);
        let archives = Self::list_archive_files(&object_store, &archive_dir).await?;
        let mut seen_versions = HashSet::new();
        let mut entries = Vec::new();

        for (_, path) in archives {
            match Self::read_archive(&base, object_store.clone(), &path, config).await {
                Ok(archive) => {
                    for entry in archive.versions {
                        if seen_versions.insert(entry.version) {
                            entries.push(entry);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to load archive file {}: {}", path, e);
                }
            }
        }

        entries.sort_by_key(|entry| entry.version);
        if entries.len() > config.max_entries {
            let remove_count = entries.len() - config.max_entries;
            entries.drain(0..remove_count);
        }
        Ok(entries)
    }

    /// Add new version archive entries to the archive
    /// Entries are sorted by version before adding
    pub fn add_entries(&mut self, entries: &[VersionArchiveEntry]) {
        if entries.is_empty() {
            return;
        }
        self.versions.extend(entries.iter().cloned());
    }

    /// Finalize the archive before flushing
    fn finalize_entries(&mut self) {
        if self.versions.is_empty() {
            return;
        }

        self.versions.sort_by_key(|v| v.version);
        if self.dataset_created_millis == 0 {
            self.dataset_created_millis = self
                .versions
                .first()
                .map(|v| v.timestamp_millis)
                .unwrap_or(0);
        }

        if self.versions.len() > self.config.max_entries {
            let remove_count = self.versions.len() - self.config.max_entries;
            self.versions.drain(0..remove_count);
        }

        self.latest_version_number = self.versions.iter().map(|v| v.version).max().unwrap_or(0);
        self.created_at_millis = chrono::Utc::now().timestamp_millis();
    }

    /// Flush the archive to storage
    pub async fn flush(&mut self) -> Result<()> {
        self.finalize_entries();

        if self.versions.is_empty() {
            return Ok(());
        }

        self.write_archive().await?;
        self.cleanup_old_archives().await?;

        Ok(())
    }

    async fn cleanup_old_archives(&self) -> Result<()> {
        let archive_dir = self.archive_dir();
        let archives = Self::list_archive_files(&self.object_store, &archive_dir).await?;

        if archives.len() > self.config.max_archive_files {
            // archives is sorted in descending order (newest first)
            // skip the newest max_archive_files, delete the rest (older ones)
            for (_, path) in archives.iter().skip(self.config.max_archive_files) {
                if let Err(e) = self.object_store.delete(path).await {
                    tracing::warn!("Failed to delete old archive file {}: {}", path, e);
                }
            }
        }

        Ok(())
    }

    pub fn latest_version(&self) -> u64 {
        self.latest_version_number
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Parse typed metadata from schema with default value on error
    ///
    /// This generic function attempts to parse a metadata value as the specified type T.
    /// If the key is missing or parsing fails, it logs a warning and returns the default value.
    ///
    /// # Type Parameters
    /// * `T` - The target type to parse the metadata value as (must implement FromStr and Default)
    ///
    /// # Arguments
    /// * `metadata` - The metadata HashMap to read from
    /// * `key` - The metadata key to look up
    /// * `default` - The default value to use if the key is missing or parsing fails
    /// * `path` - The archive file path for error reporting
    /// * `type_name` - Human-readable type name for error messages (e.g., "i64", "u64")
    ///
    /// # Returns
    /// The parsed value or the default value
    ///
    /// # Example
    /// ```ignore
    /// let value = Self::parse_metadata(
    ///     &schema.metadata,
    ///     "lance:archive:dataset_created",
    ///     0,
    ///     &path,
    ///     "i64"
    /// );
    /// ```
    fn parse_metadata<T: std::str::FromStr + Default + std::fmt::Debug>(
        metadata: &HashMap<String, String>,
        key: &str,
        default: T,
        path: &Path,
        type_name: &str,
    ) -> T
    where
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        match metadata.get(key) {
            Some(metadata_value) => match metadata_value.parse::<T>() {
                Ok(parsed_value) => parsed_value,
                Err(parse_error) => {
                    tracing::warn!(
                        "Failed to parse {} metadata '{}' as {}: {}, using default {:?}",
                        key,
                        metadata_value,
                        type_name,
                        parse_error,
                        default
                    );
                    default
                }
            },
            None => {
                tracing::warn!(
                    "Missing {} metadata in archive file {}, using default {:?}",
                    key,
                    path,
                    default
                );
                default
            }
        }
    }

    #[cfg(test)]
    fn config(&self) -> &VersionArchiveConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use lance_io::object_store::ObjectStore;
    use lance_table::format::ManifestSummary;

    use super::*;

    fn create_test_version_archive_entry(version: u64) -> VersionArchiveEntry {
        VersionArchiveEntry {
            version,
            timestamp_millis: version as i64 * 1000,
            manifest_summary: ManifestSummary {
                total_fragments: version,
                total_data_files: version,
                total_files_size: version * 100,
                total_deletion_files: 0,
                total_data_file_rows: version * 100,
                total_deletion_file_rows: 0,
                total_rows: version * 100,
            },
            is_tagged: false,
            transaction_uuid: None,
            read_version: None,
            operation_type: None,
            transaction_properties: HashMap::new(),
        }
    }

    struct ArchiveTestFixture {
        archive: VersionArchive,
    }

    impl ArchiveTestFixture {
        async fn new() -> Self {
            Self::new_with_config(VersionArchiveConfig::default()).await
        }

        async fn new_with_config(config: VersionArchiveConfig) -> Self {
            let object_store = ObjectStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                url::Url::parse("memory://").unwrap(),
                None,
                None,
                false,
                true,
                1,
                3,
                None,
            );
            let archive =
                VersionArchive::load_or_new(Path::from("test"), Arc::new(object_store), config)
                    .await
                    .unwrap();
            Self { archive }
        }
    }

    #[tokio::test]
    async fn test_archive_load_or_new_empty() {
        let fixture = ArchiveTestFixture::new().await;
        assert!(fixture.archive.versions.is_empty());
        assert_eq!(fixture.archive.latest_version(), 0);
    }

    #[tokio::test]
    async fn test_archive_add_entries_and_flush() {
        let mut fixture = ArchiveTestFixture::new().await;
        assert!(
            fixture.archive.is_enabled(),
            "Archive should be enabled by default"
        );

        fixture.archive.add_entries(&[
            create_test_version_archive_entry(1),
            create_test_version_archive_entry(2),
        ]);
        fixture.archive.flush().await.unwrap();

        assert_eq!(fixture.archive.versions.len(), 2);
        assert_eq!(fixture.archive.latest_version(), 2);

        // Test disabled archive - is_enabled() returns false
        let disabled_fixture = ArchiveTestFixture::new_with_config(VersionArchiveConfig {
            enabled: false,
            ..Default::default()
        })
        .await;
        assert!(
            !disabled_fixture.archive.is_enabled(),
            "Archive should be disabled when enabled=false"
        );
    }

    #[tokio::test]
    async fn test_archive_load_existing() {
        let mut fixture = ArchiveTestFixture::new().await;
        fixture
            .archive
            .add_entries(&[create_test_version_archive_entry(1)]);
        fixture.archive.flush().await.unwrap();

        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.versions.len(), 1);
        assert_eq!(loaded.versions[0].version, 1);
        assert_eq!(loaded.latest_version(), 1);

        // Test is_tagged field serialization/deserialization
        let mut fixture2 = ArchiveTestFixture::new().await;
        let mut summary = create_test_version_archive_entry(2);
        summary.is_tagged = true;
        fixture2.archive.add_entries(&[summary]);
        fixture2.archive.flush().await.unwrap();

        let loaded2 = VersionArchive::load_or_new(
            fixture2.archive.base.clone(),
            fixture2.archive.object_store.clone(),
            *fixture2.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded2.versions.len(), 1);
        assert!(
            loaded2.versions[0].is_tagged,
            "is_tagged should be preserved"
        );
    }

    #[tokio::test]
    async fn test_archive_truncation() {
        let mut fixture = ArchiveTestFixture::new_with_config(VersionArchiveConfig {
            max_entries: 2,
            ..Default::default()
        })
        .await;

        fixture.archive.add_entries(&[
            create_test_version_archive_entry(1),
            create_test_version_archive_entry(2),
            create_test_version_archive_entry(3),
        ]);
        fixture.archive.flush().await.unwrap();

        assert_eq!(fixture.archive.versions.len(), 2);
        assert_eq!(fixture.archive.versions[0].version, 2);
        assert_eq!(fixture.archive.versions[1].version, 3);
    }

    #[tokio::test]
    async fn test_archive_corruption_graceful_degradation() {
        let mut fixture = ArchiveTestFixture::new().await;
        fixture
            .archive
            .add_entries(&[create_test_version_archive_entry(1)]);
        fixture.archive.flush().await.unwrap();

        let archive_dir = fixture.archive.archive_dir();

        // Corrupt the archive file
        let lance_path = archive_dir.join(archive_filename(1));
        fixture
            .archive
            .object_store
            .put(&lance_path, b"corrupted data")
            .await
            .unwrap();

        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert!(loaded.versions.is_empty());
    }

    #[test]
    fn test_config_from_config() {
        let mut config = HashMap::new();
        config.insert(
            "lance.version_archive.enabled".to_string(),
            "false".to_string(),
        );
        config.insert(
            "lance.version_archive.max_entries".to_string(),
            "100".to_string(),
        );
        config.insert(
            "lance.version_archive.max_archive_files".to_string(),
            "5".to_string(),
        );

        let archive_config = VersionArchiveConfig::from_config(&config);
        assert!(!archive_config.enabled);
        assert_eq!(archive_config.max_entries, 100);
        assert_eq!(archive_config.max_archive_files, 5);
    }

    #[test]
    fn test_config_from_config_defaults() {
        let config = HashMap::new();
        let archive_config = VersionArchiveConfig::from_config(&config);
        assert!(archive_config.enabled);
        assert_eq!(archive_config.max_entries, 10000);
        assert_eq!(archive_config.max_archive_files, 2);
    }

    #[tokio::test]
    async fn test_add_entries_empty() {
        let mut fixture = ArchiveTestFixture::new().await;
        fixture.archive.add_entries(&[]);
        fixture.archive.flush().await.unwrap();

        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert!(loaded.versions.is_empty());
    }

    #[tokio::test]
    async fn test_add_entries_sorts_by_version() {
        let mut fixture = ArchiveTestFixture::new().await;

        fixture.archive.add_entries(&[
            create_test_version_archive_entry(3),
            create_test_version_archive_entry(1),
            create_test_version_archive_entry(2),
        ]);
        fixture.archive.flush().await.unwrap();

        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.versions.len(), 3);
        assert_eq!(loaded.versions[0].version, 1);
        assert_eq!(loaded.versions[1].version, 2);
        assert_eq!(loaded.versions[2].version, 3);
    }

    #[tokio::test]
    async fn test_max_archive_files_cleanup() {
        let mut fixture = ArchiveTestFixture::new_with_config(VersionArchiveConfig {
            max_archive_files: 2,
            ..Default::default()
        })
        .await;

        for i in 1..=4 {
            fixture
                .archive
                .add_entries(&[create_test_version_archive_entry(i)]);
            fixture.archive.flush().await.unwrap();
        }

        let archive_dir = fixture.archive.archive_dir();
        let mut versions = Vec::new();
        let mut stream = fixture.archive.object_store.list(Some(archive_dir));
        while let Some(meta) = stream.next().await.transpose().unwrap() {
            if let Some(filename) = meta.location.filename()
                && let Some(version) = filename
                    .strip_suffix(VERSION_ARCHIVE_FILE_SUFFIX)
                    .and_then(|s| s.parse::<u64>().ok())
            {
                versions.push(version);
            }
        }

        // Verify only 2 files remain
        assert_eq!(versions.len(), 2, "Expected 2 archive files");

        // Verify the newest files are retained (v3 and v4)
        versions.sort();
        assert_eq!(
            versions,
            vec![3, 4],
            "Expected to retain the newest archive files (v3 and v4)"
        );
    }

    #[tokio::test]
    async fn test_scan_all_archives_dedupes_with_newest_winning() {
        let mut fixture = ArchiveTestFixture::new_with_config(VersionArchiveConfig {
            max_archive_files: 2,
            ..Default::default()
        })
        .await;

        fixture.archive.add_entries(&[
            create_test_version_archive_entry(1),
            create_test_version_archive_entry(2),
        ]);
        fixture.archive.flush().await.unwrap();

        fixture.archive.versions[1]
            .transaction_properties
            .insert("source".to_string(), "newer".to_string());
        fixture
            .archive
            .add_entries(&[create_test_version_archive_entry(3)]);
        fixture.archive.flush().await.unwrap();

        let entries = VersionArchive::scan(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].version, 1);
        assert_eq!(entries[1].version, 2);
        assert_eq!(
            entries[1].transaction_properties.get("source"),
            Some(&"newer".to_string()),
            "duplicate version should come from the newest archive file"
        );
        assert_eq!(entries[2].version, 3);
    }

    #[tokio::test]
    async fn test_scan_archive_respects_max_entries() {
        let mut fixture = ArchiveTestFixture::new_with_config(VersionArchiveConfig {
            max_entries: 2,
            max_archive_files: 2,
            ..Default::default()
        })
        .await;

        for version in 1..=3 {
            fixture
                .archive
                .add_entries(&[create_test_version_archive_entry(version)]);
            fixture.archive.flush().await.unwrap();
        }

        let entries = VersionArchive::scan(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].version, 2);
        assert_eq!(entries[1].version, 3);
    }

    #[tokio::test]
    async fn test_load_newest_valid_archive() {
        let mut fixture = ArchiveTestFixture::new().await;

        fixture
            .archive
            .add_entries(&[create_test_version_archive_entry(1)]);
        fixture.archive.flush().await.unwrap();

        fixture
            .archive
            .add_entries(&[create_test_version_archive_entry(2)]);
        fixture.archive.flush().await.unwrap();

        let archive_dir = fixture.archive.archive_dir();

        // Corrupt v2 archive to ensure we fall back to v1
        let v2_path = archive_dir.join(archive_filename(2));
        fixture
            .archive
            .object_store
            .put(&v2_path, b"corrupted")
            .await
            .unwrap();

        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.latest_version(), 1);
    }

    #[tokio::test]
    async fn test_transaction_properties_size_limit() {
        // Test that transaction_properties exceeding 10KB is truncated
        // and the raw value is preserved for debugging when parsing fails
        let mut fixture = ArchiveTestFixture::new().await;

        // Create a large transaction_properties (exceeds 10KB)
        let large_value = "x".repeat(15 * 1024); // 15KB
        let mut summary = create_test_version_archive_entry(1);
        summary
            .transaction_properties
            .insert("large_key".to_string(), large_value);

        fixture.archive.add_entries(&[summary]);
        fixture.archive.flush().await.unwrap();

        // Load and verify
        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.versions.len(), 1);
        let loaded_props = &loaded.versions[0].transaction_properties;

        // When JSON is truncated and becomes invalid, it falls back to preserving
        // the raw value in the "transaction_properties" key
        assert!(
            loaded_props.contains_key("transaction_properties"),
            "Should preserve raw value when truncated JSON parsing fails"
        );

        // Verify the raw value contains indicators of truncation
        let raw_value = loaded_props.get("transaction_properties").unwrap();
        assert!(
            raw_value.contains("_truncated"),
            "Raw value should contain _truncated marker"
        );
        assert!(
            raw_value.contains("_size"),
            "Raw value should contain _size marker"
        );
        assert!(
            raw_value.contains("_data"),
            "Raw value should contain _data marker"
        );
    }

    #[tokio::test]
    async fn test_unicode_transaction_properties_size_limit() {
        let mut fixture = ArchiveTestFixture::new().await;

        let mut summary = create_test_version_archive_entry(1);
        summary
            .transaction_properties
            .insert("unicode".to_string(), "数据".repeat(8 * 1024));

        fixture.archive.add_entries(&[summary]);
        fixture.archive.flush().await.unwrap();

        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.versions.len(), 1);
        assert!(
            loaded.versions[0]
                .transaction_properties
                .contains_key("transaction_properties"),
            "truncated unicode JSON should be preserved as raw fallback"
        );
    }

    #[tokio::test]
    async fn test_transaction_properties_under_limit() {
        // Test that transaction_properties under 10KB is preserved intact
        let mut fixture = ArchiveTestFixture::new().await;

        // Create a small transaction_properties (under 10KB)
        let mut summary = create_test_version_archive_entry(1);
        summary
            .transaction_properties
            .insert("key1".to_string(), "value1".to_string());
        summary
            .transaction_properties
            .insert("key2".to_string(), "value2".to_string());

        fixture.archive.add_entries(&[summary.clone()]);
        fixture.archive.flush().await.unwrap();

        // Load and verify
        let loaded = VersionArchive::load_or_new(
            fixture.archive.base.clone(),
            fixture.archive.object_store.clone(),
            *fixture.archive.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.versions.len(), 1);
        let loaded_props = &loaded.versions[0].transaction_properties;

        // Should preserve original keys
        assert_eq!(loaded_props.get("key1"), Some(&"value1".to_string()));
        assert_eq!(loaded_props.get("key2"), Some(&"value2".to_string()));
        assert!(!loaded_props.contains_key("_truncated"));
    }
}
