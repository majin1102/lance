// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Version Archive module
//!
//! This module provides version archival functionality for preserving version metadata
//! when manifests are cleaned up.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::StreamExt;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_table::format::{pb_archive, ManifestSummary};
use object_store::path::Path;
use prost::Message;
use snafu::location;

pub const ARCHIVE_DIR: &str = "_archive";
pub const VERSION_ARCHIVE_SUBDIR: &str = "versions";
pub const VERSION_ARCHIVE_FILE_SUFFIX: &str = ".binpb";

// Version number inversion for file naming (consistent with manifest V2)
const INVERTED_VERSION_OFFSET: u64 = u64::MAX;

/// Convert version to inverted version for file naming
pub fn to_inverted_version(version: u64) -> u64 {
    INVERTED_VERSION_OFFSET - version
}

/// Convert inverted version back to original version
pub fn from_inverted_version(inverted: u64) -> u64 {
    INVERTED_VERSION_OFFSET - inverted
}

/// VersionArchive configuration
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
    pub fn from_config(config: &HashMap<String, String>) -> Self {
        Self {
            enabled: config
                .get("lance.version_archive.enabled")
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            max_entries: config
                .get("lance.version_archive.max_entries")
                .and_then(|v| v.parse().ok())
                .unwrap_or(10000),
            max_archive_files: config
                .get("lance.version_archive.max_archive_files")
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
        }
    }
}

/// Version summary with flattened manifest statistics
#[derive(Debug, Clone, PartialEq)]
pub struct VersionSummary {
    pub version: u64,
    pub timestamp_millis: i64,
    pub manifest_summary: ManifestSummary,
    pub is_tagged: bool,
    pub is_cleaned_up: bool,
    pub transaction_uuid: Option<String>,
    pub read_version: Option<u64>,
    pub operation_type: Option<String>,
    pub transaction_properties: HashMap<String, String>,
}

impl From<&VersionSummary> for pb_archive::VersionSummary {
    fn from(s: &VersionSummary) -> Self {
        Self {
            version: s.version,
            timestamp_millis: s.timestamp_millis,
            total_fragments: s.manifest_summary.total_fragments,
            total_data_files: s.manifest_summary.total_data_files,
            total_files_size: s.manifest_summary.total_files_size,
            total_deletion_files: s.manifest_summary.total_deletion_files,
            total_data_file_rows: s.manifest_summary.total_data_file_rows,
            total_deletion_file_rows: s.manifest_summary.total_deletion_file_rows,
            total_rows: s.manifest_summary.total_rows,
            is_tagged: s.is_tagged,
            is_cleaned_up: s.is_cleaned_up,
            transaction_uuid: s.transaction_uuid.clone(),
            read_version: s.read_version,
            operation_type: s.operation_type.clone(),
            transaction_properties: s.transaction_properties.clone(),
        }
    }
}

impl From<pb_archive::VersionSummary> for VersionSummary {
    fn from(proto: pb_archive::VersionSummary) -> Self {
        Self {
            version: proto.version,
            timestamp_millis: proto.timestamp_millis,
            manifest_summary: ManifestSummary {
                total_fragments: proto.total_fragments,
                total_data_files: proto.total_data_files,
                total_files_size: proto.total_files_size,
                total_deletion_files: proto.total_deletion_files,
                total_data_file_rows: proto.total_data_file_rows,
                total_deletion_file_rows: proto.total_deletion_file_rows,
                total_rows: proto.total_rows,
            },
            is_tagged: proto.is_tagged,
            is_cleaned_up: proto.is_cleaned_up,
            transaction_uuid: proto.transaction_uuid,
            read_version: proto.read_version,
            operation_type: proto.operation_type,
            transaction_properties: proto.transaction_properties,
        }
    }
}

/// Version archive with persistence capability
#[derive(Debug, Clone)]
pub struct VersionArchive {
    pub versions: Vec<VersionSummary>,
    pub latest_version_number: u64,
    pub dataset_created_millis: u64,
    pub created_at_millis: u64,
    config: VersionArchiveConfig,
    base: Path,
    object_store: Arc<ObjectStore>,
}

impl From<&VersionArchive> for pb_archive::VersionArchive {
    fn from(archive: &VersionArchive) -> Self {
        Self {
            versions: archive.versions.iter().map(|v| v.into()).collect(),
            latest_version_number: archive.latest_version_number,
            dataset_created_millis: archive.dataset_created_millis as i64,
            created_at_millis: archive.created_at_millis as i64,
        }
    }
}

impl VersionArchive {
    pub fn archive_dir(&self) -> Path {
        self.base.child(ARCHIVE_DIR).child(VERSION_ARCHIVE_SUBDIR)
    }

    async fn list_archive_files(
        object_store: &ObjectStore,
        archive_dir: &Path,
    ) -> Result<Vec<(u64, Path)>> {
        let mut archives = Vec::new();
        let mut stream = object_store.list(Some(archive_dir.clone()));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if let Some(filename) = meta.location.filename() {
                if let Some(inverted) = filename
                    .strip_suffix(VERSION_ARCHIVE_FILE_SUFFIX)
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    let version = from_inverted_version(inverted);
                    archives.push((version, meta.location));
                }
            }
        }
        archives.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(archives)
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
        let archive_dir = base.child(ARCHIVE_DIR).child(VERSION_ARCHIVE_SUBDIR);
        let archives = Self::list_archive_files(&object_store, &archive_dir).await?;

        for (_, path) in archives {
            match Self::load_from_path(&base, object_store.clone(), &path, config).await {
                Ok(archive) => return Ok(archive),
                Err(e) => {
                    tracing::warn!("Failed to load archive file {}: {}", path, e);
                }
            }
        }

        Ok(Self {
            versions: Vec::new(),
            latest_version_number: 0,
            dataset_created_millis: 0,
            created_at_millis: chrono::Utc::now().timestamp_millis() as u64,
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
        let archive_dir = base.child(ARCHIVE_DIR).child(VERSION_ARCHIVE_SUBDIR);
        let archives = Self::list_archive_files(&object_store, &archive_dir).await?;
        for (_, path) in archives {
            match Self::load_from_path(&base, object_store.clone(), &path, config).await {
                Ok(archive) => return Ok(Some(archive)),
                Err(e) => {
                    tracing::warn!("Failed to load archive file {}: {}", path, e);
                }
            }
        }
        Ok(None)
    }

    async fn load_from_path(
        base: &Path,
        path: &Path,
        object_store: Arc<ObjectStore>,
        config: VersionArchiveConfig,
    ) -> Result<Self> {
        let reader = object_store.open(path).await?;
        let data = reader.get_all().await?;
        let proto = pb_archive::VersionArchive::decode(data.as_ref()).map_err(|e| {
            Error::invalid_input(format!("Failed to decode archive: {}", e), location!())
        })?;

        let versions: Vec<VersionSummary> = proto.versions.into_iter().map(|v| v.into()).collect();
        Ok(Self {
            versions,
            latest_version_number: proto.latest_version_number,
            dataset_created_millis: proto.dataset_created_millis as u64,
            created_at_millis: proto.created_at_millis as u64,
            config,
            base: base.clone(),
            object_store,
        })
    }

    /// Add new version summaries to the archive
    /// Summaries are sorted by version before adding
    pub fn add_summaries(&mut self, summaries: &[VersionSummary]) {
        if summaries.is_empty() {
            return;
        }
        self.versions.extend(summaries.iter().cloned());
    }

    /// Finalize the archive before flushing
    fn finalize_summaries(&mut self) {
        if self.versions.is_empty() {
            return;
        }

        self.versions.sort_by_key(|v| v.version);
        if self.dataset_created_millis == 0 {
            self.dataset_created_millis = self
                .versions
                .first()
                .map(|v| v.timestamp_millis as u64)
                .unwrap_or(0);
        }

        if self.versions.len() > self.config.max_entries {
            let remove_count = self.versions.len() - self.config.max_entries;
            self.versions.drain(0..remove_count);
        }

        self.latest_version_number = self.versions.iter().map(|v| v.version).max().unwrap_or(0);
        self.created_at_millis = chrono::Utc::now().timestamp_millis() as u64;
    }

    /// Flush the archive to storage
    pub async fn flush(&mut self) -> Result<()> {
        self.finalize_summaries();

        if self.versions.is_empty() {
            return Ok(());
        }

        let archive_dir = self.archive_dir();
        let inverted = to_inverted_version(self.latest_version_number);
        let filename = format!("{:020}{}", inverted, VERSION_ARCHIVE_FILE_SUFFIX);
        let path = archive_dir.child(filename);

        let proto: pb_archive::VersionArchive = (&*self).into();
        let mut bytes = Vec::new();
        proto.encode(&mut bytes).map_err(|e| {
            Error::invalid_input(format!("Failed to encode archive: {}", e), location!())
        })?;
        self.object_store.put(&path, &bytes).await?;

        self.cleanup_old_archives().await?;

        Ok(())
    }

    async fn cleanup_old_archives(&self) -> Result<()> {
        let archive_dir = self.archive_dir();
        let archives = Self::list_archive_files(&self.object_store, &archive_dir).await?;

        if archives.len() > self.config.max_archive_files {
            let delete_count = archives.len() - self.config.max_archive_files;
            for (version, _) in archives.iter().take(delete_count) {
                let inverted = to_inverted_version(*version);
                let filename = format!("{:020}{}", inverted, VERSION_ARCHIVE_FILE_SUFFIX);
                let path = self.archive_dir().child(filename);
                if let Err(e) = self.object_store.delete(&path).await {
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

    fn create_test_version_summary(version: u64) -> VersionSummary {
        VersionSummary {
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
            is_cleaned_up: false,
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
    async fn test_archive_add_summaries_and_flush() {
        let mut fixture = ArchiveTestFixture::new().await;
        assert!(
            fixture.archive.is_enabled(),
            "Archive should be enabled by default"
        );

        fixture.archive.add_summaries(&[
            create_test_version_summary(1),
            create_test_version_summary(2),
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
            .add_summaries(&[create_test_version_summary(1)]);
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
        let mut summary = create_test_version_summary(2);
        summary.is_tagged = true;
        summary.is_cleaned_up = true;
        fixture2.archive.add_summaries(&[summary]);
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
        assert!(
            loaded2.versions[0].is_cleaned_up,
            "is_cleaned_up should be preserved"
        );
    }

    #[tokio::test]
    async fn test_archive_truncation() {
        let mut fixture = ArchiveTestFixture::new_with_config(VersionArchiveConfig {
            max_entries: 2,
            ..Default::default()
        })
        .await;

        fixture.archive.add_summaries(&[
            create_test_version_summary(1),
            create_test_version_summary(2),
            create_test_version_summary(3),
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
            .add_summaries(&[create_test_version_summary(1)]);
        fixture.archive.flush().await.unwrap();

        let archive_dir = fixture.archive.archive_dir();
        let path = archive_dir.child(format!("{:020}.binpb", to_inverted_version(1)));
        fixture
            .archive
            .object_store
            .put(&path, b"corrupted data")
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
    async fn test_add_summaries_empty() {
        let mut fixture = ArchiveTestFixture::new().await;
        fixture.archive.add_summaries(&[]);
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
    async fn test_add_summaries_sorts_by_version() {
        let mut fixture = ArchiveTestFixture::new().await;

        fixture.archive.add_summaries(&[
            create_test_version_summary(3),
            create_test_version_summary(1),
            create_test_version_summary(2),
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
                .add_summaries(&[create_test_version_summary(i)]);
            fixture.archive.flush().await.unwrap();
        }

        let archive_dir = fixture.archive.archive_dir();
        let mut count = 0;
        let mut stream = fixture.archive.object_store.list(Some(archive_dir));
        while stream.next().await.transpose().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_load_newest_valid_archive() {
        let mut fixture = ArchiveTestFixture::new().await;

        fixture
            .archive
            .add_summaries(&[create_test_version_summary(1)]);
        fixture.archive.flush().await.unwrap();

        fixture
            .archive
            .add_summaries(&[create_test_version_summary(2)]);
        fixture.archive.flush().await.unwrap();

        let v2_path = fixture
            .archive
            .archive_dir()
            .child(format!("{:020}.binpb", to_inverted_version(2)));
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

    #[test]
    fn test_version_inversion() {
        assert_eq!(from_inverted_version(to_inverted_version(1)), 1);
        assert_eq!(from_inverted_version(to_inverted_version(100)), 100);
        assert_eq!(
            from_inverted_version(to_inverted_version(u64::MAX)),
            u64::MAX
        );
        assert!(to_inverted_version(1) > to_inverted_version(2));
    }
}
