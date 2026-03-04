// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Version Checkpoint module
//!
//! This module provides version checkpoint functionality for preserving version metadata
//! when manifests are cleaned up.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::StreamExt;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_table::format::{pb_checkpoint, ManifestSummary};
use object_store::path::Path;
use prost::Message;
use snafu::location;

pub const CHECKPOINT_DIR: &str = "_checkpoint";
pub const VERSION_CHECKPOINT_FILE_SUFFIX: &str = ".binpb";

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

/// Checkpoint configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointConfig {
    /// Whether VersionCheckpoint is enabled
    pub enabled: bool,

    /// Maximum number of version entries to retain in a single checkpoint file
    pub max_entries: usize,

    /// Maximum number of checkpoint files to retain
    pub max_checkpoint_files: usize,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 10000,
            max_checkpoint_files: 2,
        }
    }
}

impl CheckpointConfig {
    /// Read configuration from manifest config
    pub fn from_config(config: &HashMap<String, String>) -> Self {
        Self {
            enabled: config
                .get("lance.version_checkpoint.enabled")
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            max_entries: config
                .get("lance.version_checkpoint.max_entries")
                .and_then(|v| v.parse().ok())
                .unwrap_or(10000),
            max_checkpoint_files: config
                .get("lance.version_checkpoint.max_checkpoint_files")
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

impl From<&VersionSummary> for pb_checkpoint::VersionSummary {
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

impl From<pb_checkpoint::VersionSummary> for VersionSummary {
    fn from(proto: pb_checkpoint::VersionSummary) -> Self {
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

/// Version checkpoint with persistence capability
#[derive(Debug, Clone)]
pub struct VersionCheckpoint {
    pub versions: Vec<VersionSummary>,
    pub latest_version_number: u64,
    pub dataset_created_millis: u64,
    pub created_at_millis: u64,
    config: CheckpointConfig,
    base: Path,
    object_store: Arc<ObjectStore>,
}

impl From<&VersionCheckpoint> for pb_checkpoint::VersionCheckpoint {
    fn from(checkpoint: &VersionCheckpoint) -> Self {
        Self {
            versions: checkpoint.versions.iter().map(|v| v.into()).collect(),
            latest_version_number: checkpoint.latest_version_number,
            dataset_created_millis: checkpoint.dataset_created_millis as i64,
            created_at_millis: checkpoint.created_at_millis as i64,
        }
    }
}

impl VersionCheckpoint {
    pub fn checkpoint_dir(&self) -> Path {
        self.base.child(CHECKPOINT_DIR)
    }

    async fn list_checkpoint_files(
        object_store: &ObjectStore,
        checkpoint_dir: &Path,
    ) -> Result<Vec<(u64, Path)>> {
        let mut checkpoints = Vec::new();
        let mut stream = object_store.list(Some(checkpoint_dir.clone()));
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if let Some(filename) = meta.location.filename() {
                if let Some(inverted) = filename
                    .strip_suffix(VERSION_CHECKPOINT_FILE_SUFFIX)
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    let version = from_inverted_version(inverted);
                    checkpoints.push((version, meta.location));
                }
            }
        }
        checkpoints.sort_by(|a, b| b.0.cmp(&a.0));
        Ok(checkpoints)
    }

    /// Load the latest checkpoint from storage, or create a new empty one
    ///
    /// Tries to load from the newest checkpoint file. If corrupted, tries older files.
    /// If no valid checkpoint exists, creates a new empty one.
    pub async fn load_or_new(
        base: Path,
        object_store: Arc<ObjectStore>,
        config: CheckpointConfig,
    ) -> Result<Self> {
        let checkpoint_dir = base.child(CHECKPOINT_DIR);
        let checkpoints = Self::list_checkpoint_files(&object_store, &checkpoint_dir).await?;

        for (_, path) in checkpoints {
            match Self::load_from_path(&base, object_store.clone(), &path, config).await {
                Ok(checkpoint) => return Ok(checkpoint),
                Err(e) => {
                    tracing::warn!("Failed to load checkpoint file {}: {}", path, e);
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

    /// Load the latest checkpoint from storage
    pub async fn load_latest(
        base: Path,
        object_store: Arc<ObjectStore>,
        config: CheckpointConfig,
    ) -> Result<Option<Self>> {
        let checkpoint_dir = base.child(CHECKPOINT_DIR);
        let checkpoints = Self::list_checkpoint_files(&object_store, &checkpoint_dir).await?;
        for (_, path) in checkpoints {
            match Self::load_from_path(&base, object_store.clone(), &path, config).await {
                Ok(checkpoint) => return Ok(Some(checkpoint)),
                Err(e) => {
                    tracing::warn!("Failed to load checkpoint file {}: {}", path, e);
                }
            }
        }
        Ok(None)
    }

    async fn load_from_path(
        base: &Path,
        object_store: Arc<ObjectStore>,
        path: &Path,
        config: CheckpointConfig,
    ) -> Result<Self> {
        let reader = object_store.open(path).await?;
        let data = reader.get_all().await?;
        let proto = pb_checkpoint::VersionCheckpoint::decode(data.as_ref()).map_err(|e| {
            Error::invalid_input(format!("Failed to decode checkpoint: {}", e), location!())
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

    /// Add new version summaries to the checkpoint
    /// Summaries are sorted by version before adding
    pub fn add_summaries(&mut self, summaries: &[VersionSummary]) {
        if summaries.is_empty() {
            return;
        }
        self.versions.extend(summaries.iter().cloned());
    }

    /// Finalize the checkpoint before flushing
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

    /// Flush the checkpoint to storage
    pub async fn flush(&mut self) -> Result<()> {
        self.finalize_summaries();

        if self.versions.is_empty() {
            return Ok(());
        }

        let checkpoint_dir = self.checkpoint_dir();
        let inverted = to_inverted_version(self.latest_version_number);
        let filename = format!("{:020}{}", inverted, VERSION_CHECKPOINT_FILE_SUFFIX);
        let path = checkpoint_dir.child(filename);

        let proto: pb_checkpoint::VersionCheckpoint = (&*self).into();
        let mut bytes = Vec::new();
        proto.encode(&mut bytes).map_err(|e| {
            Error::invalid_input(format!("Failed to encode checkpoint: {}", e), location!())
        })?;
        self.object_store.put(&path, &bytes).await?;

        self.cleanup_old_checkpoints().await?;

        Ok(())
    }

    async fn cleanup_old_checkpoints(&self) -> Result<()> {
        let checkpoint_dir = self.checkpoint_dir();
        let checkpoints = Self::list_checkpoint_files(&self.object_store, &checkpoint_dir).await?;

        if checkpoints.len() > self.config.max_checkpoint_files {
            let delete_count = checkpoints.len() - self.config.max_checkpoint_files;
            for (version, _) in checkpoints.iter().take(delete_count) {
                let inverted = to_inverted_version(*version);
                let filename = format!("{:020}{}", inverted, VERSION_CHECKPOINT_FILE_SUFFIX);
                let path = self.checkpoint_dir().child(filename);
                if let Err(e) = self.object_store.delete(&path).await {
                    tracing::warn!("Failed to delete old checkpoint file {}: {}", path, e);
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
    fn config(&self) -> &CheckpointConfig {
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

    struct CheckpointTestFixture {
        checkpoint: VersionCheckpoint,
    }

    impl CheckpointTestFixture {
        async fn new() -> Self {
            Self::new_with_config(CheckpointConfig::default()).await
        }

        async fn new_with_config(config: CheckpointConfig) -> Self {
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
            let checkpoint =
                VersionCheckpoint::load_or_new(Path::from("test"), Arc::new(object_store), config)
                    .await
                    .unwrap();
            Self { checkpoint }
        }
    }

    #[tokio::test]
    async fn test_checkpoint_load_or_new_empty() {
        let fixture = CheckpointTestFixture::new().await;
        assert!(fixture.checkpoint.versions.is_empty());
        assert_eq!(fixture.checkpoint.latest_version(), 0);
    }

    #[tokio::test]
    async fn test_checkpoint_add_summaries_and_flush() {
        let mut fixture = CheckpointTestFixture::new().await;
        assert!(fixture.checkpoint.is_enabled(), "Checkpoint should be enabled by default");

        fixture.checkpoint.add_summaries(&[
            create_test_version_summary(1),
            create_test_version_summary(2),
        ]);
        fixture.checkpoint.flush().await.unwrap();

        assert_eq!(fixture.checkpoint.versions.len(), 2);
        assert_eq!(fixture.checkpoint.latest_version(), 2);

        // Test disabled checkpoint - is_enabled() returns false
        let disabled_fixture = CheckpointTestFixture::new_with_config(CheckpointConfig {
            enabled: false,
            ..Default::default()
        })
        .await;
        assert!(
            !disabled_fixture.checkpoint.is_enabled(),
            "Checkpoint should be disabled when enabled=false"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_load_existing() {
        let mut fixture = CheckpointTestFixture::new().await;
        fixture
            .checkpoint
            .add_summaries(&[create_test_version_summary(1)]);
        fixture.checkpoint.flush().await.unwrap();

        let loaded = VersionCheckpoint::load_or_new(
            fixture.checkpoint.base.clone(),
            fixture.checkpoint.object_store.clone(),
            *fixture.checkpoint.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.versions.len(), 1);
        assert_eq!(loaded.versions[0].version, 1);
        assert_eq!(loaded.latest_version(), 1);

        // Test is_tagged field serialization/deserialization
        let mut fixture2 = CheckpointTestFixture::new().await;
        let mut summary = create_test_version_summary(2);
        summary.is_tagged = true;
        summary.is_cleaned_up = true;
        fixture2.checkpoint.add_summaries(&[summary]);
        fixture2.checkpoint.flush().await.unwrap();

        let loaded2 = VersionCheckpoint::load_or_new(
            fixture2.checkpoint.base.clone(),
            fixture2.checkpoint.object_store.clone(),
            *fixture2.checkpoint.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded2.versions.len(), 1);
        assert!(loaded2.versions[0].is_tagged, "is_tagged should be preserved");
        assert!(
            loaded2.versions[0].is_cleaned_up,
            "is_cleaned_up should be preserved"
        );
    }

    #[tokio::test]
    async fn test_checkpoint_truncation() {
        let mut fixture = CheckpointTestFixture::new_with_config(CheckpointConfig {
            max_entries: 2,
            ..Default::default()
        })
        .await;

        fixture.checkpoint.add_summaries(&[
            create_test_version_summary(1),
            create_test_version_summary(2),
            create_test_version_summary(3),
        ]);
        fixture.checkpoint.flush().await.unwrap();

        assert_eq!(fixture.checkpoint.versions.len(), 2);
        assert_eq!(fixture.checkpoint.versions[0].version, 2);
        assert_eq!(fixture.checkpoint.versions[1].version, 3);
    }

    #[tokio::test]
    async fn test_checkpoint_corruption_graceful_degradation() {
        let mut fixture = CheckpointTestFixture::new().await;
        fixture
            .checkpoint
            .add_summaries(&[create_test_version_summary(1)]);
        fixture.checkpoint.flush().await.unwrap();

        let checkpoint_dir = fixture.checkpoint.checkpoint_dir();
        let path = checkpoint_dir.child(format!("{:020}.binpb", to_inverted_version(1)));
        fixture
            .checkpoint
            .object_store
            .put(&path, b"corrupted data")
            .await
            .unwrap();

        let loaded = VersionCheckpoint::load_or_new(
            fixture.checkpoint.base.clone(),
            fixture.checkpoint.object_store.clone(),
            *fixture.checkpoint.config(),
        )
        .await
        .unwrap();

        assert!(loaded.versions.is_empty());
    }

    #[test]
    fn test_config_from_config() {
        let mut config = HashMap::new();
        config.insert(
            "lance.version_checkpoint.enabled".to_string(),
            "false".to_string(),
        );
        config.insert(
            "lance.version_checkpoint.max_entries".to_string(),
            "100".to_string(),
        );
        config.insert(
            "lance.version_checkpoint.max_checkpoint_files".to_string(),
            "5".to_string(),
        );

        let checkpoint_config = CheckpointConfig::from_config(&config);
        assert!(!checkpoint_config.enabled);
        assert_eq!(checkpoint_config.max_entries, 100);
        assert_eq!(checkpoint_config.max_checkpoint_files, 5);
    }

    #[test]
    fn test_config_from_config_defaults() {
        let config = HashMap::new();
        let checkpoint_config = CheckpointConfig::from_config(&config);
        assert!(checkpoint_config.enabled);
        assert_eq!(checkpoint_config.max_entries, 10000);
        assert_eq!(checkpoint_config.max_checkpoint_files, 2);
    }

    #[tokio::test]
    async fn test_add_summaries_empty() {
        let mut fixture = CheckpointTestFixture::new().await;
        fixture.checkpoint.add_summaries(&[]);
        fixture.checkpoint.flush().await.unwrap();

        let loaded = VersionCheckpoint::load_or_new(
            fixture.checkpoint.base.clone(),
            fixture.checkpoint.object_store.clone(),
            *fixture.checkpoint.config(),
        )
        .await
        .unwrap();

        assert!(loaded.versions.is_empty());
    }

    #[tokio::test]
    async fn test_add_summaries_sorts_by_version() {
        let mut fixture = CheckpointTestFixture::new().await;

        fixture.checkpoint.add_summaries(&[
            create_test_version_summary(3),
            create_test_version_summary(1),
            create_test_version_summary(2),
        ]);
        fixture.checkpoint.flush().await.unwrap();

        let loaded = VersionCheckpoint::load_or_new(
            fixture.checkpoint.base.clone(),
            fixture.checkpoint.object_store.clone(),
            *fixture.checkpoint.config(),
        )
        .await
        .unwrap();

        assert_eq!(loaded.versions.len(), 3);
        assert_eq!(loaded.versions[0].version, 1);
        assert_eq!(loaded.versions[1].version, 2);
        assert_eq!(loaded.versions[2].version, 3);
    }

    #[tokio::test]
    async fn test_max_checkpoint_files_cleanup() {
        let mut fixture = CheckpointTestFixture::new_with_config(CheckpointConfig {
            max_checkpoint_files: 2,
            ..Default::default()
        })
        .await;

        for i in 1..=4 {
            fixture
                .checkpoint
                .add_summaries(&[create_test_version_summary(i)]);
            fixture.checkpoint.flush().await.unwrap();
        }

        let checkpoint_dir = fixture.checkpoint.checkpoint_dir();
        let mut count = 0;
        let mut stream = fixture.checkpoint.object_store.list(Some(checkpoint_dir));
        while stream.next().await.transpose().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_load_newest_valid_checkpoint() {
        let mut fixture = CheckpointTestFixture::new().await;

        fixture
            .checkpoint
            .add_summaries(&[create_test_version_summary(1)]);
        fixture.checkpoint.flush().await.unwrap();

        fixture
            .checkpoint
            .add_summaries(&[create_test_version_summary(2)]);
        fixture.checkpoint.flush().await.unwrap();

        let v2_path = fixture
            .checkpoint
            .checkpoint_dir()
            .child(format!("{:020}.binpb", to_inverted_version(2)));
        fixture
            .checkpoint
            .object_store
            .put(&v2_path, b"corrupted")
            .await
            .unwrap();

        let loaded = VersionCheckpoint::load_or_new(
            fixture.checkpoint.base.clone(),
            fixture.checkpoint.object_store.clone(),
            *fixture.checkpoint.config(),
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
