// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use object_store::path::Path;

/// The kind of physical file represented by a PathSpec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathKind {
    /// A data file under the dataset's data directory.
    Data,
    /// A deletion file under the dataset's _deletions directory.
    Deletion,
    /// An index file under the dataset's _indices directory.
    Index,
    /// An external row-id file.
    RowId,
}

/// Minimal path specification used by deep_clone.
///
/// - `path` is dataset-relative for internal files (e.g., "data/{...}", "_deletions/{...}", "_indices/{uuid}/{...}").
/// - For external row-id files, `path` may be an absolute URI or fully-qualified object store path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathSpec {
    pub path_kind: PathKind,
    pub relative_path: String,
    pub base_path: Path,
}

impl PathSpec {
    /// Join a dataset-relative path to a root base without percent-encoding '/'
    fn absolute_path_with_base(&self, base: &Path) -> Path {
        let mut path = match self.path_kind {
            PathKind::Data => base.child("data"),
            PathKind::Deletion => base.child("_deletions"),
            PathKind::Index => base.child("_indices"),
            PathKind::RowId => base.clone(),
        };
        for seg in self.relative_path.split('/') {
            if !seg.is_empty() {
                path = path.child(seg);
            }
        }
        path
    }

    fn absolute_path(&self) -> Path {
        self.absolute_path_with_base(&self.base_path)
    }

    /// Produce (source_path, target_path) for server-side copy.
    /// Returns None for external RowId paths (absolute URIs), which should not be copied.
    pub fn produce_copy_pair(
        &self,
        target_root: &Path,
    ) -> Option<(Path, Path)> {
        match self.path_kind {
            PathKind::Data | PathKind::Deletion | PathKind::Index => {
                Some((self.absolute_path(), self.absolute_path_with_base(target_root)))
            }
            PathKind::RowId => {
                // If the row-id file is absolute, we skip copying.
                if self.relative_path.contains("://") || self.relative_path.starts_with('/') {
                    None
                } else {
                    Some((self.absolute_path(), self.absolute_path_with_base(target_root)))
                }
            }
        }
    }
}
