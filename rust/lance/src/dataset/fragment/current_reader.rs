// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Current-format file reads from explicit schema, storage, and cache inputs.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use lance_core::Result;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema;
use lance_encoding::decoder::DecoderPlugins;
use lance_file::reader::{
    CachedFileMetadata, FileMetadataIndex, FileReaderOptions, ProjectedFileReader,
};
use lance_file::version::ConcreteFileVersion;
use lance_file::{LanceEncodingsIo, versions as file_versions};
use lance_io::scheduler::FileScheduler;
use lance_table::format::DataFile;

use super::{
    FileMetadataCacheKey, FileMetadataIndexCacheKey, GenericFileReader, MetadataMode, NullReader,
    v2_adapter,
};

pub(super) struct CurrentFileReadContext<'a> {
    data_file: &'a DataFile,
    data_file_schema: Arc<Schema>,
    projection: Arc<Schema>,
    physical_rows: Option<usize>,
}

impl<'a> CurrentFileReadContext<'a> {
    /// Resolve projection before opening a file so excluded files perform no I/O.
    pub(super) fn try_new(
        full_schema: &Schema,
        data_file: &'a DataFile,
        projection: Option<&Schema>,
        physical_rows: Option<usize>,
    ) -> Result<Option<Self>> {
        let data_file_schema = Arc::new(data_file.schema(full_schema));
        let projection = projection.unwrap_or(full_schema);
        let projection = Arc::new(projection.intersection_ignore_types(data_file_schema.as_ref())?);
        if projection.fields.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            data_file,
            data_file_schema,
            projection,
            physical_rows,
        }))
    }

    pub(super) async fn open(
        self,
        file_scheduler: FileScheduler,
        reader_priority: u32,
        metadata_cache: LanceCache,
        file_reader_options: FileReaderOptions,
        metadata_mode: MetadataMode,
    ) -> Result<Box<dyn GenericFileReader>> {
        let path = file_scheduler.reader().path().clone();
        let field_id_to_column_idx = Arc::new(BTreeMap::from_iter(
            self.data_file
                .fields
                .iter()
                .copied()
                .zip(self.data_file.column_indices.iter().copied())
                .filter_map(|(field_id, column_index)| {
                    (column_index >= 0).then_some((field_id as u32, column_index as u32))
                }),
        ));
        let file_version = self.data_file.file_version()?;
        let physical_schema = if matches!(
            file_version,
            ConcreteFileVersion::V2_0 | ConcreteFileVersion::V2_1
        ) {
            v2_adapter::legacy_blob_read_schema(&self.projection)
        } else {
            self.projection.as_ref().clone()
        };
        let reader_projection = file_versions::reader_projection_from_field_ids(
            file_version,
            &physical_schema,
            field_id_to_column_idx.as_ref(),
        )?;
        let prefer_indexed = metadata_mode == MetadataMode::LazyAllowed
            && reader_projection.column_indices.len().saturating_mul(4)
                < self
                    .data_file
                    .column_indices
                    .iter()
                    .filter(|column_index| **column_index >= 0)
                    .count();
        let known_schema = self
            .physical_rows
            .map(|num_rows| (self.data_file_schema.clone(), num_rows as u64));
        let encodings_io = Arc::new(
            LanceEncodingsIo::new(file_scheduler.clone())
                .with_read_chunk_size(file_reader_options.read_chunk_size),
        );
        let reader = file_versions::open_projected_reader(
            file_version,
            &reader_projection,
            prefer_indexed,
            || async {
                let metadata_index =
                    get_file_metadata_index(&file_scheduler, &metadata_cache, known_schema.clone())
                        .await?;
                if (reader_projection.column_indices.len() as u32).saturating_mul(4)
                    >= metadata_index.num_columns()
                {
                    return Ok(None);
                }
                Ok(Some(
                    ProjectedFileReader::try_open_with_metadata_index(
                        encodings_io.clone(),
                        path.clone(),
                        Some(reader_projection.clone()),
                        Arc::<DecoderPlugins>::default(),
                        metadata_index,
                        &metadata_cache,
                        file_reader_options.clone(),
                    )
                    .await?,
                ))
            },
            || async {
                let file_metadata = get_file_metadata(&file_scheduler, &metadata_cache).await?;
                ProjectedFileReader::try_open_with_file_metadata(
                    encodings_io.clone(),
                    path.clone(),
                    None,
                    Arc::<DecoderPlugins>::default(),
                    file_metadata,
                    &metadata_cache,
                    file_reader_options.clone(),
                )
                .await
            },
        )
        .await?;
        Ok(Box::new(v2_adapter::Reader::new(
            Arc::new(reader),
            self.projection,
            field_id_to_column_idx,
            reader_priority,
            file_scheduler,
        )))
    }
}

pub(super) async fn get_file_metadata(
    file_scheduler: &FileScheduler,
    cache: &LanceCache,
) -> Result<Arc<CachedFileMetadata>> {
    cache
        .get_or_insert_with_key(FileMetadataCacheKey, || async {
            lance_file::reader::FileReader::read_all_metadata(file_scheduler).await
        })
        .await
}

pub(super) fn add_missing_columns(
    readers: &mut Vec<Box<dyn GenericFileReader>>,
    projection: &Schema,
    physical_rows: u32,
) {
    let field_ids_in_files = readers
        .iter()
        .flat_map(|reader| reader.projection().fields_pre_order().map(|field| field.id))
        .filter(|id| *id >= 0)
        .collect::<HashSet<_>>();
    let mut missing_fields = projection.field_ids();
    missing_fields.retain(|field| !field_ids_in_files.contains(field) && *field >= 0);
    if !missing_fields.is_empty() {
        let missing_projection = projection.project_by_ids(&missing_fields, true);
        readers.push(Box::new(NullReader::new(
            Arc::new(missing_projection),
            physical_rows,
        )));
    }
}

pub(super) async fn get_file_metadata_index(
    file_scheduler: &FileScheduler,
    cache: &LanceCache,
    known_schema: Option<(Arc<Schema>, u64)>,
) -> Result<Arc<FileMetadataIndex>> {
    cache
        .get_or_insert_with_key(FileMetadataIndexCacheKey, || async {
            if let Some((file_schema, num_rows)) = known_schema {
                lance_file::reader::FileReader::read_metadata_index_with_schema(
                    file_scheduler,
                    file_schema,
                    num_rows,
                )
                .await
            } else {
                lance_file::reader::FileReader::read_metadata_index(file_scheduler).await
            }
        })
        .await
}
