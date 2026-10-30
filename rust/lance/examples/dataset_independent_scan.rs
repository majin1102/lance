// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A worker constructs scanners directly from fragment and index primitives.
//! See `dataset_independent_scan.md` for the deliberately limited scope.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{RecordBatchIterator, record_batch};
use clap::Parser;
use datafusion::common::ScalarValue;
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::WriteParams;
use lance::dataset::fragment::read_task::FragmentReadTask;
use lance::index::DatasetIndexExt;
use lance::index::segment::{IndexSegmentReadTask, SegmentRowIdMode};
use lance_core::datatypes::Schema;
use lance_core::{Error, Result};
use lance_file::datatypes::{Fields, FieldsWithMeta};
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{SargableQuery, ScalarIndexParams};
use lance_io::object_store::uri_to_url;
use lance_table::format::{Fragment, IndexMetadata, pb};
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(Parser)]
struct Args {
    /// Internal entry point used to demonstrate transport to a fresh process.
    #[arg(long)]
    worker: Option<PathBuf>,
}

// This same-build example envelope is not a proposed stable task format.
#[derive(Serialize, Deserialize)]
struct Transport {
    table_uri: String,
    snapshot_version: u64,
    schema: Vec<u8>,
    fragments: Vec<Vec<u8>>,
    segment_path: String,
    index_metadata: Vec<u8>,
    row_id_mode: SegmentRowIdMode,
    expected_scan_ids: Vec<i32>,
    expected_index_ids: Vec<i32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    if let Some(transport_path) = Args::parse().worker {
        return run_worker(&transport_path).await;
    }

    let temporary = tempfile::tempdir()?;
    let table_path = temporary.path().join("table");
    let uri = table_path
        .to_str()
        .ok_or_else(|| Error::invalid_input("Temporary table path is not UTF-8"))?;
    let batch = record_batch!(
        ("id", Int32, [0, 1, 2, 3, 4, 5, 6, 7]),
        (
            "x",
            Int32,
            [
                Some(7),
                Some(1),
                Some(7),
                None,
                Some(7),
                Some(5),
                Some(7),
                None
            ]
        )
    )?;
    let reader = RecordBatchIterator::new([Ok(batch.clone())], batch.schema());
    let mut dataset = Dataset::write(
        reader,
        uri,
        Some(WriteParams {
            max_rows_per_file: 4,
            max_rows_per_group: 4,
            enable_stable_row_ids: false,
            ..Default::default()
        }),
    )
    .await?;
    dataset
        .create_index(
            &["x"],
            IndexType::BTree,
            Some("x_btree".to_string()),
            &ScalarIndexParams::default(),
            false,
        )
        .await?;
    // The index still contains this address. The fragment reader must apply
    // the deletion state of the snapshot used to plan the task.
    dataset.delete("id = 2").await?;

    let indices = dataset.load_indices().await?;
    if dataset.manifest().uses_stable_row_ids()
        || !dataset.manifest().base_paths.is_empty()
        || dataset.fragments().iter().any(|f| !f.overlays.is_empty())
        || indices.iter().any(lance_index::is_system_index)
    {
        return Err(Error::not_supported(
            "This demo requires address-based row IDs, no overlays, no external bases, \
             and no fragment-reuse or other system indices",
        ));
    }
    assert_eq!(dataset.fragments().len(), 2);
    let [segment] = indices.as_slice() else {
        return Err(Error::invalid_input(format!(
            "Expected one BTree segment, found {}",
            indices.len()
        )));
    };
    if segment.base_id.is_some() {
        return Err(Error::not_supported(
            "External index bases are not supported",
        ));
    }

    let mut reference_scan = dataset.scan();
    reference_scan.project(&["id"])?;
    let reference_all = reference_scan.try_into_batch().await?;
    reference_scan.filter("x = 7")?.use_scalar_index(false);
    let reference_index = reference_scan.try_into_batch().await?;
    let mut expected_scan_ids = reference_all["id"]
        .as_primitive::<Int32Type>()
        .values()
        .to_vec();
    let mut expected_index_ids = reference_index["id"]
        .as_primitive::<Int32Type>()
        .values()
        .to_vec();
    expected_scan_ids.sort_unstable();
    expected_index_ids.sort_unstable();
    assert_eq!(expected_scan_ids, [0, 1, 3, 4, 5, 6, 7]);
    assert_eq!(expected_index_ids, [0, 4, 6]);

    let fragments = dataset
        .fragments()
        .iter()
        .map(|fragment| pb::DataFragment::from(fragment).encode_to_vec())
        .collect();
    let fields: FieldsWithMeta = dataset.schema().into();
    let transport = Transport {
        table_uri: uri_to_url(dataset.uri())?.to_string(),
        snapshot_version: dataset.manifest().version,
        schema: lance_file::format::pb::Schema {
            fields: fields.fields.0,
            metadata: fields.metadata,
        }
        .encode_to_vec(),
        fragments,
        segment_path: dataset
            .indices_dir()
            .join(segment.uuid.to_string())
            .to_string(),
        index_metadata: pb::IndexMetadata::from(segment).encode_to_vec(),
        row_id_mode: SegmentRowIdMode::PhysicalAddresses,
        expected_scan_ids,
        expected_index_ids,
    };
    let transport_path = temporary.path().join("primitives.json");
    std::fs::write(&transport_path, serde_json::to_vec(&transport)?)?;
    drop(reference_scan);
    drop(indices);
    drop(dataset);

    // Only destroy manifests in this disposable fixture. A real scheduler must
    // pin the snapshot's data, index and deletion files until tasks complete.
    std::fs::remove_dir_all(table_path.join("_versions"))?;
    assert!(Dataset::open(uri).await.is_err());
    let status = Command::new(std::env::current_exe()?)
        .arg("--worker")
        .arg(&transport_path)
        .status()?;
    assert!(status.success(), "Independent worker failed: {status}");
    log::info!("Scanners constructed from primitives matched the Dataset reference");
    Ok(())
}

async fn run_worker(transport_path: &Path) -> Result<()> {
    let transport: Transport = serde_json::from_slice(&std::fs::read(transport_path)?)?;
    let schema = lance_file::format::pb::Schema::decode(transport.schema.as_slice())?;
    let schema = Arc::new(Schema::try_from(FieldsWithMeta {
        fields: Fields(schema.fields),
        metadata: schema.metadata,
    })?);
    let mut tasks = BTreeMap::new();
    let mut scanned_ids = Vec::new();
    for encoded in &transport.fragments {
        let fragment = Arc::new(Fragment::try_from(pb::DataFragment::decode(
            encoded.as_slice(),
        )?)?);
        // The worker chooses its projection and constructs its scanner after
        // the Dataset and manifest have gone. No prebuilt read task is sent.
        let task = FragmentReadTask::from_fragment(
            &transport.table_uri,
            transport.snapshot_version,
            schema.clone(),
            fragment,
            &["id"],
        )?;
        let reader = task.open(None).await?;
        let mut batches = reader.read_all(2).await?.buffered(2);
        while let Some(batch) = batches.try_next().await? {
            scanned_ids.extend_from_slice(batch["id"].as_primitive::<Int32Type>().values());
        }
        tasks.insert(task.fragment_id(), task);
    }
    scanned_ids.sort_unstable();
    assert_eq!(scanned_ids, transport.expected_scan_ids);

    let segment = Arc::new(IndexMetadata::try_from(pb::IndexMetadata::decode(
        transport.index_metadata.as_slice(),
    )?)?);
    let segment_scanner = IndexSegmentReadTask::from_segment(
        &transport.table_uri,
        object_store::path::Path::parse(&transport.segment_path)?,
        segment.clone(),
        transport.row_id_mode,
    )?
    .open(None)
    .await?;
    let query = SargableQuery::Equals(ScalarValue::Int32(Some(7)));
    let matches = segment_scanner
        .search(&query, &NoOpMetricsCollector)
        .await?;
    let addresses = matches.row_addrs().ok_or_else(|| {
        Error::not_supported("The demo requires individually enumerated row addresses")
    })?;
    let mut offsets_by_fragment: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
    for address in addresses {
        offsets_by_fragment
            .entry(u64::from(address.fragment_id()))
            .or_default()
            .push(address.row_offset());
    }
    assert_eq!(offsets_by_fragment.values().map(Vec::len).sum::<usize>(), 4);
    assert_eq!(offsets_by_fragment.len(), 2);

    // The same files retain both fragments' rows, but a metadata provider may
    // withdraw one fragment from the segment's coverage without rewriting them.
    let (&fragment_id, _) = offsets_by_fragment
        .first_key_value()
        .ok_or_else(|| Error::invalid_input("The fixture index returned no fragment addresses"))?;
    let mut narrowed_segment = segment.as_ref().clone();
    narrowed_segment.fragment_bitmap = Some([fragment_id as u32].into_iter().collect());
    let narrowed_scanner = IndexSegmentReadTask::from_segment(
        &transport.table_uri,
        object_store::path::Path::parse(&transport.segment_path)?,
        Arc::new(narrowed_segment),
        transport.row_id_mode,
    )?
    .open(None)
    .await?;
    let narrowed_matches = narrowed_scanner
        .search(&query, &NoOpMetricsCollector)
        .await?;
    let narrowed_addresses = narrowed_matches
        .row_addrs()
        .ok_or_else(|| {
            Error::not_supported("The fixture requires individually enumerated row addresses")
        })?
        .collect::<Vec<_>>();
    assert_eq!(narrowed_addresses.len(), 2);
    assert!(
        narrowed_addresses
            .iter()
            .all(|address| u64::from(address.fragment_id()) == fragment_id)
    );

    let mut selected_ids = Vec::new();
    for (fragment_id, mut offsets) in offsets_by_fragment {
        let task = tasks.get(&fragment_id).ok_or_else(|| {
            Error::invalid_input(format!(
                "Index returned fragment {fragment_id}, absent from the planned snapshot"
            ))
        })?;
        offsets.sort_unstable();
        let reader = task.open(None).await?;
        let mut batches = reader.take(&offsets, 2, None).await?.buffered(2);
        while let Some(batch) = batches.try_next().await? {
            selected_ids.extend_from_slice(batch["id"].as_primitive::<Int32Type>().values());
        }
    }
    selected_ids.sort_unstable();
    assert_eq!(selected_ids, transport.expected_index_ids);
    log::info!(
        "Worker read {} live rows and selected {} rows using only primitive metadata and files",
        scanned_ids.len(),
        selected_ids.len()
    );
    Ok(())
}
