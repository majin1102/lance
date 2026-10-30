// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A planner sends fragment tasks and one BTree segment to a fresh worker process.
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
use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::btree::BTreeIndexPlugin;
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_index::scalar::registry::ScalarIndexPlugin;
use lance_index::scalar::{SargableQuery, ScalarIndexParams, SearchResult};
use lance_io::object_store::ObjectStore;
use lance_table::format::{IndexMetadata, pb};
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
    fragment_tasks: Vec<Vec<u8>>,
    segment_path: String,
    index_metadata: Vec<u8>,
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

    let fragment_tasks = dataset
        .fragments()
        .iter()
        .map(|fragment| {
            FragmentReadTask::try_new(&dataset, fragment.id, &["id"]).map(|task| task.encode())
        })
        .collect::<Result<Vec<_>>>()?;
    let transport = Transport {
        table_uri: dataset.uri().to_string(),
        snapshot_version: dataset.manifest().version,
        fragment_tasks,
        segment_path: dataset
            .indices_dir()
            .join(segment.uuid.to_string())
            .to_string(),
        index_metadata: pb::IndexMetadata::from(segment).encode_to_vec(),
        expected_scan_ids,
        expected_index_ids,
    };
    let transport_path = temporary.path().join("tasks.json");
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
    log::info!("Fragment scans and BTree lookup/take matched the Dataset reference");
    Ok(())
}

async fn run_worker(transport_path: &Path) -> Result<()> {
    let transport: Transport = serde_json::from_slice(&std::fs::read(transport_path)?)?;
    let mut tasks = BTreeMap::new();
    let mut scanned_ids = Vec::new();
    for encoded in &transport.fragment_tasks {
        let task = FragmentReadTask::decode(encoded)?;
        assert_eq!(task.snapshot_version(), transport.snapshot_version);
        let reader = task.open(None).await?;
        let mut batches = reader.read_all(2).await?.buffered(2);
        while let Some(batch) = batches.try_next().await? {
            scanned_ids.extend_from_slice(batch["id"].as_primitive::<Int32Type>().values());
        }
        tasks.insert(task.fragment_id(), task);
    }
    scanned_ids.sort_unstable();
    assert_eq!(scanned_ids, transport.expected_scan_ids);

    let segment = IndexMetadata::try_from(pb::IndexMetadata::decode(
        transport.index_metadata.as_slice(),
    )?)?;
    let details = segment.index_details.as_deref().ok_or_else(|| {
        Error::invalid_input(format!("Index {} has no type details", segment.uuid))
    })?;
    let version = u32::try_from(segment.index_version).map_err(|_| {
        Error::invalid_input(format!(
            "Index {} has negative version {}",
            segment.uuid, segment.index_version
        ))
    })?;
    let (object_store, _) = ObjectStore::from_uri(&transport.table_uri).await?;
    let index_store = LanceIndexStore::new(
        object_store,
        object_store::path::Path::parse(&transport.segment_path)?,
        Arc::new(LanceCache::no_cache()),
    )
    .with_file_sizes(segment.file_size_map());
    let index_cache = LanceCache::no_cache();
    let index = BTreeIndexPlugin
        .load_index(Arc::new(index_store), details, version, None, &index_cache)
        .await?;
    let SearchResult::Exact(matches) = index
        .search(
            &SargableQuery::Equals(ScalarValue::Int32(Some(7))),
            &NoOpMetricsCollector,
        )
        .await?
    else {
        return Err(Error::not_supported(
            "The demo requires exact BTree results",
        ));
    };
    let true_rows = matches.true_rows();
    let addresses = true_rows.row_addrs().ok_or_else(|| {
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
        "Worker read {} live rows and selected {} rows using only transported tasks and files",
        scanned_ids.len(),
        selected_ids.len()
    );
    Ok(())
}
