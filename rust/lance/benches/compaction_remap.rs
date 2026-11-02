// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Measure real compaction commits separately from data rewriting and index building.
//!
//! ```text
//! cargo bench -p lance --profile release-with-debug --bench compaction_remap -- prepare /data/fixture fm 10000000
//! cargo bench -p lance --profile release-with-debug --bench compaction_remap -- commit /data/sample compact cold
//! ```
//!
//! Copy the prepared fixture to a fresh sample directory before EACH commit,
//! including when switching between base and PR executables. Commits change the
//! manifest, so replaying the same fixture in place would not be comparable.
//! Both executables must use the same benchmark source and prepared inputs.
//!
//! The fixture has 16 fragments, an integer ID and a deterministic 64-byte text
//! column. Build `fm` on text or `bloomfilter` on ID, delete every tenth row, then
//! execute the real compaction tasks without committing. Stable row IDs and
//! deferred remapping are disabled. Only `commit_compaction` is timed.
//!
//! Each commit runs in a fresh process. `cold` leaves the Lance index cache empty;
//! `warm` opens the index beforehand and verifies that its instance is cached.
//! Both use a 2 GiB index-cache budget. Neither flushes the OS page cache.
//! Linux RSS is sampled every 1 ms during the commit, including boundary samples.
//! Also report process VmHWM for comparison; it includes startup and any prewarm
//! peak. Correctness checks run AFTER both memory and timing measurements.
//! Output is one JSON object per sample, with logical store reads, not disk reads.

#![allow(clippy::print_stdout)]

use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::dataset::WriteParams;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::index::DatasetIndexRemapperOptions;
use lance::dataset::optimize::{
    CompactionOptions, IndexRemapMode, RewriteResult, commit_compaction, plan_compaction,
};
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const FRAGMENTS: usize = 16;
const BATCH_ROWS: usize = 8192;
const INDEX_CACHE_BYTES: usize = 2 * 1024 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
struct Fixture {
    index: String,
    column: String,
    input_rows: usize,
    expected_rows: usize,
    original_index: Uuid,
    options: CompactionOptions,
    tasks: Vec<RewriteResult>,
}

#[derive(Serialize)]
struct Measurement {
    index: String,
    mode: String,
    cache: String,
    input_rows: usize,
    output_rows: usize,
    tasks: usize,
    commit_ms: f64,
    rss_before_mib: f64,
    sampled_peak_rss_mib: f64,
    process_peak_rss_mib: f64,
    read_bytes: u64,
    read_iops: u64,
}

async fn prepare(directory: &Path, index: &str, rows: usize) -> BenchResult<()> {
    if rows < FRAGMENTS * 10 || !rows.is_multiple_of(FRAGMENTS) || rows > i64::MAX as usize {
        return Err(
            format!("rows must be >= 160, divisible by 16, and fit in i64; got {rows}").into(),
        );
    }
    let (kind, column) = match index {
        "fm" => (BuiltinIndexType::Fm, "text"),
        "bloomfilter" => (BuiltinIndexType::BloomFilter, "id"),
        _ => return Err(format!("expected index fm or bloomfilter, got {index}").into()),
    };
    let uri = directory.join("dataset");
    if uri.exists() {
        return Err(format!("fixture already exists: {}", uri.display()).into());
    }
    fs::create_dir_all(directory)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch_schema = schema.clone();
    let batches = (0..rows).step_by(BATCH_ROWS).map(move |start| {
        let end = (start + BATCH_ROWS).min(rows);
        let ids = Int64Array::from_iter_values((start..end).map(|id| id as i64));
        let texts = StringArray::from_iter_values((start..end).map(|id| {
            format!(
                "{id:016x}{:016x}{:016x}{:016x}",
                id.rotate_left(13),
                id.rotate_left(29),
                id.rotate_left(47)
            )
        }));
        RecordBatch::try_new(batch_schema.clone(), vec![Arc::new(ids), Arc::new(texts)])
    });
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(batches, schema),
        uri.to_str().ok_or("fixture path must be UTF-8")?,
        Some(WriteParams {
            max_rows_per_file: rows / FRAGMENTS,
            max_rows_per_group: BATCH_ROWS,
            enable_stable_row_ids: false,
            skip_auto_cleanup: true,
            ..Default::default()
        }),
    )
    .await?;
    assert_eq!(dataset.fragments().len(), FRAGMENTS);
    dataset
        .create_index(
            &[column],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::for_builtin(kind),
            false,
        )
        .await?;
    let original_index = dataset.load_indices().await?[0].uuid;
    dataset.delete("id % 10 = 0").await?;
    let expected_rows = rows - rows.div_ceil(10);
    assert_eq!(dataset.count_rows(None).await?, expected_rows);
    let options = CompactionOptions {
        target_rows_per_fragment: rows / (FRAGMENTS / 2),
        index_remap_mode: IndexRemapMode::Compact,
        ..Default::default()
    };
    let plan = plan_compaction(&dataset, &options).await?;
    assert!(plan.num_tasks() > 0);
    let mut tasks = Vec::with_capacity(plan.num_tasks());
    for task in plan.compaction_tasks() {
        let result = task.execute(&dataset).await?;
        assert!(
            result.row_addrs.is_some(),
            "workers must capture real addresses"
        );
        tasks.push(result);
    }
    serde_json::to_writer(
        BufWriter::new(File::create(directory.join("rewrite.json"))?),
        &Fixture {
            index: index.to_owned(),
            column: column.to_owned(),
            input_rows: rows,
            expected_rows,
            original_index,
            options,
            tasks,
        },
    )?;
    Ok(())
}

fn memory_kib(field: &str) -> BenchResult<u64> {
    let status = fs::read_to_string("/proc/self/status")
        .map_err(|error| format!("Linux RSS measurement requires /proc/self/status: {error}"))?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| format!("missing memory field {field} in /proc/self/status"))?;
    Ok(value.parse()?)
}

async fn measure(directory: &Path, mode: &str, cache: &str) -> BenchResult<()> {
    let index_remap_mode = IndexRemapMode::try_from(mode)?;
    let is_warm = match cache {
        "cold" => false,
        "warm" => true,
        _ => return Err(format!("expected cache cold or warm, got {cache}").into()),
    };
    let fixture: Fixture =
        serde_json::from_reader(BufReader::new(File::open(directory.join("rewrite.json"))?))?;
    let mut dataset = DatasetBuilder::from_uri(
        directory
            .join("dataset")
            .to_str()
            .ok_or("fixture path must be UTF-8")?,
    )
    .with_index_cache_size_bytes(INDEX_CACHE_BYTES)
    .load()
    .await?;
    if is_warm {
        let first = dataset
            .open_scalar_index(
                &fixture.column,
                &fixture.original_index,
                &NoOpMetricsCollector,
            )
            .await?;
        let cached = dataset
            .open_scalar_index(
                &fixture.column,
                &fixture.original_index,
                &NoOpMetricsCollector,
            )
            .await?;
        assert!(
            Arc::ptr_eq(&first, &cached),
            "warm index must fit in the cache"
        );
    }
    let store = dataset.object_store(None).await?;
    let before_io = store.io_stats_snapshot();
    let options = CompactionOptions {
        index_remap_mode,
        ..fixture.options
    };
    let expected_fragments = fixture
        .tasks
        .iter()
        .map(|task| task.new_fragments.len())
        .sum::<usize>();
    let task_count = fixture.tasks.len();
    let rss_before = memory_kib("VmRSS:")?;
    let peak = Arc::new(AtomicU64::new(rss_before));
    let stop = Arc::new(AtomicBool::new(false));
    let (sample_peak, sample_stop) = (peak.clone(), stop.clone());
    let sampler = std::thread::spawn(move || -> BenchResult<()> {
        while !sample_stop.load(Ordering::Relaxed) {
            sample_peak.fetch_max(memory_kib("VmRSS:")?, Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    });
    let started = Instant::now();
    let result = commit_compaction(
        &mut dataset,
        fixture.tasks,
        Arc::new(DatasetIndexRemapperOptions::default()),
        &options,
    )
    .await;
    let elapsed = started.elapsed();
    let final_rss = memory_kib("VmRSS:");
    stop.store(true, Ordering::Relaxed);
    sampler.join().map_err(|_| "RSS sampler panicked")??;
    peak.fetch_max(final_rss?, Ordering::Relaxed);
    result?;
    let process_peak = memory_kib("VmHWM:")?;
    let after_io = store.io_stats_snapshot();

    // These may scan data, so keep them outside the measured memory window too.
    assert_eq!(dataset.fragments().len(), expected_fragments);
    assert_eq!(dataset.count_rows(None).await?, fixture.expected_rows);
    assert_eq!(dataset.count_rows(Some("id = 101".to_owned())).await?, 1);
    assert_eq!(dataset.count_rows(Some("id = 100".to_owned())).await?, 0);
    let indices = dataset.load_indices().await?;
    assert_eq!(indices.len(), 1);
    assert_eq!(indices[0].uuid, fixture.original_index);
    let coverage = indices[0]
        .fragment_bitmap
        .as_ref()
        .ok_or("missing index coverage")?;
    assert!(
        dataset
            .fragments()
            .iter()
            .all(|fragment| !coverage.contains(fragment.id as u32))
    );

    println!(
        "{}",
        serde_json::to_string(&Measurement {
            index: fixture.index,
            mode: mode.to_owned(),
            cache: cache.to_owned(),
            input_rows: fixture.input_rows,
            output_rows: fixture.expected_rows,
            tasks: task_count,
            commit_ms: elapsed.as_secs_f64() * 1000.0,
            rss_before_mib: rss_before as f64 / 1024.0,
            sampled_peak_rss_mib: peak.load(Ordering::Relaxed) as f64 / 1024.0,
            process_peak_rss_mib: process_peak as f64 / 1024.0,
            read_bytes: after_io.read_bytes - before_io.read_bytes,
            read_iops: after_io.read_iops - before_io.read_iops,
        })?
    );
    Ok(())
}

fn main() -> BenchResult<()> {
    let args = std::env::args()
        .filter(|arg| arg != "--bench")
        .collect::<Vec<_>>();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    match args.as_slice() {
        [_, command, directory, index, rows] if command == "prepare" => {
            runtime.block_on(prepare(Path::new(directory), index, rows.parse()?))
        }
        [_, command, directory, mode, cache] if command == "commit" => {
            runtime.block_on(measure(Path::new(directory), mode, cache))
        }
        _ => Err("usage: compaction_remap prepare DIR fm|bloomfilter ROWS; compaction_remap commit DIR compact|direct cold|warm".into()),
    }
}
