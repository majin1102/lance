# Compaction commit benchmark

This standalone experiment accompanies [PR #10148](https://github.com/lance-format/lance/pull/10148).
It adds no production changes. The [Rust benchmark](../../rust/lance/benches/compaction_remap.rs)
measures the real `commit_compaction` call after data rewriting has completed.

Base: `3104052849d1a45c321bf13b3220254696d26eae`.
Optimized: `d2082ceb698667535d1e494c70432158cbfa4775`.
Both executables use the identical benchmark source and prepared input files.

## Results

Compact mapping enabled; medians of five runs per version; empty Lance index cache.
The OS page cache was not flushed.

| Index | Input rows | Commit time, before → after | Peak RSS, before → after |
|---|---:|---:|---:|
| FMIndex | 1M | 257.90 → 2.66 ms | 160.49 → 38.93 MiB |
| FMIndex | 10M | 2,531.80 → 3.66 ms | 1,083.18 → 39.83 MiB |
| BloomFilter | 1M | 19.66 → 2.28 ms | 61.38 → 38.57 MiB |
| BloomFilter | 10M | 182.36 → 2.40 ms | 128.21 → 38.76 MiB |

FMIndex logical reads during the 10M-row cold commit fell from 470.41 MiB / 8,006 reads
to 24.43 KiB / 5 reads. The baseline opens the index before discovering that remapping
is unsupported. Opening all 1,000 partitions decodes per-row metadata and reads SA
samples; this is substantial work even with the OS page cache warm.

With the indices preloaded before timing, 10M-row commits improved from
67.54 → 4.25 ms for FMIndex and 63.80 → 3.01 ms for BloomFilter. Their peak RSS changed
much less because the indices were already resident. Direct-mode results and the
complete cold/warm matrix are included in the summaries below.

- Compact: [raw samples](results/compact/samples.jsonl), [summary](results/compact/summary.csv), [environment](results/compact/metadata.json).
- Direct: [raw samples](results/direct/samples.jsonl), [summary](results/direct/summary.csv), [environment](results/direct/metadata.json).

## Measurement

- Linux, Xeon Platinum 8336C, 16 available CPUs, 31.1 GiB RAM; Rust 1.98.1 and the repository's `release-with-debug` profile.
- 16 input fragments; integer ID and deterministic 64-byte text; delete every tenth row. FMIndex indexes text, BloomFilter indexes ID.
- Physical row addresses, deferred remapping disabled, one index per dataset. Index-cache budget is 2 GiB in both versions.
- Index construction and data rewriting run in a separate preparation process. Dataset opening, task JSON parsing and optional cache preloading are outside the timer in both versions.
- Each sample uses a fresh process and dataset copy. Copied files and directories are fsynced before measurement; base/optimized execution order alternates between repetitions.
- Peak RSS is sampled every 1 ms during commit, including boundary samples. Raw samples also contain RSS at entry and kernel VmHWM through commit completion; VmHWM includes startup and any preloading.
- Row counts, existing/deleted ID queries, index UUID and effective fragment coverage are checked after measurement. All 100 recorded samples passed these checks.

These results measure the commit stage on synthetic local data. They exclude data
rewriting and index construction; the speedups do not describe end-to-end compaction.

## Reproduction

Use Linux and the repository's Rust environment. Build this branch for the optimized
executable. For the baseline, check out the base SHA in a separate worktree, copy the
same `rust/lance/benches/compaction_remap.rs` into it, and add only this Cargo entry:

```toml
[[bench]]
name = "compaction_remap"
harness = false
```

In each checkout, build with:

```sh
cargo build -p lance --bench compaction_remap --profile release-with-debug --message-format=json > build.jsonl
```

Copy the `executable` path from the `compaction_remap` compiler-artifact record to
separate `base` and `pr` binaries. Copy the first binary **before** building the other
version if they share a Cargo target directory: the executable paths can be identical.
Do not run a compiler alongside timed samples.

Pin the defaults used for the recorded fixtures:

```sh
export LANCE_BLOOMFILTER_DEFAULT_NUMBER_OF_ITEMS=8192
export LANCE_BLOOMFILTER_DEFAULT_PROBABILITY=0.00057
export LANCE_FMINDEX_PARTITION_ROWS=10000
export LANCE_FMINDEX_PARTITION_BYTES=16777216
```

The following runs one FMIndex case. Set the two binary paths first. Change `fm` to
`bloomfilter`, the row count to `1000000`, or the commit arguments to `direct` / `warm`
to reproduce the other cases. Every commit must consume a fresh fixture copy.

```bash
set -euo pipefail
BASE_BIN=/absolute/path/to/base
PR_BIN=/absolute/path/to/pr
BENCH_DIR=$(mktemp -d)
"$PR_BIN" prepare "$BENCH_DIR/fixture" fm 10000000
for repeat in 1 2 3 4 5; do
    arms="base pr"
    if (( repeat % 2 == 0 )); then arms="pr base"; fi
    for arm in $arms; do
        binary=$BASE_BIN
        if [[ $arm == pr ]]; then binary=$PR_BIN; fi
        sample="$BENCH_DIR/$arm-$repeat"
        cp -a "$BENCH_DIR/fixture" "$sample"
        find "$sample" -depth -exec sync {} +
        "$binary" commit "$sample" compact cold | tee "$BENCH_DIR/$arm-$repeat.json"
        rm -rf -- "$sample"
    done
done
```

Preparation writes `dataset/` and `rewrite.json`. The commit command emits a JSON
measurement after committing and checking correctness. Compare the medians of each
version's five `commit_ms` and `sampled_peak_rss_mib` values. The archived JSONL files
add `arm` and `repeat` labels to these per-process measurements.
