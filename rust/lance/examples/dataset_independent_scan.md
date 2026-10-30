# Scanners constructed from fragment and index-segment primitives

Run from the repository root:

```sh
cargo run -p lance --example dataset_independent_scan
```

Focused validation:

```sh
cargo test -p lance --lib dataset::fragment::read_task::tests
cargo test -p lance --lib index::segment::tests
cargo test -p lance --lib dataset::fragment::tests::test_lazy_column_metadata_scan_reads_less_than_full_projection
```

This is an experimental, same-build demonstration of an execution boundary for
distributed engines. The worker constructs its scanners from primitive metadata;
it does not receive scanners or read tasks created by a Dataset. This does not
implement a Spark connector or propose a stable task serialization format.

## Direct construction

A fragment reader needs the schema, one `lance_table::format::Fragment`, a storage
URI, and a projection. The fragment metadata carries its data files, physical row
count and deletion file. The snapshot version identifies the planner's view; it
does not cause a manifest lookup or pin files.

```rust
let reader = FragmentReadTask::from_fragment(
    uri,
    snapshot_version,
    schema,
    fragment,
    &["id"],
)?
.open(None)
.await?;

let batches = reader.read_all(1024).await?;
// Physical offsets can instead be materialized with reader.take(...).
```

`schema` and `fragment` are `Arc<Schema>` and `Arc<Fragment>`. An engine may obtain
these from its own metadata abstraction; constructing the reader does not require
an open Dataset or Manifest. The existing Dataset-based constructor remains an
optional convenience entry point.

An index segment similarly needs its `IndexMetadata`, resolved storage location,
and explicit row-ID semantics:

```rust
let scanner = IndexSegmentReadTask::from_segment(
    storage_uri,
    segment_path,
    index_metadata,
    SegmentRowIdMode::PhysicalAddresses,
)?
.open(None)
.await?;

let row_addresses = scanner.search(&query, &metrics).await?;
```

`segment_path` is an `object_store::path::Path`; `index_metadata` is an
`Arc<IndexMetadata>`. The demo's segment scanner supports BTree queries
whose row IDs are physical addresses. It reuses the existing BTree implementation
and returns typed row-address selections for composition with fragment readers.
It does not infer missing snapshot context or silently treat stable IDs as physical
addresses.

## What the executable verifies

The parent creates an eight-row fixture with two fragments, builds a BTree index
on nullable column `x`, then deletes one matching row. It obtains reference results
from an ordinary Dataset scan and serializes raw primitives:

- The existing schema protobuf and one `DataFragment` protobuf per fragment.
- The BTree segment's existing `IndexMetadata` protobuf, its resolved storage path
  and storage URI, and an explicit `SegmentRowIdMode`.
- The snapshot version and small expected results used only to verify the demo.

No `FragmentReadTask` is constructed or serialized by the parent. After dropping
the Dataset, the parent removes **only this temporary fixture's** manifest directory
and launches the same executable in a fresh process. The worker has no Dataset,
Manifest, inherited cache or inherited data-file handles. It then:

1. Decodes the primitive schema and fragments, chooses a projection, and constructs
   `FragmentReadTask` values directly. Their readers stream the seven live rows,
   applying the snapshot's deletion state without reopening the table.
2. Constructs an index-segment primitive and opens its scanner, then queries
   `x = 7`. It routes the four stored matches by `RowAddress` to fragment readers.
   Fragment materialization removes the deleted row, producing IDs `[0, 4, 6]`.
   Null keys do not match the equality predicate.

The worker also opens the same segment files with metadata declaring coverage of
only one fragment. That scanner must return only that fragment's two stored hits,
even though the index files still contain both fragments.

Both results must equal the Dataset reference. A successful run exits with status
zero and logs the counts. Failure of any assertion or I/O operation fails the
example; no benchmark claim is made.

```text
Fixture setup / metadata provider
    |
    +-- Schema + DataFragment metadata + storage URI
    +-- IndexMetadata + resolved segment path + row-ID semantics
    |
    | process boundary; fixture Dataset dropped and manifests removed
    v
Worker constructs scanners directly from primitives
    +-- FragmentReadTask::from_fragment -> FragmentReader.read_all
    |
    +-- IndexSegmentReadTask::from_segment -> IndexSegmentScanner.search
                                                    |
                                               RowAddress selections
                                                    |
                                                    v
                                           FragmentReader.take -> live rows
```

For convenience, this fixture sends both fragments to one worker. It proves
independent construction and execution, not bounded executor memory or improved
throughput. A distributed scheduler would assign subsets of these primitives to
workers and compose the resulting streams.

## Boundaries

The fixture deliberately uses physical-address row IDs, one fully covering BTree
segment, current file writers, and one local storage base. Fixture planning rejects
stable row IDs, overlays, external bases, fragment reuse and other system indices.
The segment's build version may precede the scan snapshot because deleting a row
does not rewrite the index; deletion state travels with each fragment primitive.

The fragment reader supports projection, scans and physical-offset takes. The
segment scanner evaluates its supported scalar query within one segment. These
are execution primitives, not a global SQL planner: index coverage, unindexed
fallback, cross-segment combination and residual filtering remain the engine's
responsibility.

Production integration still needs snapshot/file retention for scanner lifetime,
credentials and storage configuration outside the metadata payload, and shared
worker caches. Stable row IDs, fragment-reuse translation, overlays and ANN search
require explicit semantics. ANN also needs prefiltering, partition search, global
candidate merging and possibly refinement; this BTree example does not establish
those contracts. A Spark adapter can carry these primitives in input partitions
and construct readers directly on executors.
