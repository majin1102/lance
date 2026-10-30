# Dataset-independent fragment and index-segment execution

Run from the repository root:

```sh
cargo run -p lance --example dataset_independent_scan
```

Focused validation:

```sh
cargo test -p lance --lib dataset::fragment::read_task::tests
cargo test -p lance --lib dataset::fragment::tests::test_lazy_column_metadata_scan_reads_less_than_full_projection
```

This is an experimental, same-build demonstration of an execution boundary for
distributed engines. It does not implement a Spark connector or propose a stable
task serialization format.

The planner creates an eight-row table with two fragments, builds a BTree index
on `x`, then deletes one matching row. It obtains reference results from an
ordinary Dataset scan and serializes:

- One `FragmentReadTask` per fragment, projected to `id`, with the snapshot's
  schema, fragment metadata and deletion information.
- The BTree segment's existing protobuf `IndexMetadata`, its resolved storage
  path and the table URI needed to construct an object store.
- The snapshot version and small expected results used only to verify the demo.

After dropping the Dataset, the planner removes **only this temporary fixture's**
manifest directory and launches the same executable in a fresh process. The
worker has no Dataset, manifest, inherited cache or open file handle. It performs
two operations:

1. Decode the fragment tasks, open the existing `FragmentReader`, and stream the
   seven live rows. This exercises projection, physical fragment files and the
   snapshot's deletion file without reopening the table.
2. Load the BTree segment directly through the existing `LanceIndexStore` and
   `BTreeIndexPlugin`, search for `x = 7`, route returned `RowAddress` values to
   fragment tasks, and take the corresponding offsets. The segment returns four
   stored matches; fragment materialization removes the deleted row, producing
   IDs `[0, 4, 6]`. Null values do not match the equality predicate.

Both results must equal the Dataset reference. A successful run exits with status
zero and logs the counts. Failure of any assertion or I/O operation fails the
example; no benchmark claim is made.

For convenience, this fixture sends both fragments to one worker. It proves
that each task can execute without a Dataset, not that executor memory has been
benchmarked. A distributed scheduler would assign subsets of tasks to workers.

```text
Planner: Dataset at snapshot v
    |
    +-- serialized FragmentReadTask for each selected fragment
    +-- serialized BTree segment metadata + resolved path
    |
    | process boundary; fixture manifests removed
    v
Worker
    +-- FragmentReadTask -> FragmentReader -> projected scan
    |
    +-- BTreeIndexPlugin -> RowAddress selections
                              |
                              v
                       FragmentReader.take -> live rows
```

The new core surface is the fragment task and its independent opening path.
BTree segment loading and searching already accept explicit storage and metadata;
the example reuses those interfaces. The Dataset remains useful on the planning
side to select a consistent snapshot and resolve its dependencies.

The fixture deliberately uses physical-address row IDs, one fully covering BTree
segment, current file writers, and one local storage base. Planning rejects stable
row IDs, overlays, external bases, fragment reuse and other system indices. The
worker does not infer how to translate an old segment's IDs into a new snapshot.
The segment's build version may precede the scan snapshot because deleting a row
does not rewrite the index; deletion state travels with each fragment task.

Production integration still needs snapshot/file retention for task lifetime,
credentials and storage configuration outside the task payload, index coverage
and fallback planning, and shared worker caches. Stable row IDs, fragment-reuse
translation, overlays and ANN search require explicit semantics. ANN also needs
prefiltering, partition search, global candidate merging and possibly refinement;
this BTree example does not establish those contracts. A Spark adapter would
serialize these units as input partitions and supply worker runtime resources,
rather than opening the full Dataset on every executor.
