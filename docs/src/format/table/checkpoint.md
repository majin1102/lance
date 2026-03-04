# Checkpoint Format Specification

## Overview

Checkpoints provide an optimized way to store version metadata summaries, allowing efficient version listing without requiring all manifest files to be read individually. This specification defines the Lance file format used for storing checkpoints.

## Benefits of Lance Format

Using the Lance file format for checkpoints provides several advantages over the previous protobuf format:

- **Columnar Storage**: Efficient storage and access patterns for version summary data
- **Compression**: Built-in compression for reduced storage size
- **Schema Evolution**: Future-proof format that can evolve without breaking compatibility
- **Consistency**: Uses the same format as the rest of the Lance ecosystem

## Storage Layout

### Directory Structure

Checkpoints are stored in the `_checkpoint/` directory at the dataset root:

```
{dataset_root}/
    _checkpoint/
        {inverted_version}.lance    -- Checkpoint file in Lance format
        {inverted_version}.binpb    -- Legacy checkpoint file (protobuf format)
```

### File Naming Convention

Checkpoint files use the same inverted version naming convention as manifest files for efficient listing:

- **Pattern**: `{inverted_version}.lance`
- **Inversion formula**: `inverted_version = u64::MAX - version`
- **Purpose**: Ensures lexicographical sorting matches version order (newest first)

Example:
- Version 1 → `18446744073709551614.lance`
- Version 100 → `18446744073709551515.lance`

## Arrow Schema Specification

### VersionSummary Table

Checkpoint files store a table with the following schema:

| Field | Arrow Type | Nullable | Description |
|-------|------------|----------|-------------|
| `version` | `UInt64` | No | The manifest version number |
| `timestamp_millis` | `Int64` | No | Manifest creation timestamp in milliseconds since epoch |
| `total_fragments` | `UInt64` | No | Total number of fragments in the version |
| `total_data_files` | `UInt64` | No | Total number of data files |
| `total_files_size` | `UInt64` | No | Total size of all files in bytes |
| `total_deletion_files` | `UInt64` | No | Total number of deletion files |
| `total_data_file_rows` | `UInt64` | No | Total number of rows in data files |
| `total_deletion_file_rows` | `UInt64` | No | Total number of rows in deletion files |
| `total_rows` | `UInt64` | No | Total number of rows in the dataset |
| `is_tagged` | `Boolean` | No | Whether this version is pinned by a tag |
| `is_cleaned_up` | `Boolean` | No | Whether the data files have been cleaned up |
| `transaction_uuid` | `Utf8` | Yes | Transaction UUID for transaction file lookup |
| `read_version` | `UInt64` | Yes | The base version read by the transaction |
| `operation_type` | `Utf8` | Yes | Operation type (e.g., \"Append\", \"Delete\", \"Rewrite\") |
| `transaction_properties` | `Map<Utf8, Utf8>` | Yes | Additional transaction properties as key-value pairs |

### Schema Definition Code

```rust
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use std::sync::Arc;

fn version_summary_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("version", DataType::UInt64, false),
        Field::new("timestamp_millis", DataType::Int64, false),
        Field::new("total_fragments", DataType::UInt64, false),
        Field::new("total_data_files", DataType::UInt64, false),
        Field::new("total_files_size", DataType::UInt64, false),
        Field::new("total_deletion_files", DataType::UInt64, false),
        Field::new("total_data_file_rows", DataType::UInt64, false),
        Field::new("total_deletion_file_rows", DataType::UInt64, false),
        Field::new("total_rows", DataType::UInt64, false),
        Field::new("is_tagged", DataType::Boolean, false),
        Field::new("is_cleaned_up", DataType::Boolean, false),
        Field::new("transaction_uuid", DataType::Utf8, true),
        Field::new("read_version", DataType::UInt64, true),
        Field::new("operation_type", DataType::Utf8, true),
        Field::new(
            "transaction_properties",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Utf8, true),
                    ].into()),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]))
}
```

## File Metadata

Checkpoint files store additional metadata in the Lance file's schema metadata:

| Key | Type | Description |
|-----|------|-------------|
| `dataset_created_millis` | `Int64` | Dataset creation time (timestamp of the earliest version) in milliseconds since epoch |
| `created_at_millis` | `Int64` | Checkpoint file creation timestamp in milliseconds since epoch |
| `latest_version_number` | `UInt64` | The latest version number included in this checkpoint |

These values are stored as strings in the schema metadata for simplicity.

## Backward Compatibility

### Reading Legacy Format

Readers should:
1. First attempt to read the `.lance` format for a given version
2. If that fails, fall back to reading the legacy `.binpb` (protobuf) format
3. Log a warning when falling back to the legacy format

### Writing New Format

Writers should always write in the new `.lance` format. Legacy `.binpb` files will eventually be cleaned up by the cleanup policy.

### Migration

No explicit migration is required. As new checkpoints are written in the `.lance` format, old `.binpb` files will naturally be replaced and eventually cleaned up according to the checkpoint retention policy.

## Example

### Sample RecordBatch

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("version", pa.uint64()),
    pa.field("timestamp_millis", pa.int64()),
    pa.field("total_fragments", pa.uint64()),
    pa.field("total_data_files", pa.uint64()),
    pa.field("total_files_size", pa.uint64()),
    pa.field("total_deletion_files", pa.uint64()),
    pa.field("total_data_file_rows", pa.uint64()),
    pa.field("total_deletion_file_rows", pa.uint64()),
    pa.field("total_rows", pa.uint64()),
    pa.field("is_tagged", pa.bool_()),
    pa.field("is_cleaned_up", pa.bool_()),
    pa.field("transaction_uuid", pa.utf8(), nullable=True),
    pa.field("read_version", pa.uint64(), nullable=True),
    pa.field("operation_type", pa.utf8(), nullable=True),
    pa.field("transaction_properties", pa.map_(pa.utf8(), pa.utf8()), nullable=True),
])

data = [
    pa.array([1, 2, 3], type=pa.uint64()),
    pa.array([1000000, 2000000, 3000000], type=pa.int64()),
    pa.array([10, 20, 30], type=pa.uint64()),
    pa.array([5, 10, 15], type=pa.uint64()),
    pa.array([1024, 2048, 4096], type=pa.uint64()),
    pa.array([0, 1, 0], type=pa.uint64()),
    pa.array([10000, 20000, 30000], type=pa.uint64()),
    pa.array([0, 500, 0], type=pa.uint64()),
    pa.array([10000, 19500, 30000], type=pa.uint64()),
    pa.array([False, True, False], type=pa.bool_()),
    pa.array([False, False, True], type=pa.bool_()),
    pa.array([None, "550e8400-e29b-41d4-a716-446655440000", None], type=pa.utf8()),
    pa.array([None, 1, 2], type=pa.uint64()),
    pa.array(["Append", "Delete", "Rewrite"], type=pa.utf8()),
    pa.array([
        None,
        [("reason", "cleanup")],
        [("source", "import"), ("format", "parquet")]
    ], type=pa.map_(pa.utf8(), pa.utf8())),
]

batch = pa.RecordBatch.from_arrays(data, schema=schema)
```

### Sample File Metadata

```python
metadata = {
    "dataset_created_millis": "1000000",
    "created_at_millis": "4000000",
    "latest_version_number": "3",
}
```

## Implementation Notes

### Sorting

Version summaries within a checkpoint file must be sorted by version number in ascending order.

### Retention Policy

Checkpoint files follow the retention policy configured by:
- `max_entries`: Maximum number of version entries per checkpoint file
- `max_checkpoint_files`: Maximum number of checkpoint files to retain

When these limits are exceeded:
1. Older entries are truncated from the checkpoint file
2. Older checkpoint files are deleted

### File Extension Precedence

When both `.lance` and `.binpb` files exist for the same version:
- The `.lance` file should be preferred
- The `.binpb` file can be safely deleted once the `.lance` file is verified
