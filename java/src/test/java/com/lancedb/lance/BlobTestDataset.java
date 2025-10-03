/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb.lance;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BlobTestDataset: Java-side helper to construct a blob-capable dataset for tests.
 *
 * This class defines a schema that includes a blob column annotated with Lance metadata
 * and uses JNI-backed Dataset/Fragment operations to create the dataset and append rows
 * with large binary payloads. No Java-only storage/writer logic is used.
 */
public final class BlobTestDataset {

  /** Lance schema storage-class metadata key for marking blob columns. */
  private static final String LANCE_STORAGE_CLASS_META_KEY = "lance-schema:storage-class";

  /** Lance storage-class value for blob columns. */
  private static final String LANCE_STORAGE_CLASS_BLOB = "blob";

  private final BufferAllocator allocator;
  private final String datasetPath;

  public BlobTestDataset(BufferAllocator allocator, String datasetPath) {
    Preconditions.checkNotNull(allocator, "allocator cannot be null");
    Preconditions.checkArgument(datasetPath != null && !datasetPath.isEmpty(),
        "datasetPath cannot be null or empty");
    this.allocator = allocator;
    this.datasetPath = datasetPath;
  }

  /**
   * Build the Arrow schema with a filter column and a blob column marked as blob storage.
   *
   * Columns:
   * - filterme: Int64 (not nullable)
   * - blobs: Binary (nullable) with metadata {"lance-schema:storage-class":"blob"}
   *
   * Note: ArrowType.LargeBinary may not be available in our Arrow Java version; Binary is sufficient
   * for tests and aligns with Lance blob storage when annotated via metadata.
   */
  public Schema getSchema() {
    Map<String, String> blobMeta = new HashMap<>();
    blobMeta.put(LANCE_STORAGE_CLASS_META_KEY, LANCE_STORAGE_CLASS_BLOB);
    Field filterField = Field.notNullable("filterme", new ArrowType.Int(64, true));
    Field blobField = new Field(
        "blobs",
        new FieldType(true, ArrowType.Binary.INSTANCE, /* dict */ null, blobMeta),
        /* children */ Collections.emptyList());
    return new Schema(Arrays.asList(filterField, blobField), /* metadata */ null);
  }

  /** Create an empty dataset at the path with the blob-capable schema. */
  public Dataset createEmptyDataset() {
    WriteParams params = new WriteParams.Builder()
        // Enable stable row ids to simplify test assertions across fragments
        .withEnableStableRowIds(true)
        .withMode(WriteParams.WriteMode.CREATE)
        .build();
    Dataset ds = Dataset.create(allocator, datasetPath, getSchema(), params);
    Preconditions.checkArgument(ds.countRows() == 0, "dataset should be empty at creation");
    return ds;
  }

  /**
   * Create a single fragment with given row count and return its metadata.
   * The fragment contains deterministic blob payloads:
   * - Every 16th row starting at 0 has zero-length blob
   * - Every 16th row starting at 1 has a ~1 MiB payload
   * - Others have small variable blobs (128..383 bytes)
   */
  public FragmentMetadata createBlobFragment(int rowCount, int maxRowsPerFile) {
    Preconditions.checkArgument(rowCount >= 0, "rowCount must be non-negative");
    try (VectorSchemaRoot root = VectorSchemaRoot.create(getSchema(), allocator)) {
      root.allocateNew();

      BigIntVector filterVec = (BigIntVector) root.getVector("filterme");
      VarBinaryVector blobsVec = (VarBinaryVector) root.getVector("blobs");
      filterVec.allocateNew(rowCount);
      blobsVec.allocateNew();

      for (int i = 0; i < rowCount; i++) {
        filterVec.setSafe(i, i);
        if (i % 16 == 0) {
          // zero-length blob
          blobsVec.setSafe(i, new byte[0]);
        } else if (i % 16 == 1) {
          // large blob ~1MiB
          byte[] big = new byte[1024 * 1024];
          Arrays.fill(big, (byte) 0xAB);
          blobsVec.setSafe(i, big);
        } else {
          // small variable blob
          int sz = 128 + (i % 256);
          byte[] data = new byte[sz];
          Arrays.fill(data, (byte) (i % 256));
          blobsVec.setSafe(i, data);
        }
      }
      root.setRowCount(rowCount);

      WriteParams params = new WriteParams.Builder()
          .withMaxRowsPerFile(maxRowsPerFile)
          .withMode(WriteParams.WriteMode.APPEND)
          .withEnableStableRowIds(true)
          .build();

      List<FragmentMetadata> metas = Fragment.create(datasetPath, allocator, root, params);
      Preconditions.checkArgument(!metas.isEmpty(), "fragment metadata should not be empty");
      FragmentMetadata meta = metas.get(0);
      Preconditions.checkArgument(meta.getPhysicalRows() == rowCount,
          "fragment physical rows mismatch");
      return meta;
    }
  }

  /**
   * Create a dataset and append rows generated into the specified number of batches.
   * Returns the final dataset after commit.
   */
  public Dataset createAndAppendRows(int totalRows, int batches) {
    Preconditions.checkArgument(totalRows >= 0, "totalRows must be non-negative");
    int effectiveBatches = Math.max(1, batches);
    try (Dataset ds = createEmptyDataset()) {

      List<FragmentMetadata> fragments = new ArrayList<>();
      int remaining = totalRows;
      for (int b = 0; b < effectiveBatches; b++) {
        int batchRows = (b == effectiveBatches - 1) ? remaining : Math.max(1, totalRows / effectiveBatches);
        remaining = Math.max(0, remaining - batchRows);
        fragments.add(createBlobFragment(batchRows, Integer.MAX_VALUE));
      }

      Transaction txn = ds.newTransactionBuilder()
          .operation(com.lancedb.lance.operation.Append.builder().fragments(fragments).build())
          .build();
      Dataset newDs = txn.commit();
      Preconditions.checkArgument(newDs.countRows() == totalRows,
          "dataset row count mismatch after append");
      return newDs;
    }
  }

  /** Convenience method mirroring the previous JNI helper shape. */
  public static Dataset create(String path, int rows, int batches) {
    BlobTestDataset dataset = new BlobTestDataset(new RootAllocator(Long.MAX_VALUE), path);
    return dataset.createAndAppendRows(rows, batches);
  }
}
