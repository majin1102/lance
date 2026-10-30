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
package org.lance.index;

import org.lance.Dataset;
import org.lance.JniLoader;
import org.lance.LockManager;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * An experimental BTree segment task that executes without opening a Dataset or manifest.
 *
 * <p>Task bytes contain complete index metadata, resolved location and explicit physical row-ID
 * semantics. They contain no credentials or native handles. Driver and executors must use the same
 * Lance build. The planner must keep the referenced index files alive until tasks and retries
 * finish.
 *
 * <p>Results are physical {@code _rowaddr} values within the segment's declared coverage. They can
 * include deleted rows or fragments absent from the selected snapshot. The engine must intersect
 * live fragments, handle unindexed fragments and apply deletions with fragment readers. Stable IDs,
 * remapping and ANN are unsupported; this is not a global query planner.
 *
 * <pre>{@code
 * IndexSegmentReadTask task = IndexSegmentReadTask.capture(dataset, indexUuid);
 * byte[] payload = task.toBytes();
 * // The worker chooses a typed literal; no Dataset is required here.
 * try (ArrowReader hits = IndexSegmentReadTask.fromBytes(payload).reader()
 *     .allocator(allocator).session(session).equalsValue(oneRowLiteral).open()) {
 *   while (hits.loadNextBatch()) {
 *     // Route physical _rowaddr values to independently captured fragment tasks.
 *   }
 * }
 * }</pre>
 */
public final class IndexSegmentReadTask implements Serializable {
  private static final long serialVersionUID = 1L;

  static {
    JniLoader.ensureLoaded();
  }

  /** Interpretation supplied by the planner; only physical addresses are currently supported. */
  public enum RowIdMode {
    PHYSICAL_ADDRESSES("physical_addresses"),
    STABLE_IDS("stable_ids"),
    REQUIRES_REMAPPING("requires_remapping");

    private final String rustValue;

    RowIdMode(String rustValue) {
      this.rustValue = rustValue;
    }
  }

  private enum QueryType {
    EQUALS("equals"),
    IS_NULL("is_null");

    private final String rustValue;

    QueryType(String rustValue) {
      this.rustValue = rustValue;
    }
  }

  private final byte[] payload;

  private IndexSegmentReadTask(byte[] payload) {
    this.payload = payload.clone();
  }

  /**
   * Capture full native metadata from an already opened planning snapshot.
   *
   * <p>Rejects stable IDs, fragment-reuse/system indices, overlays and external index bases rather
   * than guessing whether the stored IDs still identify the selected physical fragments.
   */
  public static IndexSegmentReadTask capture(Dataset dataset, UUID indexUuid) {
    Objects.requireNonNull(dataset, "dataset");
    Objects.requireNonNull(indexUuid, "indexUuid");
    try (LockManager.ReadLock ignored = dataset.acquireReadLock()) {
      return new IndexSegmentReadTask(captureNative(dataset, indexUuid.toString()));
    }
  }

  /**
   * Construct directly from a complete Lance IndexMetadata protobuf message.
   *
   * <p>The resolved path is in the object-store namespace, not relative to the URI. Any external
   * base must already be resolved. The caller must establish the row-ID contract for its snapshot.
   */
  public static IndexSegmentReadTask fromSegment(
      String uri, String resolvedPath, byte[] metadataProto, RowIdMode rowIdMode) {
    return new IndexSegmentReadTask(
        fromSegmentNative(
            Objects.requireNonNull(uri, "uri"),
            Objects.requireNonNull(resolvedPath, "resolvedPath"),
            Objects.requireNonNull(metadataProto, "metadataProto"),
            Objects.requireNonNull(rowIdMode, "rowIdMode").rustValue));
  }

  /** Decode and validate a same-build task without storage I/O. */
  public static IndexSegmentReadTask fromBytes(byte[] payload) {
    byte[] copied = Objects.requireNonNull(payload, "payload").clone();
    validateNative(copied);
    return new IndexSegmentReadTask(copied);
  }

  /** Return a defensive copy of this same-build transport payload. */
  public byte[] toBytes() {
    return payload.clone();
  }

  /** Return declared fragment coverage, which is not a live-row or deletion mask. */
  public long[] getFragmentIds() {
    return fragmentIdsNative(payload);
  }

  /** Choose a worker-local query and runtime resources for the segment. */
  public ReaderBuilder reader() {
    return new ReaderBuilder(this);
  }

  /** Runtime resources and typed query values are deliberately separate from task bytes. */
  public static final class ReaderBuilder {
    private final IndexSegmentReadTask task;
    private BufferAllocator allocator;
    private Session session;
    private Map<String, String> storageOptions = Collections.emptyMap();
    private LanceNamespace namespaceClient;
    private List<String> tableId;
    private int blockSize;
    private int batchSize = 1024;
    private QueryType queryType;
    private VectorSchemaRoot literal;

    private ReaderBuilder(IndexSegmentReadTask task) {
      this.task = task;
    }

    /**
     * Set the caller-owned Arrow allocator, required to export a typed literal or open a reader.
     */
    public ReaderBuilder allocator(BufferAllocator allocator) {
      this.allocator = Objects.requireNonNull(allocator, "allocator");
      return this;
    }

    /**
     * Share executor-local caches; do not close this Session concurrently with opening a reader.
     */
    public ReaderBuilder session(Session session) {
      this.session = Objects.requireNonNull(session, "session");
      return this;
    }

    /** Set worker-local storage credentials/options; these are not serialized. */
    public ReaderBuilder storageOptions(Map<String, String> storageOptions) {
      this.storageOptions = new HashMap<>(Objects.requireNonNull(storageOptions, "storageOptions"));
      return this;
    }

    /** Supply a namespace only for refreshing credentials at the task's fixed location. */
    public ReaderBuilder namespaceClient(LanceNamespace namespaceClient) {
      this.namespaceClient = Objects.requireNonNull(namespaceClient, "namespaceClient");
      return this;
    }

    /** Set the nonempty table identifier for namespace credential refresh. */
    public ReaderBuilder tableId(List<String> tableId) {
      this.tableId = new ArrayList<>(Objects.requireNonNull(tableId, "tableId"));
      return this;
    }

    /** Set a positive storage read block size in bytes; omission keeps the store default. */
    public ReaderBuilder blockSize(int blockSize) {
      if (blockSize <= 0) {
        throw new IllegalArgumentException("blockSize must be positive, got " + blockSize);
      }
      this.blockSize = blockSize;
      return this;
    }

    /** Bound Arrow result batches; the underlying scalar search retains a compressed hit bitmap. */
    public ReaderBuilder batchSize(int batchSize) {
      if (batchSize <= 0) {
        throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
      }
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Evaluate SQL equality against one typed Arrow value (one column and one row).
     *
     * <p>A non-null literal's type must exactly match the indexed column, including decimal
     * precision/scale and timestamp timezone; this primitive does not coerce values. The root
     * remains caller-owned and must stay open through {@link #open()} or {@link
     * #exportArrowStream(long)}. A NULL literal matches no rows; use {@link #isNull()} for an IS
     * NULL predicate.
     */
    public ReaderBuilder equalsValue(VectorSchemaRoot literal) {
      Objects.requireNonNull(literal, "literal");
      if (literal.getRowCount() != 1 || literal.getFieldVectors().size() != 1) {
        throw new IllegalArgumentException(
            "Equality literal must contain exactly one column and one row");
      }
      this.queryType = QueryType.EQUALS;
      this.literal = literal;
      return this;
    }

    /** Evaluate IS NULL on the indexed column. */
    public ReaderBuilder isNull() {
      this.queryType = QueryType.IS_NULL;
      this.literal = null;
      return this;
    }

    /** Open a physical-row-address stream; closing it releases all retained native resources. */
    public ArrowReader open() throws IOException {
      Objects.requireNonNull(allocator, "allocator is required to open an ArrowReader");
      try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        exportArrowStream(stream.memoryAddress());
        return Data.importArrayStream(allocator, stream);
      }
    }

    /** Export to a fresh, empty caller-owned Arrow C stream. The caller must release the stream. */
    public void exportArrowStream(long streamAddress) throws IOException {
      Objects.requireNonNull(queryType, "Choose equalsValue or isNull before opening the reader");
      if (queryType == QueryType.EQUALS) {
        Objects.requireNonNull(allocator, "allocator is required to export a typed literal");
        try (ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
          Data.exportVectorSchemaRoot(allocator, literal, null, array, schema);
          exportNative(
              task.payload,
              session,
              storageOptions,
              namespaceClient,
              tableId,
              blockSize,
              batchSize,
              queryType.rustValue,
              array.memoryAddress(),
              schema.memoryAddress(),
              streamAddress);
        }
      } else {
        exportNative(
            task.payload,
            session,
            storageOptions,
            namespaceClient,
            tableId,
            blockSize,
            batchSize,
            queryType.rustValue,
            0,
            0,
            streamAddress);
      }
    }
  }

  private static native byte[] captureNative(Dataset dataset, String indexUuid);

  private static native byte[] fromSegmentNative(
      String uri, String resolvedPath, byte[] metadataProto, String rowIdMode);

  private static native void validateNative(byte[] payload);

  private static native long[] fragmentIdsNative(byte[] payload);

  private static native void exportNative(
      byte[] payload,
      Session session,
      Map<String, String> storageOptions,
      LanceNamespace namespaceClient,
      List<String> tableId,
      int blockSize,
      int batchSize,
      String queryType,
      long literalArray,
      long literalSchema,
      long streamAddress)
      throws IOException;
}
