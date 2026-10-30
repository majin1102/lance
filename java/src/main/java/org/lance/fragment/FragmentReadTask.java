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
package org.lance.fragment;

import org.lance.Dataset;
import org.lance.JniLoader;
import org.lance.LockManager;
import org.lance.Session;
import org.lance.namespace.LanceNamespace;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An experimental, serializable fragment scan that executes without opening a Dataset.
 *
 * <p>The payload contains the fragment, its Lance schema, projection, storage URI and snapshot
 * version. It contains no credentials, native handles or table-wide manifest. Driver and workers
 * must use the same Lance build; this task protocol is not a persistent file format.
 *
 * <p>The planner must retain the referenced data and deletion files until all tasks and retries
 * finish. This reader applies the captured deletion state, but does not evaluate SQL filters or
 * reopen the table to discover newer metadata. Unsupported formats, overlays and external data
 * bases are rejected. Physical row offsets and addresses are distinct from stable row IDs.
 *
 * <pre>{@code
 * // Planning; capture is optional when an engine already has primitive protobuf metadata.
 * FragmentReadTask task = FragmentReadTask.capture(dataset, fragmentId, List.of("id"));
 * byte[] payload = task.toBytes();
 * // Execution; no Dataset is needed or opened.
 * try (ArrowReader reader = FragmentReadTask.fromBytes(payload)
 *     .reader().allocator(allocator).session(session).batchSize(1024).open()) {
 *   while (reader.loadNextBatch()) {
 *     // Consume reader.getVectorSchemaRoot() before loading the next batch.
 *   }
 * }
 * }</pre>
 */
public final class FragmentReadTask implements Serializable {
  private static final long serialVersionUID = 1L;

  static {
    JniLoader.ensureLoaded();
  }

  private final byte[] payload;

  private FragmentReadTask(byte[] payload) {
    this.payload = payload.clone();
  }

  /**
   * Capture one fragment from an already resolved snapshot without retaining the Dataset.
   *
   * <p>This convenience adapter exports complete native metadata, avoiding a potentially lossy
   * round-trip through Java FragmentMetadata. An empty projection produces zero-column batches with
   * correct live row counts.
   */
  public static FragmentReadTask capture(
      Dataset dataset, long fragmentId, List<String> projection) {
    Objects.requireNonNull(dataset, "dataset");
    Objects.requireNonNull(projection, "projection");
    try (LockManager.ReadLock ignored = dataset.acquireReadLock()) {
      return new FragmentReadTask(captureNative(dataset, fragmentId, projection));
    }
  }

  /**
   * Construct a task from complete Lance Schema and DataFragment protobuf messages.
   *
   * <p>The schema must preserve Lance field IDs and describe the same snapshot as the fragment. The
   * storage URI locates the base containing data and deletion files. Construction validates
   * metadata without reading a manifest or opening any files.
   */
  public static FragmentReadTask fromFragment(
      String uri,
      long snapshotVersion,
      byte[] schemaProto,
      byte[] fragmentProto,
      List<String> projection) {
    return new FragmentReadTask(
        fromFragmentNative(
            Objects.requireNonNull(uri, "uri"),
            snapshotVersion,
            Objects.requireNonNull(schemaProto, "schemaProto"),
            Objects.requireNonNull(fragmentProto, "fragmentProto"),
            Objects.requireNonNull(projection, "projection")));
  }

  /** Decode and validate a same-build task without storage I/O. */
  public static FragmentReadTask fromBytes(byte[] payload) {
    byte[] copied = Objects.requireNonNull(payload, "payload").clone();
    validateNative(copied);
    return new FragmentReadTask(copied);
  }

  /** Return a copy of the same-build task payload for transport. */
  public byte[] toBytes() {
    return payload.clone();
  }

  /** Return a task that also emits the physical {@code _rowaddr} column. */
  public FragmentReadTask withRowAddress() {
    return new FragmentReadTask(withRowAddressNative(payload));
  }

  /** Configure worker-local resources for an independent scan or physical-offset take. */
  public ReaderBuilder reader() {
    return new ReaderBuilder(this);
  }

  /** Runtime resources are deliberately separate from the serializable task payload. */
  public static final class ReaderBuilder {
    private final FragmentReadTask task;
    private BufferAllocator allocator;
    private Session session;
    private Map<String, String> storageOptions = Collections.emptyMap();
    private int batchSize = 1024;
    private int blockSize;
    private long[] rowOffsets;
    private LanceNamespace namespaceClient;
    private List<String> tableId;

    private ReaderBuilder(FragmentReadTask task) {
      this.task = task;
    }

    /** Set the caller-owned Arrow allocator; required by {@link #open()}. */
    public ReaderBuilder allocator(BufferAllocator allocator) {
      this.allocator = Objects.requireNonNull(allocator, "allocator");
      return this;
    }

    /**
     * Share executor-local caches and storage registry. Do not close the session concurrently with
     * opening a reader. Once opened, the reader retains its own native session reference.
     */
    public ReaderBuilder session(Session session) {
      this.session = Objects.requireNonNull(session, "session");
      return this;
    }

    /** Set worker-local storage credentials/options; these are never placed in task bytes. */
    public ReaderBuilder storageOptions(Map<String, String> storageOptions) {
      this.storageOptions = new HashMap<>(Objects.requireNonNull(storageOptions, "storageOptions"));
      return this;
    }

    /** Supply a namespace only for refreshing credentials; the task already fixes its location. */
    public ReaderBuilder namespaceClient(LanceNamespace namespaceClient) {
      this.namespaceClient = Objects.requireNonNull(namespaceClient, "namespaceClient");
      return this;
    }

    /**
     * Set the table identifier for credential refresh; requires {@link
     * #namespaceClient(LanceNamespace)}.
     */
    public ReaderBuilder tableId(List<String> tableId) {
      this.tableId = new ArrayList<>(Objects.requireNonNull(tableId, "tableId"));
      return this;
    }

    /** Set the positive maximum number of physical rows requested per batch. */
    public ReaderBuilder batchSize(int batchSize) {
      if (batchSize <= 0) {
        throw new IllegalArgumentException("batchSize must be positive, got " + batchSize);
      }
      this.batchSize = batchSize;
      return this;
    }

    /** Set a positive storage block size in bytes; omitting it uses the storage default. */
    public ReaderBuilder blockSize(int blockSize) {
      if (blockSize <= 0) {
        throw new IllegalArgumentException("blockSize must be positive, got " + blockSize);
      }
      this.blockSize = blockSize;
      return this;
    }

    /**
     * Select strictly increasing, unique physical offsets in this fragment, each fitting uint32.
     * Deleted offsets are removed from the output. An empty array produces no rows; omitting this
     * option scans all live rows. These values are not logical/stable row IDs.
     */
    public ReaderBuilder rowOffsets(long[] rowOffsets) {
      this.rowOffsets = Objects.requireNonNull(rowOffsets, "rowOffsets").clone();
      return this;
    }

    /**
     * Open an Arrow reader. Closing it, including before exhaustion, releases the native stream.
     * The allocator and any supplied Session remain caller-owned.
     */
    public ArrowReader open() throws IOException {
      Objects.requireNonNull(allocator, "allocator is required to open an ArrowReader");
      try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        exportArrowStream(stream.memoryAddress());
        return Data.importArrayStream(allocator, stream);
      }
    }

    /**
     * Export to a fresh, empty caller-owned Arrow C stream, permitting other Arrow versions and
     * classloaders to consume the result. The caller must release the stream and must not share its
     * address between concurrent exports. No Java Arrow object crosses this boundary.
     */
    public void exportArrowStream(long streamAddress) throws IOException {
      exportNative(
          task.payload,
          session,
          storageOptions,
          namespaceClient,
          tableId,
          batchSize,
          blockSize,
          rowOffsets,
          streamAddress);
    }
  }

  private static native byte[] captureNative(
      Dataset dataset, long fragmentId, List<String> projection);

  private static native byte[] fromFragmentNative(
      String uri,
      long snapshotVersion,
      byte[] schemaProto,
      byte[] fragmentProto,
      List<String> projection);

  private static native void validateNative(byte[] payload);

  private static native byte[] withRowAddressNative(byte[] payload);

  private static native void exportNative(
      byte[] payload,
      Session session,
      Map<String, String> storageOptions,
      LanceNamespace namespaceClient,
      List<String> tableId,
      int batchSize,
      int blockSize,
      long[] rowOffsets,
      long streamAddress)
      throws IOException;
}
