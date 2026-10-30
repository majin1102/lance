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
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.Session;
import org.lance.TestUtils;
import org.lance.WriteParams;
import org.lance.fragment.FragmentReadTask;
import org.lance.index.scalar.BTreeIndexParams;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexSegmentReadTaskTest {
  @Test
  void segmentQueriesComposeWithFragmentTakesWithoutManifests(@TempDir Path tempDir)
      throws Exception {
    Path table = tempDir.resolve("table");
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Session session =
            Session.builder().indexCacheSizeBytes(65536).metadataCacheSizeBytes(65536).build()) {
      TestUtils.SimpleTestDataset fixture =
          new TestUtils.SimpleTestDataset(allocator, table.toString());
      fixture
          .createDatasetWithWriteParams(
              new WriteParams.Builder().withEnableStableRowIds(false).build())
          .close();
      List<FragmentMetadata> fragments;
      try (VectorSchemaRoot root = VectorSchemaRoot.create(fixture.getSchema(), allocator)) {
        root.allocateNew();
        IntVector id = (IntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        String[] values = {"match", "match", null, "other", "match", null};
        for (int i = 0; i < values.length; i++) {
          id.setSafe(i, i);
          if (values[i] == null) {
            name.setNull(i);
          } else {
            name.setSafe(i, values[i].getBytes(StandardCharsets.UTF_8));
          }
        }
        root.setRowCount(values.length);
        fragments =
            Fragment.create(
                table.toString(),
                allocator,
                root,
                new WriteParams.Builder()
                    .withMaxRowsPerFile(3)
                    .withEnableStableRowIds(false)
                    .build());
      }
      IndexSegmentReadTask task;
      Map<Long, FragmentReadTask> readers = new LinkedHashMap<>();
      try (Dataset dataset =
          Dataset.commit(
              allocator,
              table.toString(),
              new FragmentOperation.Append(fragments),
              Optional.of(1L))) {
        Index index =
            dataset.createIndex(
                IndexOptions.builder(
                        List.of("name"),
                        IndexType.BTREE,
                        IndexParams.builder()
                            .setScalarIndexParams(BTreeIndexParams.builder().build())
                            .build())
                    .build());
        dataset.delete("id = 1");
        task = IndexSegmentReadTask.capture(dataset, index.uuid());
        for (Fragment fragment : dataset.getFragments()) {
          readers.put(
              (long) fragment.getId(),
              FragmentReadTask.capture(dataset, fragment.getId(), List.of("id")));
        }
      }
      assertEquals(2, readers.size());
      assertArrayEquals(
          readers.keySet().stream().mapToLong(Long::longValue).toArray(), task.getFragmentIds());
      byte[] before = task.toBytes();
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
        output.writeObject(task);
      }
      try (ObjectInputStream input =
          new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        task = (IndexSegmentReadTask) input.readObject();
      }
      assertArrayEquals(before, task.toBytes());
      byte[] payload = task.toBytes();
      IndexSegmentReadTask transported = IndexSegmentReadTask.fromBytes(payload);
      payload[0] = 0;
      assertArrayEquals(before, transported.toBytes());
      Files.move(table.resolve("_versions"), tempDir.resolve("removed_manifests"));
      assertThrows(RuntimeException.class, () -> Dataset.open(table.toString(), allocator));

      Schema literalSchema = new Schema(List.of(Field.nullable("literal", new ArrowType.Utf8())));
      try (VectorSchemaRoot literal = VectorSchemaRoot.create(literalSchema, allocator)) {
        literal.allocateNew();
        VarCharVector value = (VarCharVector) literal.getVector(0);
        value.setSafe(0, "match".getBytes(StandardCharsets.UTF_8));
        literal.setRowCount(1);
        List<Long> addresses;
        try (ArrowReader hits =
            transported
                .reader()
                .allocator(allocator)
                .session(session)
                .batchSize(1)
                .equalsValue(literal)
                .open()) {
          addresses = readAddresses(hits, 1);
        }
        // The index retains id=1. Fragment materialization must apply the captured deletion.
        assertEquals(3, addresses.size());
        List<Integer> ids = new ArrayList<>();
        for (Map.Entry<Long, FragmentReadTask> entry : readers.entrySet()) {
          long[] offsets =
              addresses.stream()
                  .filter(address -> (address >>> 32) == entry.getKey())
                  .mapToLong(address -> address & 0xffffffffL)
                  .toArray();
          try (ArrowReader rows =
              entry
                  .getValue()
                  .reader()
                  .allocator(allocator)
                  .session(session)
                  .rowOffsets(offsets)
                  .open()) {
            while (rows.loadNextBatch()) {
              IntVector id = (IntVector) rows.getVectorSchemaRoot().getVector("id");
              for (int i = 0; i < rows.getVectorSchemaRoot().getRowCount(); i++) {
                ids.add(id.get(i));
              }
            }
          }
        }
        assertEquals(Arrays.asList(0, 4), ids);

        value.setNull(0);
        try (ArrowReader hits =
            transported
                .reader()
                .allocator(allocator)
                .session(session)
                .equalsValue(literal)
                .open()) {
          assertTrue(readAddresses(hits, 1024).isEmpty());
        }
      }
      try (ArrowReader hits =
          transported.reader().allocator(allocator).session(session).batchSize(1).isNull().open()) {
        List<Long> addresses = readAddresses(hits, 1);
        assertEquals(2, addresses.size());
        assertTrue(addresses.stream().allMatch(address -> (address & 0xffffffffL) == 2));
      }
      Schema wrongType =
          new Schema(List.of(Field.nullable("literal", new ArrowType.Int(32, true))));
      try (VectorSchemaRoot literal = VectorSchemaRoot.create(wrongType, allocator)) {
        literal.allocateNew();
        ((IntVector) literal.getVector(0)).setSafe(0, 7);
        literal.setRowCount(1);
        IllegalArgumentException error =
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    transported
                        .reader()
                        .allocator(allocator)
                        .session(session)
                        .equalsValue(literal)
                        .open());
        assertTrue(error.getMessage().contains("literal type Int32"));
        assertTrue(error.getMessage().contains("key type Utf8"));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void rejectsUnknownOrMissingTransportContractBeforeStorageIo() {
    UnsupportedOperationException version =
        assertThrows(
            UnsupportedOperationException.class,
            () -> IndexSegmentReadTask.fromBytes(new byte[] {8, 99}));
    assertTrue(version.getMessage().contains("version 99"));
    UnsupportedOperationException mode =
        assertThrows(
            UnsupportedOperationException.class,
            () -> IndexSegmentReadTask.fromBytes(new byte[] {8, 1}));
    assertTrue(mode.getMessage().contains("explicit physical row addresses"));
  }

  @ParameterizedTest
  @CsvSource({
    "PHYSICAL_ADDRESSES, missing fragment coverage",
    "STABLE_IDS, StableIds",
    "REQUIRES_REMAPPING, RequiresRemapping"
  })
  void validatesRowIdModesAcrossJni(IndexSegmentReadTask.RowIdMode mode, String expectedMessage) {
    // Minimal valid IndexMetadata protobuf: field 1 is UUID; UUID field 1 holds 16 zero bytes.
    // Core rejects the row-ID mode before requiring coverage or opening storage.
    byte[] metadata = new byte[20];
    metadata[0] = 10;
    metadata[1] = 18;
    metadata[2] = 10;
    metadata[3] = 16;
    Class<? extends RuntimeException> errorType =
        mode == IndexSegmentReadTask.RowIdMode.PHYSICAL_ADDRESSES
            ? IllegalArgumentException.class
            : UnsupportedOperationException.class;
    RuntimeException error =
        assertThrows(
            errorType,
            () ->
                IndexSegmentReadTask.fromSegment(
                    "unsupported-store://unopened", "segment", metadata, mode));
    assertTrue(error.getMessage().contains(expectedMessage));
  }

  @Test
  void rejectsInvalidIndexTimestampWithoutPanicking() {
    // UUID followed by created_at (field 8) encoded as Long.MAX_VALUE milliseconds.
    byte[] metadata = new byte[30];
    metadata[0] = 10;
    metadata[1] = 18;
    metadata[2] = 10;
    metadata[3] = 16;
    metadata[20] = 64;
    Arrays.fill(metadata, 21, 29, (byte) 0xff);
    metadata[29] = 0x7f;
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IndexSegmentReadTask.fromSegment(
                    "unsupported-store://unopened",
                    "segment",
                    metadata,
                    IndexSegmentReadTask.RowIdMode.PHYSICAL_ADDRESSES));
    assertTrue(error.getMessage().contains("created_at timestamp 9223372036854775807"));
  }

  private static List<Long> readAddresses(ArrowReader reader, int batchSize) throws Exception {
    List<Long> addresses = new ArrayList<>();
    while (reader.loadNextBatch()) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(1, root.getFieldVectors().size());
      assertTrue(root.getRowCount() <= batchSize);
      UInt8Vector values = (UInt8Vector) root.getVector("_rowaddr");
      for (int i = 0; i < root.getRowCount(); i++) {
        addresses.add(values.get(i));
      }
    }
    return addresses;
  }
}
