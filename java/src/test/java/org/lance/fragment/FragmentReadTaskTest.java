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
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.Session;
import org.lance.TestUtils;
import org.lance.namespace.DirectoryNamespace;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FragmentReadTaskTest {
  @Test
  void scansAndTakesSurviveDatasetCloseAndManifestRemoval(@TempDir Path tempDir) throws Exception {
    Path table = tempDir.resolve("table");
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Session session =
            Session.builder().indexCacheSizeBytes(0).metadataCacheSizeBytes(4096).build()) {
      TestUtils.SimpleTestDataset fixture =
          new TestUtils.SimpleTestDataset(allocator, table.toString());
      fixture.createEmptyDataset().close();
      List<FragmentMetadata> fragments = fixture.createNewFragment(6, 3);
      assertEquals(2, fragments.size());
      List<FragmentReadTask> tasks = new ArrayList<>();
      List<FragmentReadTask> countTasks = new ArrayList<>();
      try (Dataset dataset =
          Dataset.commit(
              allocator,
              table.toString(),
              new FragmentOperation.Append(fragments),
              Optional.of(1L))) {
        dataset.delete("id = 1");
        for (Fragment fragment : dataset.getFragments()) {
          tasks.add(
              FragmentReadTask.capture(dataset, fragment.getId(), Collections.singletonList("id"))
                  .withRowAddress());
          countTasks.add(
              FragmentReadTask.capture(dataset, fragment.getId(), Collections.emptyList()));
        }
      }
      Files.move(table.resolve("_versions"), tempDir.resolve("removed_manifests"));
      assertThrows(RuntimeException.class, () -> Dataset.open(table.toString(), allocator));

      List<Integer> ids = new ArrayList<>();
      for (FragmentReadTask original : tasks) {
        FragmentReadTask transported = roundTrip(original);
        try (ArrowReader reader =
            transported.reader().allocator(allocator).session(session).batchSize(2).open()) {
          while (reader.loadNextBatch()) {
            IntVector id = (IntVector) reader.getVectorSchemaRoot().getVector("id");
            UInt8Vector address = (UInt8Vector) reader.getVectorSchemaRoot().getVector("_rowaddr");
            assertEquals(2, reader.getVectorSchemaRoot().getFieldVectors().size());
            for (int i = 0; i < reader.getVectorSchemaRoot().getRowCount(); i++) {
              ids.add(id.get(i));
              assertEquals(id.get(i) % 3, address.get(i) & 0xffffffffL);
            }
          }
        }
      }
      assertEquals(Arrays.asList(0, 2, 3, 4, 5), ids);

      try (ArrowReader reader =
          tasks
              .get(0)
              .reader()
              .allocator(allocator)
              .session(session)
              .rowOffsets(new long[] {0, 1, 2})
              .open()) {
        List<Integer> selected = new ArrayList<>();
        while (reader.loadNextBatch()) {
          IntVector id = (IntVector) reader.getVectorSchemaRoot().getVector("id");
          for (int i = 0; i < reader.getVectorSchemaRoot().getRowCount(); i++) {
            selected.add(id.get(i));
          }
        }
        assertEquals(Arrays.asList(0, 2), selected);
      }

      int liveRows = 0;
      for (FragmentReadTask task : countTasks) {
        try (ArrowReader reader = task.reader().allocator(allocator).session(session).open()) {
          while (reader.loadNextBatch()) {
            assertTrue(reader.getVectorSchemaRoot().getFieldVectors().isEmpty());
            liveRows += reader.getVectorSchemaRoot().getRowCount();
          }
        }
      }
      assertEquals(5, liveRows);

      try (ArrowReader reader =
          tasks.get(0).reader().allocator(allocator).rowOffsets(new long[0]).open()) {
        int selectedRows = 0;
        while (reader.loadNextBatch()) {
          selectedRows += reader.getVectorSchemaRoot().getRowCount();
        }
        assertEquals(0, selectedRows);
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void ownsStreamResourcesAndRejectsInvalidRequests(@TempDir Path tempDir) throws Exception {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      TestUtils.SimpleTestDataset fixture =
          new TestUtils.SimpleTestDataset(allocator, tempDir.resolve("table").toString());
      fixture.createEmptyDataset().close();
      FragmentReadTask task;
      try (Dataset dataset = fixture.write(1, 4)) {
        task =
            FragmentReadTask.capture(dataset, dataset.getFragments().get(0).getId(), List.of("id"));
      }

      byte[] original = task.toBytes();
      byte[] bytes = task.toBytes();
      FragmentReadTask copy = FragmentReadTask.fromBytes(bytes);
      bytes[0] = 0;
      assertArrayEquals(original, copy.toBytes());

      Session session =
          Session.builder().indexCacheSizeBytes(0).metadataCacheSizeBytes(4096).build();
      try (ArrowReader reader =
          task.reader().allocator(allocator).session(session).batchSize(1).open()) {
        session.close();
        assertTrue(reader.loadNextBatch());
        assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
        // Early close must release the remaining native stream without retaining Arrow buffers.
      } finally {
        session.close();
      }
      assertEquals(0, allocator.getAllocatedMemory());
      assertThrows(
          IllegalArgumentException.class,
          () -> task.reader().allocator(allocator).session(session).open());
      assertThrows(
          IllegalArgumentException.class,
          () -> task.reader().allocator(allocator).rowOffsets(new long[] {2, 1}).open());
      assertThrows(
          IllegalArgumentException.class,
          () -> task.reader().allocator(allocator).rowOffsets(new long[] {1, 1}).open());
      assertThrows(
          IllegalArgumentException.class,
          () -> task.reader().allocator(allocator).rowOffsets(new long[] {-1}).open());
      assertThrows(
          IllegalArgumentException.class,
          () -> task.reader().allocator(allocator).rowOffsets(new long[] {4}).open());
      assertThrows(
          IllegalArgumentException.class,
          () -> task.reader().allocator(allocator).tableId(List.of("table")).open());
      try (DirectoryNamespace namespace = new DirectoryNamespace()) {
        IllegalStateException error =
            assertThrows(
                IllegalStateException.class,
                () ->
                    task.reader()
                        .allocator(allocator)
                        .namespaceClient(namespace)
                        .tableId(List.of("table"))
                        .open());
        assertTrue(error.getMessage().contains("not initialized"));
      }
      assertThrows(IllegalArgumentException.class, () -> task.reader().batchSize(0));

      try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        task.reader().exportArrowStream(stream.memoryAddress());
        IllegalArgumentException error =
            assertThrows(
                IllegalArgumentException.class,
                () -> task.reader().exportArrowStream(stream.memoryAddress()));
        assertTrue(error.getMessage().contains("already populated"));
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  void rejectsUnknownTaskVersionBeforeStorageIo() {
    UnsupportedOperationException error =
        assertThrows(
            UnsupportedOperationException.class,
            () -> FragmentReadTask.fromBytes(new byte[] {8, 99}));
    assertTrue(error.getMessage().contains("version 99"));
  }

  private static FragmentReadTask roundTrip(FragmentReadTask task) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(task);
    }
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (FragmentReadTask) input.readObject();
    }
  }
}
