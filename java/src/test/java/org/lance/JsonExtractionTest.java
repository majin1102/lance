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
package org.lance;

import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.lance.util.JsonFields;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonExtractionTest {

  @Test
  void testJsonExtraction(@TempDir Path tempDir) throws Exception {
    String datasetPath = tempDir.resolve("json_extraction_test").toString();
    try (BufferAllocator allocator = new RootAllocator()) {
      Schema schema =
          new Schema(
              Arrays.asList(
                  Field.nullable("id", new ArrowType.Int(32, true)),
                  JsonFields.jsonUtf8("data", true)));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        root.allocateNew();

        IntVector idVector = (IntVector) root.getVector("id");
        VarCharVector dataVector = (VarCharVector) root.getVector("data");

        idVector.setSafe(0, 1);
        idVector.setSafe(1, 2);
        idVector.setSafe(2, 3);

        dataVector.setSafe(0, "{\"user\":{\"theme\":\"dark\"}}".getBytes(StandardCharsets.UTF_8));
        dataVector.setSafe(1, "{\"user\":{\"theme\":\"light\"}}".getBytes(StandardCharsets.UTF_8));
        dataVector.setSafe(2, "{\"user\":{\"theme\":\"dark\"}}".getBytes(StandardCharsets.UTF_8));

        root.setRowCount(3);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }

        byte[] bytes = out.toByteArray();
        try (ArrowStreamReader reader =
            new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(bytes), allocator)) {
          try (Dataset ds =
              Dataset.write()
                  .allocator(allocator)
                  .reader(reader)
                  .uri(datasetPath)
                  .mode(WriteParams.WriteMode.OVERWRITE)
                  .execute()) {
            assertEquals(datasetPath, ds.uri());
          }
        }
      }

      try (Dataset dataset = Dataset.open().allocator(allocator).uri(datasetPath).build()) {
        String filter = "json_extract(data, '$.user.theme') = '\"dark\"'";
        try (LanceScanner scanner =
            dataset.newScan(new ScanOptions.Builder().filter(filter).build())) {
          try (ArrowReader resultReader = scanner.scanBatches()) {
            int totalRows = 0;
            boolean hadBatch = false;
            while (resultReader.loadNextBatch()) {
              hadBatch = true;
              totalRows += resultReader.getVectorSchemaRoot().getRowCount();
            }
            assertTrue(hadBatch, "Expected at least one batch to be loaded");
            assertEquals(2, totalRows, "Expected exactly two rows matching the filter");
          }
        }
      }
    }
  }
}
