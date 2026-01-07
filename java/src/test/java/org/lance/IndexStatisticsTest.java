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

import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndexStatisticsTest {

  @TempDir Path tempDir;

  @Test
  public void testIndexStatistics() throws Exception {
    Path datasetPath = tempDir.resolve("vector_dataset");

    try (TestVectorDataset vectorDataset = new TestVectorDataset(datasetPath)) {
      try (Dataset dataset = vectorDataset.create()) {
        ScalarIndexParams scalarParams = ScalarIndexParams.create("btree");
        IndexParams indexParams = IndexParams.builder().setScalarIndexParams(scalarParams).build();
        dataset.createIndex(
            Collections.singletonList("i"),
            IndexType.BTREE,
            Optional.of(TestVectorDataset.indexName),
            indexParams,
            true);

        String statsJson = dataset.getIndexStatistics(TestVectorDataset.indexName);
        assertNotNull(statsJson, "Index statistics JSON should not be null");
        assertFalse(statsJson.isEmpty(), "Index statistics JSON should not be empty");

        assertTrue(
            statsJson.contains("\"name\":\"" + TestVectorDataset.indexName + "\""),
            "Index statistics should contain the index name");
        assertTrue(
            statsJson.contains("\"index_type\""),
            "Index statistics should contain index_type information");
      }
    }
  }
}
