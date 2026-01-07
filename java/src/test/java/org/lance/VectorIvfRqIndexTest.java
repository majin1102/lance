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

import org.lance.index.DistanceType;
import org.lance.index.Index;
import org.lance.index.IndexOptions;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.vector.IvfBuildParams;
import org.lance.index.vector.RQBuildParams;
import org.lance.index.vector.VectorIndexParams;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class VectorIvfRqIndexTest {

  @TempDir Path tempDir;

  @Test
  public void testCreateIvfRqIndexAndListMetadata() throws Exception {
    Path datasetPath = tempDir.resolve("ivf_rq_index");

    try (TestVectorDataset testVectorDataset = new TestVectorDataset(datasetPath)) {
      try (Dataset dataset = testVectorDataset.create()) {
        IvfBuildParams ivf = new IvfBuildParams.Builder().setNumPartitions(2).build();
        RQBuildParams rq = new RQBuildParams.Builder().setNumBits((short) 1).build();

        VectorIndexParams vectorIndexParams =
            VectorIndexParams.withIvfRqParams(DistanceType.L2, ivf, rq);
        IndexParams indexParams =
            IndexParams.builder().setVectorIndexParams(vectorIndexParams).build();

        dataset.createIndex(
            IndexOptions.builder(
                    Collections.singletonList(TestVectorDataset.vectorColumnName),
                    IndexType.IVF_RQ,
                    indexParams)
                .withIndexName(TestVectorDataset.indexName)
                .build());

        List<Index> indexes = dataset.getIndexes();
        Index rqIndex =
            indexes.stream()
                .filter(idx -> TestVectorDataset.indexName.equals(idx.name()))
                .findFirst()
                .orElse(null);

        assertNotNull(rqIndex, "Expected IVF_RQ index to be present");

        IndexType indexType = rqIndex.indexType();
        assertNotNull(indexType, "IndexType should be set for IVF_RQ index");

        // Today all vector indices share the same VectorIndexDetails type and map to VECTOR.
        // This assertion allows both VECTOR and IVF_RQ so it remains valid if the mapping
        // is refined in the future.
        assertTrue(
            indexType == IndexType.VECTOR || indexType == IndexType.IVF_RQ,
            "IndexType for IVF_RQ index should be VECTOR or IVF_RQ but was " + indexType);
      }
    }
  }
}
